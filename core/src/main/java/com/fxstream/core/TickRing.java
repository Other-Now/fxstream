package com.fxstream.core;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;

/**
 * Bounded multi-producer / single-consumer ring of LP ticks, Disruptor-style.
 *
 * <p>Many FIX session threads (one per liquidity provider) write; exactly one pricing thread per pair
 * reads. Slots are preallocated as one flat {@code long[]} with 8 longs (64 bytes, one cache line) per
 * slot, so publishing a tick writes primitives into existing memory and never allocates.
 *
 * <p>Protocol:
 * <ol>
 *   <li>Producer claims sequence {@code t} by CAS on {@code tail}, but only if {@code t - head < capacity}
 *       (the slot's previous occupant, {@code t - capacity}, has been consumed).</li>
 *   <li>Producer writes the payload with plain stores, then {@code setRelease(slot.seq, t + 1)}.
 *       The release store is the publication: the consumer can never see seq == t+1 without also
 *       seeing the payload written before it.</li>
 *   <li>Consumer at {@code head = h} spins on {@code getAcquire(slot.seq) == h + 1}, reads the payload,
 *       then {@code setRelease(head, h + 1)} to hand the slot back.</li>
 * </ol>
 * A per-slot sequence (rather than one shared "published" cursor) is what lets producers finish out of
 * order: producer B can publish t+1 while A is still writing t, and the consumer simply stops at t.
 */
public final class TickRing {
    // slot layout, in longs
    private static final int SEQ = 0, LP = 1, BID = 2, ASK = 3, BID_QTY = 4, ASK_QTY = 5, ORIGIN = 6;
    private static final int STRIDE = 8;

    private static final VarHandle SLOTS = MethodHandles.arrayElementVarHandle(long[].class);
    private static final VarHandle TAIL, HEAD;
    static {
        try {
            MethodHandles.Lookup l = MethodHandles.lookup();
            TAIL = l.findVarHandle(TickRing.class, "tail", long.class);
            HEAD = l.findVarHandle(TickRing.class, "head", long.class);
        } catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    private final long[] slots;
    private final int mask;
    private final int capacity;

    // head and tail on separate cache lines from each other and from the array reference
    @SuppressWarnings("unused") private long p01, p02, p03, p04, p05, p06, p07;
    private volatile long tail;
    @SuppressWarnings("unused") private long p11, p12, p13, p14, p15, p16, p17;
    private volatile long head;
    @SuppressWarnings("unused") private long p21, p22, p23, p24, p25, p26, p27;

    public TickRing(int capacityPow2) {
        if (Integer.bitCount(capacityPow2) != 1) throw new IllegalArgumentException("capacity must be a power of 2");
        this.capacity = capacityPow2;
        this.mask = capacityPow2 - 1;
        this.slots = new long[capacityPow2 * STRIDE];
    }

    /** Producer side, any thread. Returns false if the ring is full; the caller decides to spin or drop. */
    public boolean offer(int lp, long bid, long ask, long bidQty, long askQty, long originNanos) {
        long t;
        do {
            t = (long) TAIL.getVolatile(this);
            if (t - (long) HEAD.getAcquire(this) >= capacity) return false;
        } while (!TAIL.compareAndSet(this, t, t + 1));

        int base = (int) (t & mask) * STRIDE;
        slots[base + LP] = lp;
        slots[base + BID] = bid;
        slots[base + ASK] = ask;
        slots[base + BID_QTY] = bidQty;
        slots[base + ASK_QTY] = askQty;
        slots[base + ORIGIN] = originNanos;
        SLOTS.setRelease(slots, base + SEQ, t + 1);
        return true;
    }

    /** Consumer side, the owning pricing thread only. Hands up to {@code max} ticks to {@code h}. */
    public int drain(TickHandler h, int max) {
        long hd = head; // only this thread writes head
        int n = 0;
        while (n < max) {
            int base = (int) (hd & mask) * STRIDE;
            if ((long) SLOTS.getAcquire(slots, base + SEQ) != hd + 1) break;
            h.onTick((int) slots[base + LP], slots[base + BID], slots[base + ASK],
                    slots[base + BID_QTY], slots[base + ASK_QTY], slots[base + ORIGIN]);
            hd++;
            n++;
            HEAD.setRelease(this, hd);
        }
        return n;
    }

    /** Volatile read of tail: true if a producer has claimed a slot the consumer has not taken yet. */
    public boolean hasPending() {
        return tail != head;
    }

    public int capacity() {
        return capacity;
    }

    public long size() {
        return tail - head;
    }

    @FunctionalInterface
    public interface TickHandler {
        void onTick(int lp, long bid, long ask, long bidQty, long askQty, long originNanos);
    }
}
