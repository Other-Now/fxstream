package com.fxstream.core;

import java.util.concurrent.locks.LockSupport;

/**
 * One currency pair's pricing thread: drain ticks from the ring, update the book, and if the composite
 * changed, price every tier and hand the result to the sink.
 *
 * <p>Batching is free conflation: if 20 ticks queued up while the sink was busy, all 20 are applied and
 * one price goes out, not 20 stale ones. The latency stamp carried on that price is the oldest tick of
 * the batch, so batching can never make the measured latency look better than it was.
 *
 * <p>The tick path ({@link #offer} -> {@link #pollOnce}) allocates nothing; {@code TickPathAllocTest} and
 * the JMH {@code gc.alloc.rate.norm} column check that. The one allocation is the audit snapshot, once a
 * second.
 */
public final class PairEngine implements TickRing.TickHandler, Runnable {
    private final Pair pair;
    private final TickRing ring;
    private final Book book;
    private final Pricer pricer;
    private final PriceSink sink;
    private final long snapshotEveryNanos;
    private final int[] streamTiers;

    private volatile boolean running = true;
    private volatile boolean sleeping;
    private volatile Thread thread;
    private long seq;
    private long batchOrigin;
    private long nextSnapshotNanos;
    private long tickNow;

    // read by other threads (quotes, trade checks, metrics); written only by the engine thread
    private volatile long lastBestBid, lastBestAsk, lastSeq;
    private volatile int lastFreshLps;
    private volatile long ticks, prices, crossed, ringFull;

    public PairEngine(Pair pair, int maxLps, long staleNanos, int ringCapacity, Pricer pricer, PriceSink sink,
                      long snapshotEveryNanos) {
        this.pair = pair;
        this.ring = new TickRing(ringCapacity);
        this.book = new Book(maxLps, staleNanos);
        this.pricer = pricer;
        this.sink = sink;
        this.snapshotEveryNanos = snapshotEveryNanos;
        this.streamTiers = new int[pricer.tiers()];
        for (int i = 0; i < streamTiers.length; i++) streamTiers[i] = i;
    }

    /**
     * Called by FIX session threads. Spins while the ring is full: back-pressure onto the LP session rather
     * than silently dropping a tick the book never sees.
     */
    public void offer(int lp, long bid, long ask, long bidQty, long askQty, long originNanos) {
        while (!ring.offer(lp, bid, ask, bidQty, askQty, originNanos)) {
            ringFull++; // racy counter across producers, diagnostic only
            Thread.onSpinWait();
        }
        // The ring's tail CAS (a volatile write) happened above; this volatile read of `sleeping` is ordered
        // after it. The engine does the mirror image (write sleeping, then read tail) before parking, so at
        // least one side always sees the other: either the engine sees the tick, or we see it asleep.
        if (sleeping) LockSupport.unpark(thread);
    }

    /** One iteration of the engine loop. Returns the number of ticks applied. Engine thread only. */
    public int pollOnce(long nowNanos) {
        tickNow = nowNanos;
        batchOrigin = 0;
        int n = ring.drain(this, 1024);
        if (n > 0) ticks += n;
        if (book.recompute(nowNanos)) publish();
        if (snapshotEveryNanos > 0 && nowNanos - nextSnapshotNanos >= 0) {
            nextSnapshotNanos = nowNanos + snapshotEveryNanos;
            sink.onSnapshot(snapshot());
        }
        return n;
    }

    @Override
    public void onTick(int lp, long bid, long ask, long bidQty, long askQty, long originNanos) {
        book.apply(lp, bid, ask, bidQty, askQty, tickNow);
        if (batchOrigin == 0 || originNanos < batchOrigin) batchOrigin = originNanos;
    }

    private void publish() {
        lastBestBid = book.bestBid();
        lastBestAsk = book.bestAsk();
        lastFreshLps = book.freshLps();
        if (!book.isTwoWay()) return;          // one-sided or empty: nothing a client can trade
        if (book.isCrossed()) { crossed++; return; }
        long s = ++seq;
        lastSeq = s;
        for (int tier : streamTiers) {
            sink.onPrice(pair, tier, s,
                    pricer.clientBid(pair, tier, 0, book.bestBid()),
                    pricer.clientAsk(pair, tier, 0, book.bestAsk()),
                    book.freshLps(), batchOrigin);
        }
        prices++;
    }

    private PriceSink.BookSnapshot snapshot() {
        int n = book.maxLps();
        long[] b = new long[n], a = new long[n];
        boolean[] f = new boolean[n];
        for (int i = 0; i < n; i++) {
            b[i] = book.lpBid(i);
            a[i] = book.lpAsk(i);
            f[i] = book.lpFresh(i, tickNow);
        }
        return new PriceSink.BookSnapshot(pair, System.currentTimeMillis(), book.bestBid(), book.bestAsk(),
                book.freshLps(), b, a, f);
    }

    /**
     * Idle strategy: busy-spin while ticks flow, then yield, then park - and a producer that finds the
     * engine parked unparks it (see {@link #offer}). So a tick never waits for a park timeout; the timeout
     * only bounds how late a stale LP is noticed when nothing at all is arriving.
     *
     * <p>Why not just a short timed park: on Windows {@code parkNanos(50_000)} sleeps ~15.8 ms (the
     * scheduler tick), which made the first version's latency at 1k ticks/s ten times worse than at
     * 10k ticks/s. On Linux the same call is ~60 us, but waking on demand is right on both.
     */
    @Override
    public void run() {
        thread = Thread.currentThread();
        int idle = 0;
        while (running) {
            if (pollOnce(System.nanoTime()) > 0) {
                idle = 0;
            } else if (++idle < 2_000) {
                Thread.onSpinWait();
            } else if (idle < 2_100) {
                Thread.yield();
            } else {
                sleeping = true;
                if (!ring.hasPending()) LockSupport.parkNanos(1_000_000);
                sleeping = false;
            }
        }
    }

    public Thread start() {
        Thread t = new Thread(this, "engine-" + pair.symbol());
        t.setDaemon(true);
        thread = t;
        t.start();
        return t;
    }

    public void stop() { running = false; }

    public Pair pair() { return pair; }
    public Pricer pricer() { return pricer; }
    public long bestBid() { return lastBestBid; }
    public long bestAsk() { return lastBestAsk; }
    public int freshLps() { return lastFreshLps; }
    public long seq() { return lastSeq; }
    public long ticks() { return ticks; }
    public long prices() { return prices; }
    public long crossed() { return crossed; }
    public long ringFull() { return ringFull; }
}
