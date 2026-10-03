package com.fxstream.core;

import org.junit.jupiter.api.Test;

import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class PairEngineTest {
    static final long MS = 1_000_000L;
    final Pair eur = Pair.bySymbol("EURUSD");

    record P(int tier, long seq, long bid, long ask, int lps, long origin) {}

    static final class Capture implements PriceSink {
        final List<P> got = new ArrayList<>();
        @Override public void onPrice(Pair pair, int tier, long seq, long bid, long ask, int lps, long origin) {
            got.add(new P(tier, seq, bid, ask, lps, origin));
        }
    }

    @Test
    void oneBookChangePricesEveryTierUnderOneSequence() {
        Capture c = new Capture();
        PairEngine e = new PairEngine(eur, 4, 200 * MS, 1024, Pricer.defaults(), c, 0);
        e.offer(0, 108345, 108350, 1, 1, 77);
        e.pollOnce(1);
        assertThat(c.got).containsExactly(
                new P(0, 1, 108343, 108352, 1, 77),
                new P(1, 1, 108340, 108355, 1, 77),
                new P(2, 1, 108335, 108360, 1, 77));
    }

    @Test
    void aBatchOfTicksIsConflatedIntoOnePriceStampedWithTheOldestTick() {
        Capture c = new Capture();
        PairEngine e = new PairEngine(eur, 4, 200 * MS, 1024, new Pricer(new long[] {0}, new long[] {Long.MAX_VALUE}, new long[] {0}), c, 0);
        e.offer(0, 108345, 108350, 1, 1, 500);
        e.offer(1, 108346, 108351, 1, 1, 300);
        e.offer(0, 108347, 108350, 1, 1, 900);
        e.pollOnce(1);
        assertThat(c.got).containsExactly(new P(0, 1, 108347, 108350, 2, 300));
    }

    @Test
    void unchangedTopPublishesNothingAndStalenessPublishesWithZeroOrigin() {
        Capture c = new Capture();
        PairEngine e = new PairEngine(eur, 4, 200 * MS, 1024, new Pricer(new long[] {0}, new long[] {Long.MAX_VALUE}, new long[] {0}), c, 0);
        e.offer(0, 108345, 108350, 1, 1, 1);
        e.offer(1, 108340, 108352, 1, 1, 1);
        e.pollOnce(0);
        e.offer(1, 108341, 108352, 1, 1, 2);   // behind the top on both sides
        e.pollOnce(100 * MS);
        assertThat(c.got).hasSize(1);

        e.offer(1, 108341, 108352, 1, 1, 3);   // LP1 keeps ticking, LP0 goes quiet
        e.pollOnce(201 * MS);
        assertThat(c.got).hasSize(2);
        assertThat(c.got.get(1)).isEqualTo(new P(0, 2, 108341, 108352, 1, 3));
        e.pollOnce(402 * MS);                  // now LP1 is stale too: nothing two-way to send
        assertThat(c.got).hasSize(2);
        assertThat(e.freshLps()).isZero();
    }

    /**
     * A parked engine must be woken by the tick, not by its park timeout. Each round lets the engine go
     * idle and park, then sends one tick and times how long until the price comes out. A lost wake-up
     * would show as a full park timeout (1 ms on Linux, ~16 ms on Windows).
     */
    @Test
    void parkedEngineWakesOnOffer() throws Exception {
        var latest = new java.util.concurrent.atomic.AtomicLong();
        PriceSink sink = (pair, tier, seq, bid, ask, lps, origin) -> { if (tier == 0) latest.set(System.nanoTime()); };
        PairEngine e = new PairEngine(eur, 2, 10_000 * MS, 1024, Pricer.defaults(), sink, 0);
        e.start();
        long[] wake = new long[50];
        for (int i = 0; i < wake.length; i++) {
            Thread.sleep(20);                                  // engine spins, yields, then parks
            latest.set(0);
            long t0 = System.nanoTime();
            e.offer(0, 108300 + i, 108320 + i, 1, 1, 1);
            while (latest.get() == 0) Thread.onSpinWait();
            wake[i] = latest.get() - t0;
        }
        e.stop();
        java.util.Arrays.sort(wake);
        System.out.printf("parked engine wake-up: p50 %.1f us, max %.1f us%n", wake[25] / 1e3, wake[49] / 1e3);
        assertThat(wake[25]).as("median wake-up ns").isLessThan(500 * 1_000L);
    }

    /**
     * The tick path must not allocate. Measured with the JVM's per-thread allocation counter after JIT
     * warm-up, over a million ticks; a single boxed long or iterator per tick would show up as >= 16 MB.
     */
    @Test
    void tickPathAllocatesNothing() {
        var mx = (com.sun.management.ThreadMXBean) ManagementFactory.getThreadMXBean();
        long tid = Thread.currentThread().threadId();
        long[] sinkSum = {0};
        PriceSink sink = (pair, tier, seq, bid, ask, lps, origin) -> sinkSum[0] += bid ^ ask;
        PairEngine e = new PairEngine(eur, 8, 200 * MS, 1024, Pricer.defaults(), sink, 0);

        Runnable run = () -> {
            for (int i = 0; i < 1_000_000; i++) {
                long px = 108300 + i % 1000;   // rising sawtooth: nearly every tick is a new best bid
                e.offer(i & 7, px, px + 20, 1_000_000, 1_000_000, i + 1); // spread > 8-LP lag, so never crossed
                e.pollOnce(i);
            }
        };
        for (int w = 0; w < 5; w++) run.run();   // let C2 compile it

        long pricesBefore = e.prices();
        long before = mx.getThreadAllocatedBytes(tid);
        run.run();
        long allocated = mx.getThreadAllocatedBytes(tid) - before;

        System.out.printf("tick path: %d bytes allocated over 1M ticks, %d prices%n", allocated, e.prices() - pricesBefore);
        assertThat(e.prices() - pricesBefore).as("prices published in the measured run").isGreaterThan(900_000);
        assertThat(allocated).as("bytes allocated over 1M ticks").isLessThan(1024);
    }
}
