package com.fxstream.core;

import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;

import static org.assertj.core.api.Assertions.assertThat;

class TickRingTest {

    @Test
    void fullRingRefusesUntilDrained() {
        TickRing r = new TickRing(4);
        for (int i = 0; i < 4; i++) assertThat(r.offer(0, i, 0, 0, 0, 0)).isTrue();
        assertThat(r.offer(0, 99, 0, 0, 0, 0)).isFalse();

        long[] got = new long[8];
        int[] n = {0};
        assertThat(r.drain((lp, b, a, bq, aq, o) -> got[n[0]++] = b, 2)).isEqualTo(2);
        assertThat(r.offer(0, 4, 0, 0, 0, 0)).isTrue();
        assertThat(r.offer(0, 5, 0, 0, 0, 0)).isTrue();
        r.drain((lp, b, a, bq, aq, o) -> got[n[0]++] = b, 100);
        assertThat(got).startsWith(0, 1, 2, 3, 4, 5);
    }

    /**
     * Four producers race on a deliberately small ring so it wraps ~60k times. Every tick must arrive
     * exactly once, with all its fields intact, and each producer's ticks in that producer's order.
     */
    @Test
    void manyProducersNothingLostTornOrReordered() throws Exception {
        int producers = 4, perProducer = 1_000_000;
        TickRing r = new TickRing(64);
        CountDownLatch go = new CountDownLatch(1);
        Thread[] ts = new Thread[producers];
        for (int p = 0; p < producers; p++) {
            int lp = p;
            ts[p] = new Thread(() -> {
                try { go.await(); } catch (InterruptedException e) { return; }
                for (long i = 1; i <= perProducer; i++) {
                    // every field derived from i so a torn slot shows up as an inconsistent tick
                    while (!r.offer(lp, i, i * 3, i * 5, i * 7, i * 11)) Thread.onSpinWait();
                }
            });
            ts[p].start();
        }
        long[] last = new long[producers];
        long[] count = {0};
        boolean[] bad = {false};
        go.countDown();
        long total = (long) producers * perProducer;
        while (count[0] < total) {
            r.drain((lp, b, a, bq, aq, o) -> {
                if (a != b * 3 || bq != b * 5 || aq != b * 7 || o != b * 11 || b != last[lp] + 1) bad[0] = true;
                last[lp] = b;
                count[0]++;
            }, 256);
        }
        for (Thread t : ts) t.join();
        assertThat(bad[0]).as("torn or out-of-order tick").isFalse();
        assertThat(last).containsOnly(perProducer);
        assertThat(r.size()).isZero();
    }
}
