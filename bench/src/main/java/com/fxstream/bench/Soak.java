package com.fxstream.bench;

import com.fxstream.core.Pair;
import com.fxstream.core.PairEngine;
import com.fxstream.core.PriceSink;
import com.fxstream.core.Pricer;

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.SplittableRandom;

/**
 * The pricing core alone, no FIX and no broker: P producer threads (playing the FIX sessions) hammer
 * the four pair engines as fast as they can, and we count garbage collections during the measured
 * window. If the tick path allocated even a few bytes per tick, tens of millions of ticks would force
 * young collections; zero collections is the end-to-end check on the JMH allocation numbers.
 */
final class Soak {
    private Soak() {}

    static void run(Main.Args a) throws Exception {
        int producers = a.i("producers", 4);
        long warmupS = a.l("warmup", 5), seconds = a.l("seconds", 30);
        long[] sinkSum = new long[Pair.ALL.size()];
        PriceSink sink = (pair, tier, seq, bid, ask, lps, origin) -> sinkSum[pair.id()] += bid ^ ask; // engine-thread local slot
        PairEngine[] engines = new PairEngine[Pair.ALL.size()];
        for (Pair p : Pair.ALL) {
            engines[p.id()] = new PairEngine(p, 8, 200_000_000L, 4096, Pricer.defaults(), sink, 0);
            engines[p.id()].start();
        }
        running = true;
        Thread[] ps = new Thread[producers];
        for (int i = 0; i < producers; i++) {
            int lp = i;
            ps[i] = new Thread(() -> {
                SplittableRandom r = new SplittableRandom(lp);
                while (running) {
                    long now = System.nanoTime();
                    long mid = 108_300 + (now >> 20) % 50;   // shared clock-driven mid: producers agree, book stays uncrossed
                    int pair = r.nextInt(engines.length);
                    engines[pair].offer(lp, mid - 5 - r.nextInt(3), mid + 5 + r.nextInt(3), 1_000_000, 1_000_000, now);
                }
            }, "producer-" + i);
            ps[i].start();
        }

        Thread.sleep(warmupS * 1000);
        long gcCount0 = gcCount(), gcTime0 = gcTime(), ticks0 = ticks(engines), prices0 = prices(engines);
        long heap0 = usedHeap();
        long t0 = System.nanoTime();
        Thread.sleep(seconds * 1000);
        long dt = System.nanoTime() - t0;
        long gcCount1 = gcCount(), gcTime1 = gcTime(), ticks1 = ticks(engines), prices1 = prices(engines);
        long heap1 = usedHeap();
        running = false;
        for (Thread t : ps) t.join();
        for (PairEngine e : engines) e.stop();

        Map<String, Object> r = new LinkedHashMap<>();
        r.put("producers", producers);
        r.put("pairs", engines.length);
        r.put("seconds", seconds);
        r.put("ticks", ticks1 - ticks0);
        r.put("ticks_per_s", Math.round((ticks1 - ticks0) * 1e9 / dt));
        r.put("book_changes", prices1 - prices0);
        r.put("gc_collections", gcCount1 - gcCount0);
        r.put("gc_time_ms", gcTime1 - gcTime0);
        r.put("heap_used_growth_bytes", heap1 - heap0);
        r.put("ring_full_spins", ringFull(engines));
        r.put("jvm", System.getProperty("java.vm.name") + " " + System.getProperty("java.version"));
        r.put("cpus", Runtime.getRuntime().availableProcessors());
        Out.write(a, a.str("name", "soak"), r);
    }

    private static volatile boolean running;

    static long gcCount() {
        long n = 0;
        for (GarbageCollectorMXBean b : ManagementFactory.getGarbageCollectorMXBeans()) n += Math.max(0, b.getCollectionCount());
        return n;
    }

    static long gcTime() {
        long n = 0;
        for (GarbageCollectorMXBean b : ManagementFactory.getGarbageCollectorMXBeans()) n += Math.max(0, b.getCollectionTime());
        return n;
    }

    static long usedHeap() {
        return ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getUsed();
    }

    static long ticks(PairEngine[] es) { long n = 0; for (PairEngine e : es) n += e.ticks(); return n; }
    static long prices(PairEngine[] es) { long n = 0; for (PairEngine e : es) n += e.prices(); return n; }
    static long ringFull(PairEngine[] es) { long n = 0; for (PairEngine e : es) n += e.ringFull(); return n; }
}
