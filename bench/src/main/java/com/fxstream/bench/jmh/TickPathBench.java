package com.fxstream.bench.jmh;

import com.fxstream.core.Book;
import com.fxstream.core.Pair;
import com.fxstream.core.PairEngine;
import com.fxstream.core.PriceSink;
import com.fxstream.core.Pricer;
import com.fxstream.core.TickRing;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.util.concurrent.TimeUnit;

/**
 * Single-threaded cost of each stage of the tick path. Run with {@code -prof gc}: the
 * {@code gc.alloc.rate.norm} column (bytes per op) is the allocation claim.
 *
 * <ul>
 *   <li>{@code ringRoundTrip}  - offer one tick + drain it (uncontended)</li>
 *   <li>{@code bookRecompute}  - apply one LP update + rebuild the composite over N LPs</li>
 *   <li>{@code engineTick}     - the full path: ring -> book -> 3 tiers priced -> sink</li>
 * </ul>
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(1)
public class TickPathBench {
    @Param({"4", "8"})
    int lps;

    TickRing ring;
    Book book;
    PairEngine engine;
    Blackhole bh;
    long i, now;

    @Setup
    public void setup(Blackhole bh) {
        this.bh = bh;
        ring = new TickRing(1024);
        book = new Book(lps, 200_000_000L);
        PriceSink sink = (pair, tier, seq, bid, ask, n, origin) -> bh.consume(bid ^ ask);
        engine = new PairEngine(Pair.bySymbol("EURUSD"), lps, 200_000_000L, 1024, Pricer.defaults(), sink, 0);
    }

    private final TickRing.TickHandler drainSink = (lp, bid, ask, bq, aq, o) -> bh.consume(bid + ask);

    @Benchmark
    public int ringRoundTrip() {
        ring.offer(1, i, i + 20, 1_000_000, 1_000_000, i);
        i++;
        return ring.drain(drainSink, 1);
    }

    @Benchmark
    public boolean bookRecompute() {
        long px = 108_300 + (i % 1000);
        book.apply((int) (i % lps), px, px + 20, 1_000_000, 1_000_000, now);
        i++;
        now += 1000;
        return book.recompute(now);
    }

    @Benchmark
    public int engineTick() {
        long px = 108_300 + (i % 1000);   // rising sawtooth: nearly every tick moves the top
        engine.offer((int) (i % lps), px, px + 20, 1_000_000, 1_000_000, i + 1);
        i++;
        now += 1000;
        return engine.pollOnce(now);
    }
}
