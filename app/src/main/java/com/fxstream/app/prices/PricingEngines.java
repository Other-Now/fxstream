package com.fxstream.app.prices;

import com.fxstream.app.FxProperties;
import com.fxstream.app.audit.SnapshotWriter;
import com.fxstream.core.Pair;
import com.fxstream.core.PairEngine;
import com.fxstream.core.Pricer;
import org.springframework.boot.autoconfigure.amqp.RabbitProperties;
import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Component;

/** One {@link PairEngine} (one thread) per currency pair, all streaming through one publisher. */
@Component
public class PricingEngines implements SmartLifecycle {
    private final PairEngine[] engines;
    private final RabbitPricePublisher publisher;
    private final Pricer pricer = Pricer.defaults();
    private volatile boolean running;

    public PricingEngines(FxProperties fx, RabbitProperties rabbit, SnapshotWriter snapshots) throws Exception {
        this.publisher = new RabbitPricePublisher(rabbit, pricer.tiers(), snapshots);
        this.engines = new PairEngine[Pair.ALL.size()];
        for (Pair p : Pair.ALL) {
            engines[p.id()] = new PairEngine(p, fx.maxLps(), fx.staleMs() * 1_000_000L, fx.ringCapacity(),
                    pricer, publisher, fx.snapshotMs() * 1_000_000L);
        }
    }

    public PairEngine engine(Pair pair) {
        return engines[pair.id()];
    }

    public PairEngine[] all() {
        return engines;
    }

    public Pricer pricer() {
        return pricer;
    }

    public RabbitPricePublisher publisher() {
        return publisher;
    }

    @Override
    public void start() {
        for (PairEngine e : engines) e.start();
        running = true;
    }

    @Override
    public void stop() {
        for (PairEngine e : engines) e.stop();
        publisher.close();
        running = false;
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    /** Start before the FIX acceptor (which uses the default phase) and stop after it. */
    @Override
    public int getPhase() {
        return Integer.MAX_VALUE - 1000;
    }
}
