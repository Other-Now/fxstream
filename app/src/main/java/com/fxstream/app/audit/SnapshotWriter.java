package com.fxstream.app.audit;

import com.fxstream.core.Px;
import com.fxstream.core.PriceSink.BookSnapshot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * Takes snapshots from the engine threads and writes them to MongoDB on its own thread. The hand-off
 * queue is bounded and drops when full: a slow or dead MongoDB must never stall pricing.
 */
@Component
public class SnapshotWriter implements Consumer<BookSnapshot> {
    private static final Logger log = LoggerFactory.getLogger(SnapshotWriter.class);
    private final SnapshotRepository repo;
    private final ThreadPoolExecutor exec = new ThreadPoolExecutor(1, 1, 0, TimeUnit.MILLISECONDS,
            new ArrayBlockingQueue<>(256), r -> {
                Thread t = new Thread(r, "mongo-snapshots");
                t.setDaemon(true);
                return t;
            }, new ThreadPoolExecutor.DiscardPolicy());

    public SnapshotWriter(SnapshotRepository repo) {
        this.repo = repo;
    }

    @Override
    public void accept(BookSnapshot s) {
        exec.execute(() -> {
            try {
                List<PriceSnapshotDoc.LpQuote> lps = new ArrayList<>();
                for (int i = 0; i < s.lpBid().length; i++) {
                    if (s.lpBid()[i] == 0 && s.lpAsk()[i] == 0) continue;
                    lps.add(new PriceSnapshotDoc.LpQuote(i + 1, Px.format(s.lpBid()[i]), Px.format(s.lpAsk()[i]), s.lpFresh()[i]));
                }
                if (lps.isEmpty()) return;
                repo.save(new PriceSnapshotDoc(null, s.pair().symbol(), Instant.ofEpochMilli(s.epochMillis()),
                        Px.format(s.bestBid()), Px.format(s.bestAsk()), s.freshLps(), lps));
            } catch (RuntimeException e) {
                log.warn("snapshot write failed: {}", e.toString());
            }
        });
    }
}
