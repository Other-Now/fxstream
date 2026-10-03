package com.fxstream.bench;

import com.fxstream.core.Pair;
import com.fxstream.core.PriceCodec;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import org.HdrHistogram.Histogram;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * How long does a provider that goes silent stay in the client price?
 *
 * <p>Three LPs stream EUR/USD; LP1 is skewed so it always holds the best bid. Each trial LP1 simply
 * stops sending (its FIX session stays up - the realistic failure is a stuck price feed, not a dropped
 * connection). Time is measured from LP1's last tick to the first client price that shows one fewer
 * contributing provider. Expected: the configured stale threshold plus at most one engine idle-park.
 */
final class StaleBench {
    private StaleBench() {}

    static void run(Main.Args a) throws Exception {
        int trials = a.i("trials", 20);
        long staleMs = a.l("stale-ms", 200);
        Pair eur = Pair.bySymbol("EURUSD");
        Histogram h = new Histogram(60_000_000_000L, 3);
        int[] lpsSeen = {-1};
        long[] firstDropNanos = {0};

        try (Connection c = Out.rabbit(a, "bench-stale"); LpFleet fleet = new LpFleet(a.host(), a.i("fix-port", 9878), 3, List.of(eur), 10)) {
            Channel ch = c.createChannel();
            String q = ch.queueDeclare().getQueue();
            ch.queueBind(q, "fx.prices", "price.EURUSD.0");
            ch.basicConsume(q, true, (tag, d) -> {
                long now = System.nanoTime();
                int lps = PriceCodec.freshLps(d.getBody());
                synchronized (lpsSeen) {
                    if (lpsSeen[0] == 3 && lps == 2 && firstDropNanos[0] == 0) firstDropNanos[0] = now;
                    lpsSeen[0] = lps;
                }
            }, tag -> {});

            fleet.skew(0, 3); // LP1's bid 0.3 pip better than the others: it is the top of book
            fleet.start(600); // 200 ticks/s per LP
            for (int t = 0; t < trials; t++) {
                fleet.pause(0, false);
                Out.sleep(1000);
                synchronized (lpsSeen) { firstDropNanos[0] = 0; }
                fleet.pause(0, true);
                long deadline = System.currentTimeMillis() + 5000;
                while (true) {
                    synchronized (lpsSeen) { if (firstDropNanos[0] != 0) break; }
                    if (System.currentTimeMillis() > deadline) throw new IllegalStateException("LP1 never dropped out");
                    Out.sleep(1);
                }
                long detect;
                synchronized (lpsSeen) { detect = firstDropNanos[0] - fleet.lastSentNanos(0); }
                h.recordValue(detect);
                System.out.printf("trial %d: LP1 excluded %.1f ms after its last tick%n", t + 1, detect / 1e6);
            }
            fleet.stop();
        }
        Map<String, Object> r = new LinkedHashMap<>();
        r.put("stale_threshold_ms", staleMs);
        r.put("trials", trials);
        r.put("last_tick_to_exclusion", Out.millis(h));
        r.put("note", "includes FIX + broker transit of the final price; threshold is the service's fx.stale-ms");
        Out.write(a, a.str("name", "stale"), r);
    }
}
