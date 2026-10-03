package com.fxstream.bench;

import com.fxstream.core.Pair;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;

import java.sql.DriverManager;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.SplittableRandom;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Trade-path checks, judged from what Postgres actually stored rather than from what the client was told.
 */
final class TradeBench {
    private TradeBench() {}

    static final AMQP.BasicProperties PERSISTENT = new AMQP.BasicProperties.Builder()
            .deliveryMode(2).contentType("application/json").build();

    /** Results queue for one client; durable quorum so results published during a broker restart survive. */
    static ConcurrentHashMap<String, AtomicInteger> collectResults(Connection c, String clientId) throws Exception {
        ConcurrentHashMap<String, AtomicInteger> got = new ConcurrentHashMap<>();
        Channel ch = c.createChannel();
        String q = "bench.results." + clientId;
        ch.queueDeclare(q, true, false, false, Map.of("x-queue-type", "quorum"));
        ch.queuePurge(q);
        ch.queueBind(q, "fx.trade.results", "result." + clientId);
        ch.basicConsume(q, true, (tag, d) -> {
            @SuppressWarnings("unchecked")
            Map<String, Object> r = Out.JSON.readValue(d.getBody(), Map.class);
            got.computeIfAbsent((String) r.get("clientOrderId"), k -> new AtomicInteger()).incrementAndGet();
        }, tag -> {});
        return got;
    }

    static Map<String, Object> quote(Main.Args a, String clientId, String side, long ttlMs) throws Exception {
        return Out.postJson(a.api() + "/api/quotes",
                Map.of("clientId", clientId, "pair", "EURUSD", "side", side, "qty", 1_000_000, "ttlMs", ttlMs));
    }

    static byte[] request(String orderId, String clientId, String quoteId) throws Exception {
        return Out.JSON.writeValueAsBytes(Map.of("clientOrderId", orderId, "clientId", clientId, "quoteId", quoteId));
    }

    static java.sql.Connection db(Main.Args a) throws Exception {
        return DriverManager.getConnection(a.str("jdbc", "jdbc:postgresql://localhost:5432/fxstream"),
                a.str("db-user", "fxstream"), a.str("db-pass", "fxstream"));
    }

    static boolean waitFor(ConcurrentHashMap<String, AtomicInteger> got, int n, long timeoutMs) {
        long end = System.currentTimeMillis() + timeoutMs;
        while (got.size() < n && System.currentTimeMillis() < end) Out.sleep(100);
        return got.size() >= n;
    }

    /**
     * 1,000 quotes with a 200 ms TTL, each traded after a random 0-400 ms delay, so about half arrive
     * late. Then, from the stored rows: every request received at or after its quote's expiry must be
     * REJECTED_EXPIRED, and none received before expiry may be rejected as expired.
     */
    static void expiry(Main.Args a) throws Exception {
        int n = a.i("quotes", 1000);
        long ttl = a.l("ttl-ms", 200);
        String run = "exp-" + Long.toString(System.currentTimeMillis(), 36) + "-";
        String client = "acme";
        SplittableRandom rnd = new SplittableRandom(7);
        ScheduledExecutorService sched = Executors.newSingleThreadScheduledExecutor();

        try (Connection c = Out.rabbit(a, "bench-expiry");
             LpFleet fleet = new LpFleet(a.host(), a.i("fix-port", 9878), 2, List.of(Pair.bySymbol("EURUSD")), 10)) {
            fleet.start(200);
            Out.sleep(1000);
            var got = collectResults(c, client);
            Channel pub = c.createChannel();
            pub.confirmSelect();
            for (int i = 0; i < n; i++) {
                Map<String, Object> q = quote(a, client, i % 2 == 0 ? "BUY" : "SELL", ttl);
                byte[] body = request(run + i, client, (String) q.get("quoteId"));
                sched.schedule(() -> {
                    synchronized (pub) { pub.basicPublish("fx.trades", "trade.request", PERSISTENT, body); }
                    return null;
                }, rnd.nextLong(2 * ttl + 1), TimeUnit.MILLISECONDS);
                Out.sleep(5);
            }
            sched.shutdown();
            sched.awaitTermination(1, TimeUnit.MINUTES);
            synchronized (pub) { pub.waitForConfirmsOrDie(10_000); }
            if (!waitFor(got, n, 60_000)) System.err.println("only " + got.size() + " results");
            fleet.stop();
        }

        Map<String, Object> r = new LinkedHashMap<>();
        r.put("quotes", n);
        r.put("ttl_ms", ttl);
        r.put("trade_delay", "uniform 0.." + (2 * ttl) + " ms after the quote");
        try (var db = db(a); var st = db.prepareStatement("""
                SELECT received_at >= quote_expires_at AS late, status, coalesce(reason, '') AS reason, count(*)
                FROM trades WHERE client_order_id LIKE ? GROUP BY 1, 2, 3 ORDER BY 1, 2, 3""")) {
            st.setString(1, run + "%");
            ResultSet rs = st.executeQuery();
            long late = 0, lateRejectedExpired = 0, lateFilled = 0, onTimeRejectedExpired = 0, rows = 0;
            Map<String, Long> breakdown = new LinkedHashMap<>();
            while (rs.next()) {
                boolean isLate = rs.getBoolean(1);
                String status = rs.getString(2), reason = rs.getString(3);
                long cnt = rs.getLong(4);
                rows += cnt;
                breakdown.put((isLate ? "late " : "on_time ") + status + (reason.isEmpty() ? "" : " " + reason), cnt);
                if (isLate) {
                    late += cnt;
                    if (reason.equals("REJECTED_EXPIRED")) lateRejectedExpired += cnt;
                    if (status.equals("FILLED")) lateFilled += cnt;
                } else if (reason.equals("REJECTED_EXPIRED")) {
                    onTimeRejectedExpired += cnt;
                }
            }
            r.put("rows", rows);
            r.put("late_requests", late);
            r.put("late_rejected_expired", lateRejectedExpired);
            r.put("late_filled", lateFilled);
            r.put("on_time_wrongly_rejected_expired", onTimeRejectedExpired);
            r.put("pct_late_rejected", late == 0 ? null : 100.0 * lateRejectedExpired / late);
            r.put("breakdown", breakdown);
        }
        Out.write(a, a.str("name", "expiry"), r);
    }

    /**
     * Zero lost, zero duplicated trades while the broker is killed (SIGKILL) and restarted repeatedly.
     *
     * <p>The client is deliberately at-least-once: it publishes in confirmed batches and republishes any
     * batch it did not get a full confirm for, so requests the broker had in fact stored arrive twice.
     * The service is at-least-once too (a crash between commit and ack redelivers). Only the service's
     * idempotency stands between that and a double trade - which the control run, with the service
     * started {@code --fx.idempotent=false}, shows.
     */
    static void chaos(Main.Args a) throws Exception {
        int orders = a.i("orders", 15000);
        double rate = a.l("rate", 200);
        int batch = a.i("batch", 50);
        int kills = a.i("kills", 6);
        long firstKillMs = a.l("first-kill-ms", 5000), killEveryMs = a.l("kill-every-ms", 8000);
        String restartCmd = a.str("restart-cmd", "");
        String client = "globex";
        String run = "chaos-" + Long.toString(System.currentTimeMillis(), 36) + "-";
        Map<String, Object> statsBefore = Out.getJson(a.api() + "/api/stats");
        AtomicLong republishes = new AtomicLong();
        AtomicInteger killsDone = new AtomicInteger();
        List<Long> killTimes = new ArrayList<>();

        try (Connection c = Out.rabbit(a, "bench-chaos-results"); Connection pc = Out.rabbit(a, "bench-chaos-pub");
             LpFleet fleet = new LpFleet(a.host(), a.i("fix-port", 9878), 2, List.of(Pair.bySymbol("EURUSD")), 10)) {
            fleet.start(200);
            Out.sleep(1000);
            var got = collectResults(c, client);
            long t0 = System.currentTimeMillis();

            Thread killer = new Thread(() -> {
                if (restartCmd.isBlank()) return;
                Out.sleep(firstKillMs);
                for (int k = 0; k < kills; k++) {
                    try {
                        killTimes.add(System.currentTimeMillis() - t0);
                        System.out.printf("[%5.1fs] kill -9 broker + restart (%d/%d)%n", (System.currentTimeMillis() - t0) / 1e3, k + 1, kills);
                        new ProcessBuilder(restartCmd.split(" ")).inheritIO().start().waitFor();
                        killsDone.incrementAndGet();
                    } catch (Exception e) {
                        System.err.println("restart command failed: " + e);
                    }
                    Out.sleep(killEveryMs);
                }
            }, "killer");
            killer.start();

            // Batched confirms, the usual way to get throughput out of a confirming publisher: publish a
            // batch, wait for the broker to confirm all of it, and if anything goes wrong republish the
            // WHOLE batch - including messages the broker had in fact already stored. At-least-once.
            Channel ch = pc.createChannel();
            ch.confirmSelect();
            long batchNanos = (long) (1e9 * batch / rate), next = System.nanoTime();
            for (int start = 0; start < orders; start += batch) {
                long wait = next - System.nanoTime();
                if (wait > 0) Out.sleep(wait / 1_000_000);
                next += batchNanos;
                List<byte[]> bodies = new ArrayList<>();
                for (int i = start; i < Math.min(orders, start + batch); i++) {
                    Map<String, Object> q = quote(a, client, i % 2 == 0 ? "BUY" : "SELL", 120_000);
                    bodies.add(request(run + i, client, (String) q.get("quoteId")));
                }
                for (int attempt = 0; ; attempt++) {
                    try {
                        if (attempt == 1) republishes.incrementAndGet();
                        for (byte[] body : bodies) ch.basicPublish("fx.trades", "trade.request", PERSISTENT, body);
                        ch.waitForConfirmsOrDie(5_000);
                        break;
                    } catch (Exception e) {
                        Out.sleep(200);
                        if (!ch.isOpen()) {
                            try { ch = pc.createChannel(); ch.confirmSelect(); } catch (Exception ignored) {}
                        }
                    }
                }
                if (start % 1000 == 0) System.out.printf("[%5.1fs] %d orders confirmed, %d results%n", (System.currentTimeMillis() - t0) / 1e3, start, got.size());
            }
            killer.join();
            System.out.println("all orders confirmed by broker; waiting for results");
            boolean all = waitFor(got, orders, 120_000);
            Out.sleep(3000); // late duplicates, if any, land in the DB too
            fleet.stop();

            Map<String, Object> r = new LinkedHashMap<>();
            Map<String, Object> statsAfter = Out.getJson(a.api() + "/api/stats");
            r.put("idempotent", statsAfter.get("idempotent"));
            r.put("orders", orders);
            r.put("publish_batch", batch);
            r.put("broker_kills", killsDone.get());
            r.put("kill_times_ms", killTimes);
            r.put("batches_republished", republishes.get());
            r.put("all_results_received", all);
            r.put("orders_with_result", got.size());
            r.put("result_messages_received", got.values().stream().mapToInt(AtomicInteger::get).sum());

            try (var db = db(a); var st = db.prepareStatement("""
                    SELECT count(*), count(DISTINCT client_order_id),
                           count(*) FILTER (WHERE status = 'FILLED'),
                           count(DISTINCT client_order_id) FILTER (WHERE status = 'FILLED')
                    FROM trades WHERE client_order_id LIKE ?""")) {
                st.setString(1, run + "%");
                ResultSet rs = st.executeQuery();
                rs.next();
                long rows = rs.getLong(1), distinct = rs.getLong(2), fills = rs.getLong(3), distinctFills = rs.getLong(4);
                r.put("trade_rows", rows);
                r.put("lost_orders", orders - distinct);
                r.put("duplicate_rows", rows - distinct);
                r.put("double_fills", fills - distinctFills);
                r.put("fills", fills);
            }
            try (var db = db(a); var st = db.prepareStatement("""
                    SELECT count(*) FROM (SELECT client_order_id FROM trades WHERE client_order_id LIKE ?
                                          GROUP BY 1 HAVING count(DISTINCT status) > 1) x""")) {
                st.setString(1, run + "%");
                ResultSet rs = st.executeQuery();
                rs.next();
                r.put("orders_with_conflicting_outcomes", rs.getLong(1)); // e.g. FILLED, then REJECTED on replay
            }
            try (Channel admin = c.createChannel()) {
                r.put("dlq_depth", admin.queueDeclarePassive("fx.trade.requests.dlq").getMessageCount());
            }
            r.put("service", Out.delta(statsBefore, statsAfter));
            Out.write(a, a.str("name", "chaos"), r);
        }
    }
}
