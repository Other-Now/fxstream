package com.fxstream.bench;

import com.fxstream.core.Pair;
import com.fxstream.core.PriceCodec;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.DeliverCallback;
import org.HdrHistogram.Histogram;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.LockSupport;

final class LatencyBench {
    private LatencyBench() {}

    /**
     * End to end: LP sends a FIX quote at its scheduled time T -> service parses, aggregates, prices,
     * publishes -> this process receives the tier-0 client price and records now - T. One histogram per
     * rate; prices caused by a provider going stale (origin 0) are not ticks and are skipped.
     */
    static void tickToPrice(Main.Args a) throws Exception {
        int lps = a.i("lps", 4);
        int npairs = a.i("pairs", 4);
        long warmupS = a.l("warmup", 10), seconds = a.l("seconds", 30);
        String[] rates = a.str("rates", "1000,10000,50000").split(",");
        List<Pair> pairs = Pair.ALL.subList(0, npairs);

        Histogram h = new Histogram(3_600_000_000_000L, 3);
        boolean[] recording = {false};
        long[] received = {0};
        try (Connection c = Out.rabbit(a, "bench-latency"); LpFleet fleet = new LpFleet(a.host(), a.i("fix-port", 9878), lps, pairs, 1)) {
            Channel ch = c.createChannel();
            String q = ch.queueDeclare().getQueue();
            ch.queueBind(q, "fx.prices", "price.*.0");
            DeliverCallback cb = (tag, d) -> {
                long now = System.nanoTime();
                long origin = PriceCodec.origin(d.getBody());
                if (origin == 0 || !recording[0]) return;
                synchronized (h) { h.recordValue(Math.max(0, now - origin)); received[0]++; }
            };
            ch.basicConsume(q, true, cb, tag -> {});

            List<Map<String, Object>> runs = new ArrayList<>();
            for (String rs : rates) {
                double rate = Double.parseDouble(rs);
                fleet.start(rate);
                Out.sleep(warmupS * 1000);
                Map<String, Object> before = Out.getJson(a.api() + "/api/stats");
                synchronized (h) { h.reset(); received[0] = 0; }
                fleet.senderLag.reset();
                recording[0] = true;
                Out.sleep(seconds * 1000);
                recording[0] = false;
                fleet.stop();
                Map<String, Object> after = Out.getJson(a.api() + "/api/stats");
                Out.sleep(1000);

                Map<String, Object> run = new LinkedHashMap<>();
                run.put("ticks_per_s", rate);
                run.put("lps", lps);
                run.put("pairs", npairs);
                run.put("seconds", seconds);
                synchronized (h) { run.put("tick_to_client_price", Out.micros(h)); }
                run.put("harness_sender_lag", Out.micros(fleet.senderLag));
                run.put("service", Out.delta(before, after));
                runs.add(run);
                System.out.printf("rate %.0f/s: %s%n", rate, run.get("tick_to_client_price"));
            }
            Out.write(a, a.str("name", "latency"), Map.of("runs", runs,
                    "note", "latency measured from each tick's scheduled send time; price = tier-0 stream; same host"));
        }
    }

    /** Baseline: what does one hop through the broker cost on this machine, with no service in between? */
    static void amqpOnly(Main.Args a) throws Exception {
        String[] rates = a.str("rates", "1000,10000").split(",");
        long warmupS = a.l("warmup", 5), seconds = a.l("seconds", 20);
        Histogram h = new Histogram(3_600_000_000_000L, 3);
        boolean[] recording = {false};
        try (Connection pubConn = Out.rabbit(a, "bench-amqp-pub"); Connection subConn = Out.rabbit(a, "bench-amqp-sub")) {
            Channel pub = pubConn.createChannel(), sub = subConn.createChannel();
            pub.exchangeDeclare("bench.amqp", "topic", false, true, null);
            String q = sub.queueDeclare().getQueue();
            sub.queueBind(q, "bench.amqp", "x");
            sub.basicConsume(q, true, (tag, d) -> {
                long now = System.nanoTime();
                if (!recording[0]) return;
                synchronized (h) { h.recordValue(Math.max(0, now - PriceCodec.origin(d.getBody()))); }
            }, tag -> {});
            AMQP.BasicProperties props = new AMQP.BasicProperties.Builder().deliveryMode(1).build();
            byte[] body = new byte[PriceCodec.SIZE];

            List<Map<String, Object>> runs = new ArrayList<>();
            for (String rs : rates) {
                double rate = Double.parseDouble(rs);
                long interval = (long) (1e9 / rate);
                long end = System.nanoTime() + (warmupS + seconds) * 1_000_000_000L;
                long measureFrom = System.nanoTime() + warmupS * 1_000_000_000L;
                long next = System.nanoTime();
                synchronized (h) { h.reset(); }
                while (next < end) {
                    long wait = next - System.nanoTime();
                    if (wait > 1_500_000) { LockSupport.parkNanos(wait - 1_000_000); continue; }
                    if (wait > 0) { Thread.onSpinWait(); continue; }
                    recording[0] = next >= measureFrom;
                    PriceCodec.encode(body, 0, 0, 0, 0, 0, 0, next);
                    pub.basicPublish("bench.amqp", "x", props, body);
                    next += interval;
                }
                Out.sleep(500);
                recording[0] = false;
                Map<String, Object> run = new LinkedHashMap<>();
                run.put("msgs_per_s", rate);
                synchronized (h) { run.put("publish_to_consume", Out.micros(h)); }
                runs.add(run);
                System.out.printf("rate %.0f/s: %s%n", rate, run.get("publish_to_consume"));
            }
            Out.write(a, a.str("name", "amqp"), Map.of("runs", runs,
                    "note", "transient 48-byte messages, autoAck consumer, publisher and consumer on separate connections, same host"));
        }
    }
}
