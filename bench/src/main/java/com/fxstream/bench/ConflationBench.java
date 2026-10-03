package com.fxstream.bench;

import com.fxstream.core.Pair;
import com.fxstream.core.PriceCodec;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.AMQP;
import org.HdrHistogram.Histogram;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * A client that can only process 50 prices a second, on a stream producing ~1,000 a second.
 *
 * <ul>
 *   <li><b>conflated</b>: the client's queue is declared {@code x-max-length=1, x-overflow=drop-head}.
 *       When a new price arrives and one is already waiting, the broker drops the older one. The client
 *       always gets the latest price and the queue can never grow.</li>
 *   <li><b>plain</b>: same client, ordinary queue. It works through every price in order, so what it
 *       sees gets older and older and the broker holds an ever-growing backlog.</li>
 * </ul>
 * Both use prefetch 1 + manual ack, so "in flight" is at most one message on top of the queue depth.
 */
final class ConflationBench {
    private ConflationBench() {}

    static final class SlowClient extends DefaultConsumer {
        final Histogram age = new Histogram(3_600_000_000_000L, 3);
        final long workMs;
        volatile long lastSeq, received;

        SlowClient(Channel ch, long workMs) { super(ch); this.workMs = workMs; }

        @Override
        public void handleDelivery(String tag, Envelope env, AMQP.BasicProperties p, byte[] body) throws IOException {
            long now = System.nanoTime();
            long origin = PriceCodec.origin(body);
            if (origin != 0) synchronized (age) { age.recordValue(Math.max(0, now - origin)); }
            lastSeq = PriceCodec.seq(body);
            received++;
            Out.sleep(workMs);                       // the slow part: rendering, risk checks, whatever
            getChannel().basicAck(env.getDeliveryTag(), false);
        }
    }

    static void run(Main.Args a) throws Exception {
        long seconds = a.l("seconds", 20), workMs = a.l("work-ms", 20);
        double rate = a.l("rate", 1000);
        Pair eur = Pair.bySymbol("EURUSD");
        Map<String, Object> r = new LinkedHashMap<>();

        try (Connection c = Out.rabbit(a, "bench-conflation"); LpFleet fleet = new LpFleet(a.host(), a.i("fix-port", 9878), 2, List.of(eur), 1)) {
            Channel admin = c.createChannel();
            String conflated = "bench.slow.conflated", plain = "bench.slow.plain";
            admin.queueDelete(conflated);
            admin.queueDelete(plain);
            admin.queueDeclare(conflated, false, false, false, Map.of("x-max-length", 1, "x-overflow", "drop-head"));
            admin.queueDeclare(plain, false, false, false, null);
            admin.queueBind(conflated, "fx.prices", "price.EURUSD.0");
            admin.queueBind(plain, "fx.prices", "price.EURUSD.0");

            Channel ch1 = c.createChannel(), ch2 = c.createChannel();
            ch1.basicQos(1);
            ch2.basicQos(1);
            SlowClient cc = new SlowClient(ch1, workMs), pc = new SlowClient(ch2, workMs);
            ch1.basicConsume(conflated, false, cc);
            ch2.basicConsume(plain, false, pc);

            long maxConflated = 0, maxPlain = 0;
            fleet.start(rate);
            long end = System.currentTimeMillis() + seconds * 1000;
            while (System.currentTimeMillis() < end) {
                maxConflated = Math.max(maxConflated, admin.queueDeclarePassive(conflated).getMessageCount());
                maxPlain = Math.max(maxPlain, admin.queueDeclarePassive(plain).getMessageCount());
                Out.sleep(100);
            }
            fleet.stop();
            Out.sleep(1000); // LPs go stale after the stop and may move the book one last time; let that settle
            long publishedSeq = ((Number) Out.getJsonList(a.api() + "/api/prices").get(eur.id()).get("seq")).longValue();
            Out.sleep(3 * workMs + 200); // let the conflated client take the one waiting message
            long plainBacklog = admin.queueDeclarePassive(plain).getMessageCount();

            r.put("stream_ticks_per_s", rate);
            r.put("client_capacity_per_s", 1000 / workMs);
            r.put("seconds", seconds);
            r.put("conflated", Map.of(
                    "max_queue_depth", maxConflated,
                    "received", cc.received,
                    "price_age_seen_by_client", Out.millis(cc.age),
                    "last_seq_received", cc.lastSeq,
                    "last_seq_published", publishedSeq,
                    "ends_on_latest_price", cc.lastSeq == publishedSeq));
            r.put("plain", Map.of(
                    "max_queue_depth", maxPlain,
                    "backlog_at_end", plainBacklog,
                    "received", pc.received,
                    "price_age_seen_by_client", Out.millis(pc.age)));
            ch1.close();
            ch2.close();
            admin.queueDelete(conflated);
            admin.queueDelete(plain);
        }
        Out.write(a, a.str("name", "conflation"), r);
    }
}
