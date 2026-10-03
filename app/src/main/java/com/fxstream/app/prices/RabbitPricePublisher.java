package com.fxstream.app.prices;

import com.fxstream.core.Pair;
import com.fxstream.core.PriceCodec;
import com.fxstream.core.PriceSink;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.amqp.RabbitProperties;

import java.io.IOException;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Consumer;

/**
 * Streams client prices to the {@code fx.prices} topic exchange with routing key
 * {@code price.<PAIR>.<tier>}, so a client binds {@code price.EURUSD.0} or {@code price.*.1}.
 *
 * <p>This uses the raw RabbitMQ client rather than Spring's RabbitTemplate: one channel per pair thread
 * (channels are not thread-safe, and one-per-thread needs no lock), one reused 48-byte body per pair,
 * routing keys built once. Prices are transient (delivery mode 1) and fire-and-forget - a price is
 * superseded within milliseconds, so it is never worth a disk write or a confirm round trip. Trades get
 * the opposite treatment; see {@code TradeListener}.
 *
 * <p>If the broker is down, publishes are dropped and counted; the client library reconnects on its own
 * and the next book change goes out normally.
 */
public final class RabbitPricePublisher implements PriceSink, AutoCloseable {
    public static final String EXCHANGE = "fx.prices";
    private static final Logger log = LoggerFactory.getLogger(RabbitPricePublisher.class);
    private static final AMQP.BasicProperties PROPS = new AMQP.BasicProperties.Builder()
            .deliveryMode(1).contentType("application/x-fxstream-price").build();

    private final Connection connection;
    private final Channel[] channels;
    private final byte[][] buffers;
    private final String[][] routingKeys;
    private final Consumer<BookSnapshot> snapshots;
    private final LongAdder published = new LongAdder(), dropped = new LongAdder();

    public RabbitPricePublisher(RabbitProperties rabbit, int tiers, Consumer<BookSnapshot> snapshots) throws Exception {
        ConnectionFactory cf = new ConnectionFactory();
        cf.setHost(rabbit.determineHost());
        cf.setPort(rabbit.determinePort());
        cf.setUsername(rabbit.determineUsername());
        cf.setPassword(rabbit.determinePassword());
        cf.setAutomaticRecoveryEnabled(true);
        cf.setNetworkRecoveryInterval(500);
        this.connection = cf.newConnection("fxstream-prices");
        try (Channel c = connection.createChannel()) {
            c.exchangeDeclare(EXCHANGE, "topic", true);
        }
        int n = Pair.ALL.size();
        channels = new Channel[n];
        buffers = new byte[n][PriceCodec.SIZE];
        routingKeys = new String[n][tiers];
        for (Pair p : Pair.ALL) {
            channels[p.id()] = connection.createChannel();
            for (int t = 0; t < tiers; t++) routingKeys[p.id()][t] = "price." + p.symbol() + "." + t;
        }
        this.snapshots = snapshots;
    }

    @Override
    public void onPrice(Pair pair, int tier, long seq, long bid, long ask, int freshLps, long originNanos) {
        byte[] body = buffers[pair.id()];
        PriceCodec.encode(body, pair.id(), tier, seq, bid, ask, freshLps, originNanos);
        try {
            // basicPublish frames and writes the body before returning, so reusing the buffer is safe
            channels[pair.id()].basicPublish(EXCHANGE, routingKeys[pair.id()][tier], PROPS, body);
            published.increment();
        } catch (IOException | RuntimeException e) { // AlreadyClosedException while the broker is down
            dropped.increment();
        }
    }

    @Override
    public void onSnapshot(BookSnapshot snapshot) {
        snapshots.accept(snapshot);
    }

    public long published() { return published.sum(); }
    public long dropped() { return dropped.sum(); }

    @Override
    public void close() {
        try {
            connection.close(2_000);
        } catch (Exception e) {
            log.debug("close", e);
        }
    }
}
