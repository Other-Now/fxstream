package com.fxstream.app.trades;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fxstream.app.trades.TradeService.TradeRequest;
import com.fxstream.app.trades.TradeService.TradeResult;
import com.rabbitmq.client.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageBuilder;
import org.springframework.amqp.core.MessageDeliveryMode;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.time.Clock;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

/**
 * Consumes trade requests with manual acks, in this order:
 * <ol>
 *   <li>decide + persist (idempotent - see {@link TradeService});</li>
 *   <li>publish the result and <b>wait for the broker's confirm</b>;</li>
 *   <li>only then ack the request.</li>
 * </ol>
 * A crash anywhere before step 3 means the request is redelivered; step 1 then replays the stored
 * outcome instead of trading twice, and step 2 republishes it (clients de-duplicate results by order
 * id). That is at-least-once delivery turned into exactly-once <em>effect</em>.
 *
 * <p>An unparseable message is rejected without requeue, which dead-letters it to the DLQ.
 */
@Component
public class TradeListener {
    private static final Logger log = LoggerFactory.getLogger(TradeListener.class);

    private final TradeService trades;
    private final RabbitTemplate rabbit;
    private final ObjectMapper json;
    private final Clock clock;
    private final LongAdder poison = new LongAdder(), retried = new LongAdder();

    public TradeListener(TradeService trades, RabbitTemplate rabbit, ObjectMapper json, Clock clock) {
        this.trades = trades;
        this.rabbit = rabbit;
        this.json = json;
        this.clock = clock;
    }

    @RabbitListener(queues = AmqpConfig.REQUESTS, containerFactory = "tradeListenerFactory")
    public void onRequest(Message message, Channel channel) throws IOException {
        long tag = message.getMessageProperties().getDeliveryTag();
        TradeRequest req;
        try {
            req = json.readValue(message.getBody(), TradeRequest.class);
            if (req.clientOrderId() == null || req.clientId() == null || req.quoteId() == null) {
                throw new IllegalArgumentException("missing field");
            }
        } catch (Exception e) {
            poison.increment();
            log.warn("dead-lettering unparseable trade request: {}", e.toString());
            channel.basicReject(tag, false);
            return;
        }
        try {
            TradeResult result = trades.process(req, clock.instant().truncatedTo(ChronoUnit.MICROS));
            publishConfirmed(result);
            channel.basicAck(tag, false);
        } catch (Exception e) {
            retried.increment();
            log.warn("trade {} not completed, will be redelivered: {}", req.clientOrderId(), e.toString());
            try {
                channel.basicNack(tag, false, true);
            } catch (Exception closed) {
                // channel already gone with the broker; the unacked message is requeued anyway
            }
        }
    }

    private void publishConfirmed(TradeResult r) throws Exception {
        Message msg = MessageBuilder.withBody(json.writeValueAsBytes(r))
                .setContentType("application/json")
                .setDeliveryMode(MessageDeliveryMode.PERSISTENT)
                .setMessageId(r.clientOrderId())
                .build();
        CorrelationData cd = new CorrelationData(r.clientOrderId());
        rabbit.send(AmqpConfig.RESULTS_EXCHANGE, "result." + r.clientId(), msg, cd);
        CorrelationData.Confirm c = cd.getFuture().get(5, TimeUnit.SECONDS);
        if (!c.isAck()) throw new IllegalStateException("result nacked by broker: " + c.getReason());
    }

    public long poison() { return poison.sum(); }
    public long retried() { return retried.sum(); }
}
