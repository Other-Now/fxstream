package com.fxstream.app.trades;

import org.springframework.amqp.core.AcknowledgeMode;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.Declarables;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.QueueBuilder;
import org.springframework.amqp.core.TopicExchange;
import org.springframework.amqp.rabbit.config.SimpleRabbitListenerContainerFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Clock;

/**
 * Trade topology. Requests go to a durable <em>quorum</em> queue (Raft-replicated log; on one node it
 * still gives fsync-before-confirm and a delivery counter). A message that cannot be parsed, or that
 * keeps failing past the delivery limit, is dead-lettered to {@code fx.trade.requests.dlq} instead of
 * looping forever.
 *
 * <pre>
 *  client --(confirm)--> fx.trades [direct] --trade.request--> fx.trade.requests (quorum)
 *                                                                   | reject / delivery-limit
 *                                                                   v
 *                                   fx.dlx [direct] --trade.dead--> fx.trade.requests.dlq
 *  service --(confirm)--> fx.trade.results [topic] --result.&lt;clientId&gt;--> client's queue
 * </pre>
 */
@Configuration
public class AmqpConfig {
    public static final String TRADES_EXCHANGE = "fx.trades";
    public static final String REQUESTS = "fx.trade.requests";
    public static final String DLX = "fx.dlx";
    public static final String DLQ = "fx.trade.requests.dlq";
    public static final String RESULTS_EXCHANGE = "fx.trade.results";

    @Bean
    Declarables tradeTopology() {
        DirectExchange trades = new DirectExchange(TRADES_EXCHANGE, true, false);
        DirectExchange dlx = new DirectExchange(DLX, true, false);
        TopicExchange results = new TopicExchange(RESULTS_EXCHANGE, true, false);
        Queue requests = QueueBuilder.durable(REQUESTS).quorum()
                .deadLetterExchange(DLX).deadLetterRoutingKey("trade.dead")
                .deliveryLimit(20)
                .build();
        Queue dlq = QueueBuilder.durable(DLQ).quorum().build();
        Binding b1 = BindingBuilder.bind(requests).to(trades).with("trade.request");
        Binding b2 = BindingBuilder.bind(dlq).to(dlx).with("trade.dead");
        return new Declarables(trades, dlx, results, requests, dlq, b1, b2);
    }

    @Bean
    SimpleRabbitListenerContainerFactory tradeListenerFactory(ConnectionFactory cf) {
        SimpleRabbitListenerContainerFactory f = new SimpleRabbitListenerContainerFactory();
        f.setConnectionFactory(cf);
        f.setAcknowledgeMode(AcknowledgeMode.MANUAL);
        f.setConcurrentConsumers(4);
        f.setPrefetchCount(50);
        f.setRecoveryInterval(500L);   // reconnect quickly after a broker restart
        f.setMissingQueuesFatal(false);
        return f;
    }

    @Bean
    Clock clock() {
        return Clock.systemUTC();
    }
}
