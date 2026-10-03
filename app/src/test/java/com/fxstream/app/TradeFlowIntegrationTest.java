package com.fxstream.app;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fxstream.app.audit.QuoteDoc;
import com.fxstream.app.audit.QuoteRepository;
import com.fxstream.app.prices.PricingEngines;
import com.fxstream.app.quotes.QuoteService;
import com.fxstream.app.trades.AmqpConfig;
import com.fxstream.app.trades.TradeRepository;
import com.fxstream.core.Pair;
import com.fxstream.core.QuoteCheck.Side;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageBuilder;
import org.springframework.amqp.core.TopicExchange;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import quickfix.Application;
import quickfix.DefaultMessageFactory;
import quickfix.MemoryStoreFactory;
import quickfix.ScreenLogFactory;
import quickfix.Session;
import quickfix.SessionID;
import quickfix.SessionSettings;
import quickfix.SocketInitiator;
import quickfix.field.MsgType;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Full-stack tests against real Postgres, MongoDB and RabbitMQ (local services, or the service
 * containers in CI). Prices are injected straight into the EUR/USD engine as "LP8" so each test controls
 * the market; one test goes in over a real FIX session instead.
 */
@SpringBootTest(properties = {"fx.fix-port=19878", "server.port=0"})
class TradeFlowIntegrationTest {
    static final int TEST_LP = 7; // LP8
    final Pair eur = Pair.bySymbol("EURUSD");

    @Autowired PricingEngines engines;
    @Autowired QuoteService quotes;
    @Autowired QuoteRepository quoteDocs;
    @Autowired TradeRepository trades;
    @Autowired RabbitTemplate rabbit;
    @Autowired RabbitAdmin admin;
    @Autowired ObjectMapper json;

    ScheduledExecutorService feeder;
    volatile long mid = 108_300;
    String client;
    String resultQueue;

    @BeforeEach
    void setUp() {
        client = "it-" + UUID.randomUUID().toString().substring(0, 8);
        // not AnonymousQueue: that is auto-delete, and receive(timeout) subscribes and cancels each call
        resultQueue = admin.declareQueue(new org.springframework.amqp.core.Queue("it.results." + client, false, false, false));
        admin.declareBinding(BindingBuilder.bind(new org.springframework.amqp.core.Queue(resultQueue))
                .to(new TopicExchange(AmqpConfig.RESULTS_EXCHANGE)).with("result." + client));
        feeder = Executors.newSingleThreadScheduledExecutor();
        feeder.scheduleAtFixedRate(() -> engines.engine(eur).offer(TEST_LP, mid - 5, mid + 5, 1_000_000, 1_000_000, 0),
                0, 20, TimeUnit.MILLISECONDS);
        awaitTrue(() -> engines.engine(eur).bestBid() == mid - 5); // not just any bid: the previous test may have moved it
    }

    @AfterEach
    void tearDown() {
        feeder.shutdownNow();
        admin.deleteQueue(resultQueue);
    }

    void sendTrade(String orderId, String quoteId) throws Exception {
        rabbit.send(AmqpConfig.TRADES_EXCHANGE, "trade.request", MessageBuilder.withBody(json.writeValueAsBytes(
                Map.of("clientOrderId", orderId, "clientId", client, "quoteId", quoteId))).build());
    }

    @SuppressWarnings("unchecked")
    Map<String, Object> nextResult() throws Exception {
        Message m = rabbit.receive(resultQueue, 10_000);
        assertThat(m).as("trade result within 10s").isNotNull();
        return json.readValue(m.getBody(), Map.class);
    }

    @Test
    void fillsOnceAndReplaysTheSameAnswerForADuplicateRequest() throws Exception {
        QuoteDoc q = quotes.issue(client, "EURUSD", Side.BUY, 1_000_000, 10_000L);
        assertThat(q.price()).as("best offer + 1.0 pip: unknown clients get tier 2").isEqualTo(mid + 5 + 10);
        String orderId = client + "-1";
        sendTrade(orderId, q.quoteId());
        sendTrade(orderId, q.quoteId());

        Map<String, Object> first = nextResult(), second = nextResult();
        assertThat(first.get("status")).isEqualTo("FILLED");
        assertThat(second.get("status")).isEqualTo("FILLED");
        assertThat(second.get("tradeId")).isEqualTo(first.get("tradeId"));
        assertThat(java.util.List.of(first.get("replay"), second.get("replay"))).containsExactlyInAnyOrder(false, true);
        assertThat(trades.findByClientOrderId(orderId)).hasSize(1);
        awaitTrue(() -> "FILLED".equals(quoteDocs.findById(q.quoteId()).map(QuoteDoc::outcome).orElse(null)));
    }

    @Test
    void tradeAfterExpiryIsRejected() throws Exception {
        QuoteDoc q = quotes.issue(client, "EURUSD", Side.SELL, 1_000_000, 50L);
        Thread.sleep(120);
        sendTrade(client + "-exp", q.quoteId());
        Map<String, Object> r = nextResult();
        assertThat(r.get("status")).isEqualTo("REJECTED");
        assertThat(r.get("reason")).isEqualTo("REJECTED_EXPIRED");
    }

    @Test
    void tradeAfterThePriceMovedBeyondToleranceIsRejected() throws Exception {
        QuoteDoc q = quotes.issue(client, "EURUSD", Side.BUY, 1_000_000, 10_000L);
        mid += 30; // 3 pips; tolerance is 1 pip
        Thread.sleep(100);
        sendTrade(client + "-moved", q.quoteId());
        Map<String, Object> r = nextResult();
        assertThat(r.get("reason")).isEqualTo("REJECTED_PRICE_MOVED");
    }

    @Test
    void unknownQuoteIsRejectedNotDropped() throws Exception {
        sendTrade(client + "-ghost", "no-such-quote");
        assertThat(nextResult().get("reason")).isEqualTo("REJECTED_UNKNOWN_QUOTE");
    }

    @Test
    void unparseableRequestIsDeadLettered() {
        String marker = "not json " + UUID.randomUUID();
        rabbit.send(AmqpConfig.TRADES_EXCHANGE, "trade.request", MessageBuilder.withBody(marker.getBytes()).build());
        boolean found = false;
        long end = System.currentTimeMillis() + 10_000;
        while (!found && System.currentTimeMillis() < end) {
            Message m = rabbit.receive(AmqpConfig.DLQ, 500);
            if (m != null && new String(m.getBody()).equals(marker)) found = true;
        }
        assertThat(found).as("poison message reached " + AmqpConfig.DLQ).isTrue();
    }

    @Test
    void quoteSentOverFixReachesTheBook() throws Exception {
        SessionSettings s = new SessionSettings();
        s.setString("ConnectionType", "initiator");
        s.setString("SocketConnectHost", "localhost");
        s.setLong("SocketConnectPort", 19878);
        s.setString("StartTime", "00:00:00");
        s.setString("EndTime", "00:00:00");
        s.setLong("HeartBtInt", 30);
        s.setString("UseDataDictionary", "N");
        s.setString("ResetOnLogon", "Y");
        SessionID id = new SessionID("FIX.4.4", "LP1", "FXSTREAM");
        s.setString(id, "BeginString", "FIX.4.4");
        SocketInitiator init = new SocketInitiator(new NoopApp(), new MemoryStoreFactory(), s,
                new ScreenLogFactory(false, false, false), new DefaultMessageFactory());
        init.start();
        try {
            awaitTrue(() -> Session.lookupSession(id) != null && Session.lookupSession(id).isLoggedOn());
            quickfix.Message m = new quickfix.Message();
            m.getHeader().setString(MsgType.FIELD, MsgType.QUOTE);
            m.setString(117, "q1");
            m.setString(55, "GBPUSD");
            m.setString(132, "1.27001");
            m.setString(133, "1.27009");
            m.setString(134, "1000000");
            m.setString(135, "1000000");
            Session.lookupSession(id).send(m);
            Pair gbp = Pair.bySymbol("GBPUSD");
            awaitTrue(() -> engines.engine(gbp).bestBid() == 127_001 && engines.engine(gbp).bestAsk() == 127_009);
        } finally {
            init.stop(true);
        }
    }

    static void awaitTrue(java.util.function.BooleanSupplier c) {
        long end = System.currentTimeMillis() + 10_000;
        while (!c.getAsBoolean()) {
            if (System.currentTimeMillis() > end) throw new AssertionError("condition not met within 10s");
            try { Thread.sleep(20); } catch (InterruptedException e) { throw new RuntimeException(e); }
        }
    }

    static final class NoopApp implements Application {
        @Override public void onCreate(SessionID s) {}
        @Override public void onLogon(SessionID s) {}
        @Override public void onLogout(SessionID s) {}
        @Override public void toAdmin(quickfix.Message m, SessionID s) {}
        @Override public void fromAdmin(quickfix.Message m, SessionID s) {}
        @Override public void toApp(quickfix.Message m, SessionID s) {}
        @Override public void fromApp(quickfix.Message m, SessionID s) {}
    }
}
