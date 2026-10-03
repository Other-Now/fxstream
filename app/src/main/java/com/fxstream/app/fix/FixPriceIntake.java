package com.fxstream.app.fix;

import com.fxstream.app.FxProperties;
import com.fxstream.app.prices.PricingEngines;
import com.fxstream.core.Pair;
import com.fxstream.core.Px;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Component;
import quickfix.Application;
import quickfix.ConfigError;
import quickfix.FieldNotFound;
import quickfix.MemoryStoreFactory;
import quickfix.Message;
import quickfix.SessionID;
import quickfix.SessionSettings;
import quickfix.ThreadedSocketAcceptor;
import quickfix.DefaultMessageFactory;
import quickfix.field.MsgType;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;

/**
 * FIX 4.4 acceptor for liquidity-provider price streams. Each LP logs on as LP1..LPn and streams
 * Quote (35=S) messages: Symbol(55), BidPx(132), OfferPx(133), BidSize(134), OfferSize(135).
 * Tag 5050 is a custom field carrying the sender's timestamp, which is echoed onto the client price
 * untouched so the benchmark can measure tick-to-client latency end to end.
 *
 * <p>{@link ThreadedSocketAcceptor} gives every session its own thread, so each LP is one producer into
 * each pair's ring. Parsing is QuickFIX/J's and does allocate (message objects, field strings); the
 * zero-allocation claim starts at the ring, and the README says so.
 */
@Component
public class FixPriceIntake implements Application, SmartLifecycle {
    public static final int TAG_ORIGIN_NANOS = 5050;
    private static final Logger log = LoggerFactory.getLogger(FixPriceIntake.class);

    private final FxProperties fx;
    private final PricingEngines engines;
    private final ConcurrentHashMap<SessionID, Integer> lpIndex = new ConcurrentHashMap<>();
    private final LongAdder quotes = new LongAdder(), rejected = new LongAdder();
    private ThreadedSocketAcceptor acceptor;
    private volatile boolean running;

    public FixPriceIntake(FxProperties fx, PricingEngines engines) {
        this.fx = fx;
        this.engines = engines;
    }

    SessionSettings settings() {
        SessionSettings s = new SessionSettings();
        s.setString("ConnectionType", "acceptor");
        s.setLong("SocketAcceptPort", fx.fixPort());
        s.setString("StartTime", "00:00:00");
        s.setString("EndTime", "00:00:00");
        s.setLong("HeartBtInt", 30);
        s.setString("UseDataDictionary", "N");   // no per-field validation on the hot path
        s.setString("ResetOnLogon", "Y");
        s.setString("PersistMessages", "N");     // we never resend app messages to an LP
        s.setString("SocketTcpNoDelay", "Y");
        for (int i = 1; i <= fx.maxLps(); i++) {
            SessionID id = new SessionID("FIX.4.4", "FXSTREAM", "LP" + i);
            s.setString(id, "BeginString", "FIX.4.4");
            lpIndex.put(id, i - 1);
        }
        return s;
    }

    @Override
    public void fromApp(Message message, SessionID sessionId) {
        try {
            if (!MsgType.QUOTE.equals(message.getHeader().getString(MsgType.FIELD))) return;
            Integer lp = lpIndex.get(sessionId);
            Pair pair = Pair.bySymbol(message.getString(55));
            if (lp == null || pair == null) { rejected.increment(); return; }
            long bid = message.isSetField(132) ? Px.parse(message.getString(132)) : 0;
            long ask = message.isSetField(133) ? Px.parse(message.getString(133)) : 0;
            long bidQty = message.isSetField(134) ? Long.parseLong(message.getString(134)) : 0;
            long askQty = message.isSetField(135) ? Long.parseLong(message.getString(135)) : 0;
            long origin = message.isSetField(TAG_ORIGIN_NANOS) ? Long.parseLong(message.getString(TAG_ORIGIN_NANOS)) : 0;
            engines.engine(pair).offer(lp, bid, ask, bidQty, askQty, origin);
            quotes.increment();
        } catch (FieldNotFound | NumberFormatException e) {
            rejected.increment();
        }
    }

    public long quotes() { return quotes.sum(); }
    public long rejected() { return rejected.sum(); }

    @Override
    public void start() {
        try {
            acceptor = new ThreadedSocketAcceptor(this, new MemoryStoreFactory(), settings(),
                    new QuietFixLogFactory(), new DefaultMessageFactory());
            acceptor.start();
            running = true;
            log.info("FIX acceptor listening on {} for LP1..LP{}", fx.fixPort(), fx.maxLps());
        } catch (ConfigError e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public void stop() {
        if (acceptor != null) acceptor.stop(true);
        running = false;
    }

    @Override
    public boolean isRunning() { return running; }

    @Override public void onCreate(SessionID sessionId) {}
    @Override public void onLogon(SessionID sessionId) { log.info("LP logon {}", sessionId.getTargetCompID()); }
    @Override public void onLogout(SessionID sessionId) { log.info("LP logout {}", sessionId.getTargetCompID()); }
    @Override public void toAdmin(Message message, SessionID sessionId) {}
    @Override public void fromAdmin(Message message, SessionID sessionId) {}
    @Override public void toApp(Message message, SessionID sessionId) {}
}
