package com.fxstream.bench;

import com.fxstream.core.Pair;
import com.fxstream.core.Px;
import quickfix.Application;
import quickfix.DefaultMessageFactory;
import quickfix.MemoryStoreFactory;
import quickfix.Message;
import quickfix.Session;
import quickfix.SessionID;
import quickfix.SessionSettings;
import quickfix.ThreadedSocketInitiator;
import quickfix.field.MsgType;

import java.util.List;
import java.util.SplittableRandom;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.concurrent.locks.LockSupport;

/**
 * Simulated liquidity providers: N FIX 4.4 initiator sessions (LP1..LPn) streaming Quote (35=S)
 * messages for a set of pairs.
 *
 * <p>A shared "market" thread random-walks each pair's mid; every LP quotes mid +- (half spread + its own
 * small random skew), so LPs disagree a little (the composite has something to do) but rarely cross.
 *
 * <p>Each LP sends on an open-loop schedule: tick k is due at {@code start + k * interval}, and tag 5050
 * carries that <em>scheduled</em> time. If the sender falls behind, the lateness is counted as latency
 * rather than hidden - the coordinated-omission rule.
 */
final class LpFleet implements AutoCloseable {
    private final int n;
    private final List<Pair> pairs;
    private final SessionID[] ids;
    private final ThreadedSocketInitiator initiator;
    private final AtomicLongArray mid;          // by pair id
    private final long[] skewUnits;             // per LP: positive improves its bid (used by the stale bench)
    private final Thread market;
    private volatile boolean marketRunning = true;
    private Thread[] senders;
    private volatile boolean sending;
    private final boolean[] paused;
    private final long[] lastSentNanos;   // written by the sender thread, read after long sleeps
    final org.HdrHistogram.SynchronizedHistogram senderLag = new org.HdrHistogram.SynchronizedHistogram(3_600_000_000_000L, 3);

    LpFleet(String host, int port, int n, List<Pair> pairs, long marketStepMs) throws Exception {
        this.n = n;
        this.pairs = pairs;
        this.ids = new SessionID[n];
        this.skewUnits = new long[n];
        this.paused = new boolean[n];
        this.lastSentNanos = new long[n];
        this.mid = new AtomicLongArray(Pair.ALL.size());
        for (Pair p : Pair.ALL) mid.set(p.id(), switch (p.symbol()) {
            case "EURUSD" -> 108_300L;
            case "GBPUSD" -> 127_000L;
            case "USDJPY" -> 15_000_000L;
            default -> 66_000L;
        });

        SessionSettings s = new SessionSettings();
        s.setString("ConnectionType", "initiator");
        s.setString("SocketConnectHost", host);
        s.setLong("SocketConnectPort", port);
        s.setString("StartTime", "00:00:00");
        s.setString("EndTime", "00:00:00");
        s.setLong("HeartBtInt", 30);
        s.setLong("ReconnectInterval", 1);
        s.setString("UseDataDictionary", "N");
        s.setString("ResetOnLogon", "Y");
        s.setString("PersistMessages", "N");
        s.setString("SocketTcpNoDelay", "Y");
        for (int i = 0; i < n; i++) {
            ids[i] = new SessionID("FIX.4.4", "LP" + (i + 1), "FXSTREAM");
            s.setString(ids[i], "BeginString", "FIX.4.4");
        }
        initiator = new ThreadedSocketInitiator(new NoopApp(), new MemoryStoreFactory(), s, NO_LOG, new DefaultMessageFactory());
        initiator.start();
        long deadline = System.nanoTime() + 15_000_000_000L;
        for (SessionID id : ids) {
            while (Session.lookupSession(id) == null || !Session.lookupSession(id).isLoggedOn()) {
                if (System.nanoTime() > deadline) throw new IllegalStateException("LP " + id + " did not log on");
                Thread.sleep(20);
            }
        }

        market = new Thread(() -> {
            SplittableRandom r = new SplittableRandom(42);
            while (marketRunning) {
                for (Pair p : pairs) mid.addAndGet(p.id(), (r.nextInt(3) - 1) * (p.pipUnits() / 10));
                LockSupport.parkNanos(marketStepMs * 1_000_000L);
            }
        }, "market");
        market.setDaemon(true);
        market.start();
    }

    void skew(int lp, long units) { skewUnits[lp] = units; }
    void pause(int lp, boolean p) { paused[lp] = p; }
    long lastSentNanos(int lp) { return lastSentNanos[lp]; }
    long mid(Pair p) { return mid.get(p.id()); }

    /**
     * Start streaming {@code totalRate} ticks/s, spread round-robin over LPs and pairs. One sender thread
     * for the whole fleet: it spins/yields for short waits instead of parking, because on Windows a
     * timed park sleeps a whole ~15.8 ms scheduler tick and the harness would measure its own lateness.
     */
    void start(double totalRate) {
        sending = true;
        long intervalNanos = (long) (1e9 / totalRate);
        senders = new Thread[] {new Thread(() -> stream(intervalNanos), "lp-sender")};
        senders[0].setDaemon(true);
        senders[0].start();
    }

    void stop() throws InterruptedException {
        sending = false;
        if (senders != null) for (Thread t : senders) t.join();
        senders = null;
    }

    private void stream(long interval) {
        Session[] sessions = new Session[n];
        for (int i = 0; i < n; i++) sessions[i] = Session.lookupSession(ids[i]);
        SplittableRandom r = new SplittableRandom(7919L);
        long next = System.nanoTime();
        long k = 0;
        while (sending) {
            long wait = next - System.nanoTime();
            if (wait > 20_000_000) { Out.sleep(10); continue; }
            if (wait > 50_000) { Thread.yield(); continue; }
            if (wait > 0) { Thread.onSpinWait(); continue; }
            int lp = (int) (k % n);
            Pair p = pairs.get((int) ((k / n) % pairs.size()));
            k++;
            if (!paused[lp]) {
                long halfSpread = p.pipUnits() / 2;                     // 0.5 pip each side
                long m = mid.get(p.id());
                long bid = m - halfSpread - r.nextLong(p.pipUnits() / 5 + 1) + skewUnits[lp]; // up to 0.2 pip jitter
                long ask = m + halfSpread + r.nextLong(p.pipUnits() / 5 + 1);
                sessions[lp].send(quote(p, bid, ask, next));
                long sent = System.nanoTime();
                lastSentNanos[lp] = sent;
                senderLag.recordValue(Math.max(0, sent - next));
            }
            next += interval;
        }
    }

    static Message quote(Pair p, long bid, long ask, long originNanos) {
        Message m = new Message();
        m.getHeader().setString(MsgType.FIELD, MsgType.QUOTE);
        m.setString(117, "q");                 // QuoteID
        m.setString(55, p.symbol());
        m.setString(132, Px.format(bid));
        m.setString(133, Px.format(ask));
        m.setString(134, "1000000");
        m.setString(135, "1000000");
        m.setString(5050, Long.toString(originNanos));
        return m;
    }

    @Override
    public void close() throws Exception {
        stop();
        marketRunning = false;
        initiator.stop(true);
    }

    private static final quickfix.LogFactory NO_LOG = id -> new quickfix.Log() {
        @Override public void clear() {}
        @Override public void onIncoming(String m) {}
        @Override public void onOutgoing(String m) {}
        @Override public void onEvent(String m) {}
        @Override public void onErrorEvent(String m) { System.err.println(id + " " + m); }
    };

    private static final class NoopApp implements Application {
        @Override public void onCreate(SessionID s) {}
        @Override public void onLogon(SessionID s) {}
        @Override public void onLogout(SessionID s) {}
        @Override public void toAdmin(Message m, SessionID s) {}
        @Override public void fromAdmin(Message m, SessionID s) {}
        @Override public void toApp(Message m, SessionID s) {}
        @Override public void fromApp(Message m, SessionID s) {}
    }
}
