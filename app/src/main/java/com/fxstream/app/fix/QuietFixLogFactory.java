package com.fxstream.app.fix;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import quickfix.Log;
import quickfix.LogFactory;
import quickfix.SessionID;

/**
 * Session events and errors go to SLF4J; the per-message incoming/outgoing log is dropped.
 * Without an explicit LogFactory QuickFIX/J prints every message to stdout, which at 10k ticks/s is
 * the single most expensive thing in the process.
 */
public final class QuietFixLogFactory implements LogFactory {
    private static final Logger log = LoggerFactory.getLogger("fix.session");

    @Override
    public Log create(SessionID id) {
        return new Log() {
            @Override public void clear() {}
            @Override public void onIncoming(String m) {}
            @Override public void onOutgoing(String m) {}
            @Override public void onEvent(String m) { log.debug("{} {}", id, m); }
            @Override public void onErrorEvent(String m) { log.warn("{} {}", id, m); }
        };
    }
}
