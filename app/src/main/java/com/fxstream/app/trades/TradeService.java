package com.fxstream.app.trades;

import com.fxstream.app.FxProperties;
import com.fxstream.app.audit.QuoteDoc;
import com.fxstream.app.quotes.QuoteService;
import com.fxstream.core.Pair;
import com.fxstream.core.QuoteCheck;
import com.fxstream.core.QuoteCheck.Verdict;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.atomic.LongAdder;

/**
 * Decides a trade request against its quote and records the outcome exactly once.
 *
 * <p>Idempotency is two layers. The fast path looks the order id up first and replays the stored
 * answer. That alone has a race (two deliveries of the same request on two consumer threads both miss
 * the lookup), so the real guarantee is the UNIQUE constraint: the second insert fails, and the loser
 * reads back the winner's row. No transaction spans the lookup and insert - the database constraint
 * is the lock.
 */
@Service
public class TradeService {
    private final FxProperties fx;
    private final QuoteService quotes;
    private final TradeRepository repo;
    private final LongAdder filled = new LongAdder(), rejected = new LongAdder(), replayed = new LongAdder(),
            raceCaught = new LongAdder();

    public TradeService(FxProperties fx, QuoteService quotes, TradeRepository repo) {
        this.fx = fx;
        this.quotes = quotes;
        this.repo = repo;
    }

    public record TradeRequest(String clientOrderId, String clientId, String quoteId) {}

    public record TradeResult(String clientOrderId, String clientId, Long tradeId, String status, String reason, String pair,
                              String side, Long qty, Long price, String quoteId, boolean replay) {
        static TradeResult of(Trade t, boolean replay) {
            return new TradeResult(t.getClientOrderId(), t.getClientId(), t.getId(), t.getStatus(), t.getReason(), t.getPair(),
                    t.getSide(), t.getQty(), t.getPrice(), t.getQuoteId(), replay);
        }
    }

    public TradeResult process(TradeRequest req, Instant receivedAt) {
        if (fx.idempotent()) {
            var existing = repo.findByDedupeKey(req.clientOrderId());
            if (existing.isPresent()) {
                replayed.increment();
                return TradeResult.of(existing.get(), true);
            }
        }

        QuoteDoc q = quotes.find(req.quoteId());
        Verdict v;
        if (q == null || !q.clientId().equals(req.clientId())) {
            v = Verdict.REJECTED_UNKNOWN_QUOTE;
        } else {
            long tolerance = Pair.bySymbol(q.pair()).tenthPipsToUnits(fx.toleranceTenthPips());
            v = QuoteCheck.check(micros(receivedAt), micros(q.expiresAt()), q.price(),
                    quotes.currentPrice(q), tolerance);
        }
        boolean fill = v == Verdict.FILLED;
        String dedupeKey = fx.idempotent() ? req.clientOrderId() : UUID.randomUUID().toString();
        Trade t = new Trade(dedupeKey, req.clientOrderId(), req.clientId(), req.quoteId(),
                q == null ? null : q.pair(), q == null ? null : q.side(), q == null ? null : q.qty(),
                q == null ? null : q.price(), fill ? "FILLED" : "REJECTED", fill ? null : v.name(),
                receivedAt, q == null ? null : q.expiresAt());
        try {
            t = repo.saveAndFlush(t);
        } catch (DataIntegrityViolationException dup) {
            raceCaught.increment();
            return TradeResult.of(repo.findByDedupeKey(dedupeKey).orElseThrow(() -> dup), true);
        }
        (fill ? filled : rejected).increment();
        if (q != null) quotes.recordOutcome(q, v.name(), req.clientOrderId());
        return TradeResult.of(t, false);
    }

    /** Postgres keeps microseconds, so decide at exactly the precision the row will be audited at. */
    private static long micros(Instant i) {
        return i.getEpochSecond() * 1_000_000L + i.getNano() / 1_000;
    }

    public long filled() { return filled.sum(); }
    public long rejected() { return rejected.sum(); }
    public long replayed() { return replayed.sum(); }
    public long raceCaught() { return raceCaught.sum(); }
}
