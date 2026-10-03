package com.fxstream.app.quotes;

import com.fxstream.app.FxProperties;
import com.fxstream.app.audit.QuoteDoc;
import com.fxstream.app.audit.QuoteRepository;
import com.fxstream.app.prices.PricingEngines;
import com.fxstream.core.Pair;
import com.fxstream.core.PairEngine;
import com.fxstream.core.Pricer;
import com.fxstream.core.QuoteCheck.Side;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.Clock;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Issues firm quotes: a price for one client, pair, side and size, valid until {@code expiresAt}.
 * Live quotes are held in memory for the trade path; every quote is also written to MongoDB (audit, and
 * the fallback lookup if the service restarted between quote and trade).
 */
@Service
public class QuoteService {
    private static final Logger log = LoggerFactory.getLogger(QuoteService.class);

    private final FxProperties fx;
    private final PricingEngines engines;
    private final QuoteRepository repo;
    private final Clock clock;
    private final ConcurrentHashMap<String, QuoteDoc> live = new ConcurrentHashMap<>();

    public QuoteService(FxProperties fx, PricingEngines engines, QuoteRepository repo, Clock clock) {
        this.fx = fx;
        this.engines = engines;
        this.repo = repo;
        this.clock = clock;
    }

    public static class NoPriceException extends RuntimeException {
        public NoPriceException(String m) { super(m); }
    }

    public QuoteDoc issue(String clientId, String symbol, Side side, long qty, Long ttlMs) {
        Pair pair = Pair.bySymbol(symbol);
        if (pair == null) throw new IllegalArgumentException("unknown pair " + symbol);
        if (qty <= 0) throw new IllegalArgumentException("qty must be positive");
        PairEngine e = engines.engine(pair);
        long bestBid = e.bestBid(), bestAsk = e.bestAsk();
        int tier = fx.tierOf(clientId);
        long price = price(pair, tier, side, qty, bestBid, bestAsk);
        if (price <= 0) throw new NoPriceException("no two-way price for " + symbol);

        long ttl = Math.min(ttlMs == null ? fx.quoteTtlMs() : ttlMs, fx.maxQuoteTtlMs());
        Instant now = clock.instant().truncatedTo(ChronoUnit.MICROS);
        QuoteDoc q = new QuoteDoc(UUID.randomUUID().toString(), clientId, tier, pair.symbol(), side.name(), qty,
                price, bestBid, bestAsk, now, now.plusMillis(ttl), null, null);
        live.put(q.quoteId(), q);
        repo.save(q);
        return q;
    }

    public QuoteDoc find(String quoteId) {
        QuoteDoc q = live.get(quoteId);
        return q != null ? q : repo.findById(quoteId).orElse(null);
    }

    /** What the same client would be quoted for the same side and size right now; 0 if no price. */
    public long currentPrice(QuoteDoc q) {
        Pair pair = Pair.bySymbol(q.pair());
        PairEngine e = engines.engine(pair);
        return price(pair, q.tier(), Side.valueOf(q.side()), q.qty(), e.bestBid(), e.bestAsk());
    }

    private long price(Pair pair, int tier, Side side, long qty, long bestBid, long bestAsk) {
        if (bestBid <= 0 || bestAsk <= 0) return 0;
        Pricer p = engines.pricer();
        // client buys at our offer, sells at our bid
        return side == Side.BUY ? p.clientAsk(pair, tier, qty, bestAsk) : p.clientBid(pair, tier, qty, bestBid);
    }

    public void recordOutcome(QuoteDoc q, String outcome, String clientOrderId) {
        try {
            repo.save(q.withOutcome(outcome, clientOrderId));
        } catch (RuntimeException ex) {
            log.warn("quote outcome audit failed for {}: {}", q.quoteId(), ex.toString());
        }
    }

    @Scheduled(fixedDelay = 5_000)
    void evictExpired() {
        Instant cutoff = clock.instant().minusSeconds(60);
        live.values().removeIf(q -> q.expiresAt().isBefore(cutoff));
    }
}
