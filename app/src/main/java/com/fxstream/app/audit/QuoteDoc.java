package com.fxstream.app.audit;

import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.mapping.Document;

import java.time.Instant;

/**
 * Every quote issued, with the composite it was priced from, and later the outcome of the trade (if
 * any) made against it. Also the fallback store for quote lookups after a restart.
 */
@Document("quotes")
public record QuoteDoc(
        @Id String quoteId,
        @Indexed String clientId,
        int tier,
        String pair,
        String side,
        long qty,
        long price,
        long bestBid,
        long bestAsk,
        Instant issuedAt,
        Instant expiresAt,
        String outcome,
        String clientOrderId) {

    public QuoteDoc withOutcome(String outcome, String clientOrderId) {
        return new QuoteDoc(quoteId, clientId, tier, pair, side, qty, price, bestBid, bestAsk, issuedAt,
                expiresAt, outcome, clientOrderId);
    }
}
