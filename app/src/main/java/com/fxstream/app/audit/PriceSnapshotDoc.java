package com.fxstream.app.audit;

import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.index.CompoundIndex;
import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.mapping.Document;

import java.time.Instant;
import java.util.List;

/**
 * Once a second per pair: the composite and every LP's last quote, fresh or stale. This is what you
 * would pull up to answer "what did we show at 10:31:07 and why" - a variable-shaped, append-only
 * record that suits a document store far better than a relational table. Expires after 7 days.
 */
@Document("price_snapshots")
@CompoundIndex(name = "pair_ts", def = "{'pair': 1, 'ts': -1}")
public record PriceSnapshotDoc(
        @Id String id,
        String pair,
        @Indexed(expireAfter = "7d") Instant ts,
        String bestBid,
        String bestAsk,
        int freshLps,
        List<LpQuote> lps) {

    public record LpQuote(int lp, String bid, String ask, boolean fresh) {}
}
