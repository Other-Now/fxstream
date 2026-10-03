package com.fxstream.core;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BookAndPricerTest {
    static final long MS = 1_000_000L;
    final Pair eur = Pair.bySymbol("EURUSD"), jpy = Pair.bySymbol("USDJPY");

    @Test
    void pxRoundTripsAndRejectsJunk() {
        assertThat(Px.parse("1.08345")).isEqualTo(108345);
        assertThat(Px.parse("151.2")).isEqualTo(15120000);
        assertThat(Px.parse("1")).isEqualTo(100000);
        assertThat(Px.format(108345)).isEqualTo("1.08345");
        assertThat(Px.format(15120000)).isEqualTo("151.20000");
        assertThatThrownBy(() -> Px.parse("1.083451")).isInstanceOf(NumberFormatException.class);
        assertThatThrownBy(() -> Px.parse("1.0a")).isInstanceOf(NumberFormatException.class);
    }

    @Test
    void bestBidAndOfferMayComeFromDifferentProviders() {
        Book b = new Book(3, 200 * MS);
        b.apply(0, 108340, 108350, 1_000_000, 1_000_000, 0);
        b.apply(1, 108342, 108353, 2_000_000, 2_000_000, 0);
        b.apply(2, 108338, 108348, 3_000_000, 3_000_000, 0);
        assertThat(b.recompute(1)).isTrue();
        assertThat(b.bestBid()).isEqualTo(108342);
        assertThat(b.bestBidLp()).isEqualTo(1);
        assertThat(b.bestAsk()).isEqualTo(108348);
        assertThat(b.bestAskLp()).isEqualTo(2);
        assertThat(b.freshLps()).isEqualTo(3);
        assertThat(b.recompute(2)).as("nothing changed").isFalse();
    }

    @Test
    void staleProviderDropsOutWithoutAnyNewTick() {
        Book b = new Book(2, 200 * MS);
        b.apply(0, 108345, 108350, 1, 1, 0);           // best bid, goes quiet
        b.apply(1, 108340, 108352, 1, 1, 150 * MS);
        b.recompute(150 * MS);
        assertThat(b.bestBid()).isEqualTo(108345);

        assertThat(b.recompute(200 * MS)).as("exactly at the threshold: still fresh").isFalse();
        assertThat(b.recompute(201 * MS)).isTrue();
        assertThat(b.bestBid()).isEqualTo(108340);
        assertThat(b.freshLps()).isEqualTo(1);

        b.apply(0, 108346, 108349, 1, 1, 300 * MS);   // comes back
        b.recompute(300 * MS);
        assertThat(b.bestBid()).isEqualTo(108346);
        assertThat(b.freshLps()).isEqualTo(2);
    }

    @Test
    void withdrawnSideAndCrossedBook() {
        Book b = new Book(2, 200 * MS);
        b.apply(0, 0, 108350, 0, 1, 0);                // offer only
        b.recompute(0);
        assertThat(b.isTwoWay()).isFalse();
        b.apply(1, 108355, 108360, 1, 1, 0);           // bids through LP0's offer
        b.recompute(0);
        assertThat(b.isCrossed()).isTrue();
    }

    @Test
    void markupScalesWithTierSizeAndPipSize() {
        Pricer p = Pricer.defaults();
        // tier 0, 1M: 0.2 pip = 2 units on EURUSD
        assertThat(p.clientBid(eur, 0, 1_000_000, 108345)).isEqualTo(108343);
        assertThat(p.clientAsk(eur, 0, 1_000_000, 108350)).isEqualTo(108352);
        // tier 2 (1.0 pip) at 5M (+0.3 pip) = 1.3 pip = 13 units
        assertThat(p.markupUnits(eur, 2, 5_000_000)).isEqualTo(13);
        // >5M (+1.0 pip) on tier 1 (0.5) = 1.5 pip
        assertThat(p.markupUnits(eur, 1, 10_000_000)).isEqualTo(15);
        // same 0.2 pip on USDJPY is 0.002 = 200 units
        assertThat(p.markupUnits(jpy, 0, 1)).isEqualTo(200);
    }

    @Test
    void quoteCheck() {
        long tol = 5;
        assertThat(QuoteCheck.check(999, 1000, 108350, 108350, tol)).isEqualTo(QuoteCheck.Verdict.FILLED);
        assertThat(QuoteCheck.check(1000, 1000, 108350, 108350, tol)).isEqualTo(QuoteCheck.Verdict.REJECTED_EXPIRED);
        assertThat(QuoteCheck.check(999, 1000, 108350, 108355, tol)).isEqualTo(QuoteCheck.Verdict.FILLED);
        assertThat(QuoteCheck.check(999, 1000, 108350, 108356, tol)).isEqualTo(QuoteCheck.Verdict.REJECTED_PRICE_MOVED);
        assertThat(QuoteCheck.check(999, 1000, 108350, 108344, tol)).isEqualTo(QuoteCheck.Verdict.REJECTED_PRICE_MOVED);
        assertThat(QuoteCheck.check(999, 1000, 108350, 0, tol)).isEqualTo(QuoteCheck.Verdict.REJECTED_NO_PRICE);
        // expiry wins over everything else
        assertThat(QuoteCheck.check(5000, 1000, 108350, 0, tol)).isEqualTo(QuoteCheck.Verdict.REJECTED_EXPIRED);
    }
}
