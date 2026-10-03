package com.fxstream.core;

/**
 * Turns the composite book into client prices. Each client tier gets a base markup, widened further by
 * trade size, both configured in tenths of a pip so the same table works for EUR/USD (pip 0.0001) and
 * USD/JPY (pip 0.01). The markup is added on each side: client bid = best bid - markup, client offer =
 * best offer + markup.
 *
 * <p>Immutable after construction, so every pair thread can share one instance.
 */
public final class Pricer {
    private final long[] tierTenthPips;   // by tier: 0 = best client
    private final long[] bandMaxQty;      // ascending upper bounds, last should be Long.MAX_VALUE
    private final long[] bandTenthPips;   // extra markup for each size band

    public Pricer(long[] tierTenthPips, long[] bandMaxQty, long[] bandTenthPips) {
        if (bandMaxQty.length != bandTenthPips.length) throw new IllegalArgumentException("band arrays differ");
        this.tierTenthPips = tierTenthPips.clone();
        this.bandMaxQty = bandMaxQty.clone();
        this.bandTenthPips = bandTenthPips.clone();
    }

    /** Defaults: tiers 0.2 / 0.5 / 1.0 pip; up to 1M no extra, up to 5M +0.3 pip, above +1.0 pip. */
    public static Pricer defaults() {
        return new Pricer(new long[] {2, 5, 10},
                new long[] {1_000_000, 5_000_000, Long.MAX_VALUE},
                new long[] {0, 3, 10});
    }

    public int tiers() { return tierTenthPips.length; }

    public long markupUnits(Pair pair, int tier, long qty) {
        int band = 0;
        while (qty > bandMaxQty[band]) band++;
        return pair.tenthPipsToUnits(tierTenthPips[tier] + bandTenthPips[band]);
    }

    public long clientBid(Pair pair, int tier, long qty, long bestBid) {
        return bestBid - markupUnits(pair, tier, qty);
    }

    public long clientAsk(Pair pair, int tier, long qty, long bestAsk) {
        return bestAsk + markupUnits(pair, tier, qty);
    }
}
