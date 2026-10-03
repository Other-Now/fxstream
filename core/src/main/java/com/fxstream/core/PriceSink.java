package com.fxstream.core;

/** Where a {@link PairEngine} sends its output. Called only from that pair's engine thread. */
public interface PriceSink {

    /**
     * A new streaming price for one client tier (at the smallest size band).
     *
     * @param seq         per-pair sequence, increments once per book change, shared across tiers
     * @param freshLps    number of providers contributing to the composite
     * @param originNanos send-time stamp of the oldest tick in the batch that caused this price, passed
     *                    through untouched so the sender can measure end-to-end latency; 0 if the change
     *                    came from a provider going stale rather than from a tick
     */
    void onPrice(Pair pair, int tier, long seq, long bid, long ask, int freshLps, long originNanos);

    /** Once per snapshot interval, for audit. Allocates (once a second, off the tick path). */
    default void onSnapshot(BookSnapshot snapshot) {}

    record BookSnapshot(Pair pair, long epochMillis, long bestBid, long bestAsk, int freshLps,
                        long[] lpBid, long[] lpAsk, boolean[] lpFresh) {}
}
