package com.fxstream.core;

/**
 * Composite top-of-book for one pair across N liquidity providers. Owned by exactly one thread (the
 * pair's {@link PairEngine}), so there is no synchronisation here at all - that is the point of the
 * single-writer design: the concurrency is pushed into the ring, and the book stays plain arrays.
 *
 * <p>The best bid is the highest fresh LP bid and the best offer the lowest fresh LP offer, each with the
 * LP that provides it. An LP is <em>stale</em> once {@code staleNanos} pass without a tick from it; a
 * stale LP's last quote is excluded from the composite, because a price you can no longer trade on is
 * worse than no price. N is small (a handful of LPs), so a linear scan beats any heap or tree.
 */
public final class Book {
    private final int maxLps;
    private final long staleNanos;
    private final long[] bid, ask, bidQty, askQty, lastRecv;
    private final boolean[] seen;

    private long bestBid, bestAsk, bestBidQty, bestAskQty;
    private int bestBidLp = -1, bestAskLp = -1, freshLps;

    public Book(int maxLps, long staleNanos) {
        this.maxLps = maxLps;
        this.staleNanos = staleNanos;
        bid = new long[maxLps];
        ask = new long[maxLps];
        bidQty = new long[maxLps];
        askQty = new long[maxLps];
        lastRecv = new long[maxLps];
        seen = new boolean[maxLps];
    }

    /** Record an LP's latest two-way quote. A side of 0 means the LP has withdrawn that side. */
    public void apply(int lp, long b, long a, long bq, long aq, long nowNanos) {
        if (lp < 0 || lp >= maxLps) return;
        bid[lp] = b;
        ask[lp] = a;
        bidQty[lp] = bq;
        askQty[lp] = aq;
        lastRecv[lp] = nowNanos;
        seen[lp] = true;
    }

    /** Rebuilds the composite from fresh LPs. Returns true if anything a client would see changed. */
    public boolean recompute(long nowNanos) {
        long bb = 0, ba = Long.MAX_VALUE, bbq = 0, baq = 0;
        int bbLp = -1, baLp = -1, fresh = 0;
        for (int i = 0; i < maxLps; i++) {
            if (!seen[i] || nowNanos - lastRecv[i] > staleNanos) continue;
            fresh++;
            if (bid[i] > 0 && bid[i] > bb) { bb = bid[i]; bbq = bidQty[i]; bbLp = i; }
            if (ask[i] > 0 && ask[i] < ba) { ba = ask[i]; baq = askQty[i]; baLp = i; }
        }
        if (ba == Long.MAX_VALUE) ba = 0;
        boolean changed = bb != bestBid || ba != bestAsk || fresh != freshLps
                || bbLp != bestBidLp || baLp != bestAskLp;
        bestBid = bb; bestAsk = ba; bestBidQty = bbq; bestAskQty = baq;
        bestBidLp = bbLp; bestAskLp = baLp; freshLps = fresh;
        return changed;
    }

    public boolean isTwoWay() { return bestBid > 0 && bestAsk > 0; }
    /** Best bid at or above best offer, across different LPs. Tradeable on paper, never in practice. */
    public boolean isCrossed() { return isTwoWay() && bestBid >= bestAsk; }

    public long bestBid() { return bestBid; }
    public long bestAsk() { return bestAsk; }
    public long bestBidQty() { return bestBidQty; }
    public long bestAskQty() { return bestAskQty; }
    public int bestBidLp() { return bestBidLp; }
    public int bestAskLp() { return bestAskLp; }
    public int freshLps() { return freshLps; }
    public int maxLps() { return maxLps; }

    public long lpBid(int lp) { return bid[lp]; }
    public long lpAsk(int lp) { return ask[lp]; }
    public boolean lpFresh(int lp, long nowNanos) { return seen[lp] && nowNanos - lastRecv[lp] <= staleNanos; }
    public boolean lpSeen(int lp) { return seen[lp]; }
}
