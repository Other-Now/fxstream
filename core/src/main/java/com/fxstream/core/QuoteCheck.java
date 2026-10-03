package com.fxstream.core;

/**
 * The decision made when a client trades against a quote. Pure function: the time, the quote and the
 * current price go in, a verdict comes out, which makes the 100%-of-expired-quotes-rejected property
 * something a unit test can pin down.
 */
public final class QuoteCheck {

    public enum Side { BUY, SELL }

    public enum Verdict { FILLED, REJECTED_EXPIRED, REJECTED_PRICE_MOVED, REJECTED_NO_PRICE, REJECTED_UNKNOWN_QUOTE }

    private QuoteCheck() {}

    /**
     * @param now             server time the trade request is evaluated (any unit, same as expiresAt)
     * @param expiresAt       quote expiry; a request at exactly this instant is already too late
     * @param quotedPrice     the price the client is trading at (the quote's)
     * @param currentPrice    what the same client/size would be quoted now; 0 if there is no price
     * @param toleranceUnits  how far the market may have moved since the quote, either direction
     */
    public static Verdict check(long now, long expiresAt, long quotedPrice, long currentPrice,
                                long toleranceUnits) {
        if (now >= expiresAt) return Verdict.REJECTED_EXPIRED;
        if (currentPrice <= 0) return Verdict.REJECTED_NO_PRICE;
        if (Math.abs(currentPrice - quotedPrice) > toleranceUnits) return Verdict.REJECTED_PRICE_MOVED;
        return Verdict.FILLED;
    }
}
