package com.fxstream.core;

/**
 * Fixed-point prices: every price is a {@code long} count of 1e-5 units, so EUR/USD 1.08345 is 108345
 * and USD/JPY 151.234 is 15123400. No doubles anywhere on the price path - 0.1 + 0.2 bugs don't belong
 * in a pricing engine, and integer compares are what the book does millions of times a second.
 */
public final class Px {
    public static final int DECIMALS = 5;
    public static final long SCALE = 100_000L;

    private Px() {}

    /** Parses "1.08345" / "151.2" / "1" into units. No allocation; rejects more than 5 decimals. */
    public static long parse(CharSequence s) {
        long whole = 0, frac = 0;
        int fracDigits = 0, i = 0, n = s.length();
        if (n == 0) throw new NumberFormatException("empty price");
        for (; i < n; i++) {
            char c = s.charAt(i);
            if (c == '.') { i++; break; }
            if (c < '0' || c > '9') throw new NumberFormatException("bad price: " + s);
            whole = whole * 10 + (c - '0');
        }
        for (; i < n; i++) {
            char c = s.charAt(i);
            if (c < '0' || c > '9') throw new NumberFormatException("bad price: " + s);
            if (++fracDigits > DECIMALS) throw new NumberFormatException("more than 5 decimals: " + s);
            frac = frac * 10 + (c - '0');
        }
        for (; fracDigits < DECIMALS; fracDigits++) frac *= 10;
        return whole * SCALE + frac;
    }

    /** For logs, JSON and FIX output only - allocates. */
    public static String format(long units) {
        long whole = units / SCALE, frac = Math.abs(units % SCALE);
        String f = Long.toString(frac);
        return whole + "." + "00000".substring(f.length()) + f;
    }
}
