package com.fxstream.core;

import java.util.List;

/**
 * Supported currency pairs. {@code pipUnits} is one pip in {@link Px} units: 0.0001 for most pairs,
 * 0.01 for JPY pairs - which is why markups are configured in pips, never in raw price units.
 */
public record Pair(int id, String symbol, long pipUnits) {

    public static final List<Pair> ALL = List.of(
            new Pair(0, "EURUSD", 10),
            new Pair(1, "GBPUSD", 10),
            new Pair(2, "USDJPY", 1000),
            new Pair(3, "AUDUSD", 10));

    public static Pair of(int id) {
        return ALL.get(id);
    }

    /** Linear scan over 4 entries: cheaper than hashing and allocation-free. */
    public static Pair bySymbol(String symbol) {
        for (Pair p : ALL) if (p.symbol.equals(symbol)) return p;
        return null;
    }

    /** Pips expressed in tenths (so 0.3 pip = 3) converted to price units. */
    public long tenthPipsToUnits(long tenthPips) {
        return tenthPips * pipUnits / 10;
    }
}
