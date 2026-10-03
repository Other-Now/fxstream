package com.fxstream.core;

/**
 * The 48-byte binary body of a streamed client price, big-endian, fixed layout:
 * <pre>
 *  0 int  pairId     4 int  tier
 *  8 long seq       16 long bid      24 long ask       (prices in {@link Px} units)
 * 32 int  freshLps  36 int  reserved
 * 40 long originNanos  (opaque: echoed from the LP tick so the sender can time the round trip)
 * </pre>
 * Fixed binary instead of JSON because the publisher encodes into one reused buffer per pair thread.
 */
public final class PriceCodec {
    public static final int SIZE = 48;

    private PriceCodec() {}

    public static void encode(byte[] b, int pairId, int tier, long seq, long bid, long ask, int freshLps, long origin) {
        putInt(b, 0, pairId);
        putInt(b, 4, tier);
        putLong(b, 8, seq);
        putLong(b, 16, bid);
        putLong(b, 24, ask);
        putInt(b, 32, freshLps);
        putInt(b, 36, 0);
        putLong(b, 40, origin);
    }

    public static int pairId(byte[] b) { return getInt(b, 0); }
    public static int tier(byte[] b) { return getInt(b, 4); }
    public static long seq(byte[] b) { return getLong(b, 8); }
    public static long bid(byte[] b) { return getLong(b, 16); }
    public static long ask(byte[] b) { return getLong(b, 24); }
    public static int freshLps(byte[] b) { return getInt(b, 32); }
    public static long origin(byte[] b) { return getLong(b, 40); }

    private static void putInt(byte[] b, int o, int v) {
        b[o] = (byte) (v >>> 24); b[o + 1] = (byte) (v >>> 16); b[o + 2] = (byte) (v >>> 8); b[o + 3] = (byte) v;
    }

    private static void putLong(byte[] b, int o, long v) {
        putInt(b, o, (int) (v >>> 32));
        putInt(b, o + 4, (int) v);
    }

    private static int getInt(byte[] b, int o) {
        return (b[o] & 0xff) << 24 | (b[o + 1] & 0xff) << 16 | (b[o + 2] & 0xff) << 8 | (b[o + 3] & 0xff);
    }

    private static long getLong(byte[] b, int o) {
        return (long) getInt(b, o) << 32 | (getInt(b, o + 4) & 0xffffffffL);
    }
}
