package com.fxstream.app;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.Map;

/**
 * Everything tunable lives under {@code fx.*} in application.yml.
 *
 * @param maxLps             LP sessions LP1..LPn accepted over FIX
 * @param staleMs            an LP with no tick for this long is dropped from the composite
 * @param toleranceTenthPips how far the price may move between quote and trade (tenths of a pip)
 * @param idempotent         false only for the chaos control run: trades are no longer de-duplicated
 * @param clientTiers        clientId -> pricing tier (0 = tightest)
 */
@ConfigurationProperties("fx")
public record FxProperties(
        int fixPort,
        int maxLps,
        long staleMs,
        int ringCapacity,
        long snapshotMs,
        long quoteTtlMs,
        long maxQuoteTtlMs,
        long toleranceTenthPips,
        boolean idempotent,
        Map<String, Integer> clientTiers) {

    public int tierOf(String clientId) {
        Integer t = clientTiers == null ? null : clientTiers.get(clientId);
        return t == null ? 2 : t; // unknown clients get the widest tier
    }
}
