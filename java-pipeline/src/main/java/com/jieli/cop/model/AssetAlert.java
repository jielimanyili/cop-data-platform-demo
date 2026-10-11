package com.jieli.cop.model;

/** An anomaly detected against a configured threshold. */
public record AssetAlert(
        String assetId,
        String alertType,
        String severity,
        long eventTime,
        double value,
        double threshold,
        String message) {

    public static AssetAlert speed(String assetId, EnrichedAssetEvent e, double threshold) {
        return new AssetAlert(
                assetId,
                "SPEED_EXCEEDED",
                "WARNING",
                e.eventTime(),
                e.speedKph(),
                threshold,
                String.format("Asset %s speed %.1f kph exceeds threshold %.1f kph",
                        assetId, e.speedKph(), threshold));
    }

    public static AssetAlert fuel(String assetId, EnrichedAssetEvent e, double threshold) {
        return new AssetAlert(
                assetId,
                "LOW_FUEL",
                "CRITICAL",
                e.eventTime(),
                e.fuelPct(),
                threshold,
                String.format("Asset %s fuel %.1f%% below threshold %.1f%%",
                        assetId, e.fuelPct(), threshold));
    }
}
