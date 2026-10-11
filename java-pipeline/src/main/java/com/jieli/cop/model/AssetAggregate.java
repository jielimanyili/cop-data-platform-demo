package com.jieli.cop.model;

/**
 * Windowed per-asset summary produced by the aggregation step. Immutable:
 * {@link #accumulate} returns a new value, so the aggregation is side-effect free.
 */
public record AssetAggregate(
        String assetId,
        long windowStart,
        long windowEnd,
        long eventCount,
        double speedSum,
        double speedMax,
        double speedMin,
        double fuelMin,
        String unit,
        String callsign) {

    public static AssetAggregate empty() {
        return new AssetAggregate(
                null, 0L, 0L, 0L, 0.0, 0.0, Double.MAX_VALUE, Double.MAX_VALUE, null, null);
    }

    /** Kafka Streams aggregator: fold one enriched event into the running aggregate. */
    public AssetAggregate accumulate(String key, EnrichedAssetEvent e) {
        return new AssetAggregate(
                key,
                windowStart,
                windowEnd,
                eventCount + 1,
                speedSum + e.speedKph(),
                Math.max(speedMax, e.speedKph()),
                Math.min(speedMin, e.speedKph()),
                Math.min(fuelMin, e.fuelPct()),
                e.unit(),
                e.callsign());
    }

    /** Stamp the window boundaries from the {@code Windowed} key after aggregation. */
    public AssetAggregate withWindow(long start, long end) {
        return new AssetAggregate(
                assetId, start, end, eventCount, speedSum, speedMax, speedMin, fuelMin, unit, callsign);
    }

    public double avgSpeed() {
        return eventCount == 0 ? 0.0 : speedSum / eventCount;
    }
}
