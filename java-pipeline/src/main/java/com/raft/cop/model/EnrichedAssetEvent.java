package com.raft.cop.model;

/**
 * Telemetry joined with reference metadata. This is the enriched, query-ready
 * record that downstream consumers (and a Pinot/Elasticsearch sink in
 * production) would read.
 */
public record EnrichedAssetEvent(
        String eventId,
        String assetId,
        long eventTime,
        long processedAt,
        double latitude,
        double longitude,
        double speedKph,
        double headingDeg,
        double fuelPct,
        String status,
        String assetType,
        String unit,
        String callsign,
        String classification) {

    public static EnrichedAssetEvent of(AssetTelemetry t, AssetReference r) {
        return new EnrichedAssetEvent(
                t.eventId(),
                t.assetId(),
                t.eventTime(),
                System.currentTimeMillis(),
                t.latitude(),
                t.longitude(),
                t.speedKph(),
                t.headingDeg(),
                t.fuelPct(),
                t.status(),
                r == null ? null : r.assetType(),
                r == null ? null : r.unit(),
                r == null ? null : r.callsign(),
                r == null ? null : r.classification());
    }
}
