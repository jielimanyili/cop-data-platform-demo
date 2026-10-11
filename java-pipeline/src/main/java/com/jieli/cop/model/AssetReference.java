package com.jieli.cop.model;

/**
 * Slowly-changing reference data about an asset, loaded as a KTable from a
 * log-compacted topic. Used to enrich raw telemetry (the "transform" step).
 */
public record AssetReference(
        String assetId,
        String assetType,
        String unit,
        String callsign,
        String classification) {
}
