package com.raft.cop.bench;

/**
 * Minimal wire-format mirror of the pipeline's {@code AssetTelemetry} record.
 *
 * <p>The field names MUST stay in sync with {@code com.raft.cop.model.AssetTelemetry}
 * (the pipeline deserializes with a Jackson {@code JsonSerde}, so it expects exactly
 * these camelCase keys). {@link BenchTelemetryTest} asserts this contract.</p>
 */
public record BenchTelemetry(
        String eventId,
        String assetId,
        long eventTime,
        double latitude,
        double longitude,
        double speedKph,
        double headingDeg,
        double fuelPct,
        String status) {
}
