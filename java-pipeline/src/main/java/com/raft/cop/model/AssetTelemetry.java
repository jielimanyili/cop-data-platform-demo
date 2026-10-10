package com.raft.cop.model;

/**
 * Raw telemetry emitted by a tracked asset (vehicle, sensor, aircraft, ...).
 * This is the "extract" input of the ETL pipeline.
 *
 * @param eventId   unique id for this event (used for idempotency/dedupe downstream)
 * @param assetId   business key used as the Kafka partition key (per-entity ordering)
 * @param eventTime epoch millis — event time, not processing time (drives windowing)
 */
public record AssetTelemetry(
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
