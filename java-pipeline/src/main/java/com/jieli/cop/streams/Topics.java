package com.jieli.cop.streams;

/** Kafka topic names for the COP data platform. */
public final class Topics {

    public static final String RAW_TELEMETRY = "cop.raw-telemetry";
    public static final String ASSET_REFERENCE = "cop.asset-reference";
    public static final String ENRICHED_EVENTS = "cop.enriched-events";
    public static final String ASSET_AGGREGATES = "cop.asset-aggregates";
    public static final String ALERTS = "cop.alerts";

    private Topics() {
    }
}
