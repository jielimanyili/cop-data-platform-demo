package com.jieli.cop.streams;

/** Runtime configuration for the stream topology. */
public record TopologyConfig(
        double speedThresholdKph,
        double fuelThresholdPct,
        long windowSizeSeconds) {
}
