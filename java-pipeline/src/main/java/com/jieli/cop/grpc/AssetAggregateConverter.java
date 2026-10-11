package com.jieli.cop.grpc;

import com.jieli.cop.model.AssetAggregate;

/** Maps the domain {@link AssetAggregate} record to the protobuf {@link AssetSnapshot}. */
public final class AssetAggregateConverter {

    private AssetAggregateConverter() {
    }

    public static AssetSnapshot toProto(AssetAggregate a) {
        return AssetSnapshot.newBuilder()
                .setAssetId(a.assetId() == null ? "" : a.assetId())
                .setWindowStart(a.windowStart())
                .setWindowEnd(a.windowEnd())
                .setEventCount(a.eventCount())
                .setSpeedSum(a.speedSum())
                .setSpeedMax(a.speedMax())
                .setSpeedMin(a.speedMin())
                .setFuelMin(a.fuelMin())
                .setAvgSpeed(a.avgSpeed())
                .setUnit(a.unit() == null ? "" : a.unit())
                .setCallsign(a.callsign() == null ? "" : a.callsign())
                .build();
    }
}
