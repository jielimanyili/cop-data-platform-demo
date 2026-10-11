package com.jieli.cop.service;

import com.jieli.cop.model.AssetAggregate;

import java.util.List;
import java.util.Optional;

/**
 * Reads the "common operational picture" (latest per-asset aggregate). Defined as an
 * interface so the Kafka Streams implementation can be swapped for a fake in tests,
 * and so the REST controller and the gRPC service share one contract.
 */
public interface PictureQueryService {

    /** Latest aggregate for every known asset. */
    List<AssetAggregate> picture();

    /** Latest aggregate for one asset, if present. */
    Optional<AssetAggregate> asset(String assetId);
}
