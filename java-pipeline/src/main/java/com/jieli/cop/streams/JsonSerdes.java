package com.jieli.cop.streams;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jieli.cop.model.AssetAggregate;
import com.jieli.cop.model.AssetAlert;
import com.jieli.cop.model.AssetReference;
import com.jieli.cop.model.AssetTelemetry;
import com.jieli.cop.model.EnrichedAssetEvent;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

/**
 * Typed JSON serdes for Kafka Streams. A single shared {@link ObjectMapper} keeps
 * serialization consistent across the topology and the tests.
 */
public final class JsonSerdes {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private JsonSerdes() {
    }

    public static Serde<String> string() {
        return Serdes.String();
    }

    public static Serde<AssetTelemetry> telemetry() {
        return new JsonSerde<>(AssetTelemetry.class, MAPPER);
    }

    public static Serde<AssetReference> reference() {
        return new JsonSerde<>(AssetReference.class, MAPPER);
    }

    public static Serde<EnrichedAssetEvent> enriched() {
        return new JsonSerde<>(EnrichedAssetEvent.class, MAPPER);
    }

    public static Serde<AssetAggregate> aggregate() {
        return new JsonSerde<>(AssetAggregate.class, MAPPER);
    }

    public static Serde<AssetAlert> alert() {
        return new JsonSerde<>(AssetAlert.class, MAPPER);
    }
}
