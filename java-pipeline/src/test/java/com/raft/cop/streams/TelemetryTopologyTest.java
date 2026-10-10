package com.raft.cop.streams;

import com.raft.cop.model.AssetAggregate;
import com.raft.cop.model.AssetAlert;
import com.raft.cop.model.AssetReference;
import com.raft.cop.model.AssetTelemetry;
import com.raft.cop.model.EnrichedAssetEvent;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for the stream topology using {@link TopologyTestDriver} (no broker
 * required). Caching is disabled so each processed record is forwarded immediately,
 * making assertions on the output topics deterministic.
 */
class TelemetryTopologyTest {

    private TopologyTestDriver driver;
    private TestInputTopic<String, AssetTelemetry> rawInput;
    private TestInputTopic<String, AssetReference> refInput;
    private TestOutputTopic<String, EnrichedAssetEvent> enriched;
    private TestOutputTopic<String, AssetAggregate> aggregates;
    private TestOutputTopic<String, AssetAlert> alerts;

    @TempDir
    Path tempDir;

    @AfterEach
    void closeDriver() {
        if (driver != null) {
            driver.close();
        }
    }

    private void setUpDriver(double speedThreshold, double fuelThreshold, long windowSeconds) {
        StreamsBuilder builder = TelemetryTopology.build(
                new TopologyConfig(speedThreshold, fuelThreshold, windowSeconds));

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "cop-test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");
        props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, EventTimeExtractor.class.getName());
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        props.put(StreamsConfig.STATE_DIR_CONFIG, tempDir.resolve("kafka-streams").toString());

        driver = new TopologyTestDriver(builder.build(), props);

        Serde<String> keySerde = JsonSerdes.string();
        rawInput = driver.createInputTopic(
                Topics.RAW_TELEMETRY, keySerde.serializer(), JsonSerdes.telemetry().serializer());
        refInput = driver.createInputTopic(
                Topics.ASSET_REFERENCE, keySerde.serializer(), JsonSerdes.reference().serializer());
        enriched = driver.createOutputTopic(
                Topics.ENRICHED_EVENTS, keySerde.deserializer(), JsonSerdes.enriched().deserializer());
        aggregates = driver.createOutputTopic(
                Topics.ASSET_AGGREGATES, keySerde.deserializer(), JsonSerdes.aggregate().deserializer());
        alerts = driver.createOutputTopic(
                Topics.ALERTS, keySerde.deserializer(), JsonSerdes.alert().deserializer());
    }

    @Test
    void enrichesTelemetryWithReferenceData() {
        setUpDriver(100, 20, 10);
        refInput.pipeInput("V-1024",
                new AssetReference("V-1024", "GROUND_VEHICLE", "3rd Cavalry", "Ghost 7", "UNCLASSIFIED"));

        rawInput.pipeInput("V-1024",
                new AssetTelemetry("e1", "V-1024", 1_000L, 39.7, -84.1, 60, 90, 50, "ACTIVE"));

        List<EnrichedAssetEvent> out = enriched.readValuesToList();
        assertThat(out).hasSize(1);
        assertThat(out.get(0).unit()).isEqualTo("3rd Cavalry");
        assertThat(out.get(0).callsign()).isEqualTo("Ghost 7");
        assertThat(out.get(0).speedKph()).isEqualTo(60);
        assertThat(out.get(0).assetType()).isEqualTo("GROUND_VEHICLE");
    }

    @Test
    void producesWindowedAggregatePerAsset() {
        setUpDriver(100, 20, 10);
        refInput.pipeInput("V-1024",
                new AssetReference("V-1024", "GROUND_VEHICLE", "3rd Cavalry", "Ghost 7", "UNCLASSIFIED"));

        long t0 = 1_000_000L; // window-aligned epoch millis
        rawInput.pipeInput("V-1024",
                new AssetTelemetry("e1", "V-1024", t0, 39.7, -84.1, 60, 90, 50, "ACTIVE"));
        rawInput.pipeInput("V-1024",
                new AssetTelemetry("e2", "V-1024", t0 + 2_000, 39.7, -84.1, 80, 90, 45, "ACTIVE"));

        // Caching is off, so each update is forwarded; assert on the final accumulator.
        List<AssetAggregate> out = aggregates.readValuesToList();
        assertThat(out).isNotEmpty();
        AssetAggregate agg = out.get(out.size() - 1);
        assertThat(agg.assetId()).isEqualTo("V-1024");
        assertThat(agg.eventCount()).isEqualTo(2);
        assertThat(agg.avgSpeed()).isEqualTo(70.0);
        assertThat(agg.windowStart()).isEqualTo(1_000_000L);
        assertThat(agg.windowEnd()).isEqualTo(1_010_000L);
    }

    @Test
    void detectsSpeedAndFuelAnomalies() {
        setUpDriver(100, 20, 10);
        refInput.pipeInput("V-1024",
                new AssetReference("V-1024", "GROUND_VEHICLE", "3rd Cavalry", "Ghost 7", "UNCLASSIFIED"));

        long t0 = 2_000_000L;
        rawInput.pipeInput("V-1024",
                new AssetTelemetry("e1", "V-1024", t0, 39.7, -84.1, 140, 90, 60, "ACTIVE")); // speed > 100
        rawInput.pipeInput("V-1024",
                new AssetTelemetry("e2", "V-1024", t0 + 1_000, 39.7, -84.1, 40, 90, 8, "ACTIVE")); // fuel < 20

        List<AssetAlert> out = alerts.readValuesToList();
        assertThat(out).hasSize(2);
        assertThat(out).extracting(AssetAlert::alertType)
                .containsExactlyInAnyOrder("SPEED_EXCEEDED", "LOW_FUEL");
    }

    @Test
    void dropsMalformedTelemetry() {
        setUpDriver(100, 20, 10);
        rawInput.pipeInput("bad",
                new AssetTelemetry("e1", null, 0L, 0, 0, 0, 0, 0, "ACTIVE"));

        assertThat(enriched.readValuesToList()).isEmpty();
    }
}
