package com.raft.cop.streams;

import com.raft.cop.model.AssetAggregate;
import com.raft.cop.model.AssetAlert;
import com.raft.cop.model.AssetReference;
import com.raft.cop.model.AssetTelemetry;
import com.raft.cop.model.EnrichedAssetEvent;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.Joined;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.state.WindowStore;

import java.time.Duration;

import static com.raft.cop.streams.Topics.ALERTS;
import static com.raft.cop.streams.Topics.ASSET_AGGREGATES;
import static com.raft.cop.streams.Topics.ASSET_REFERENCE;
import static com.raft.cop.streams.Topics.ENRICHED_EVENTS;
import static com.raft.cop.streams.Topics.RAW_TELEMETRY;

/**
 * The ETL pipeline for the Common Operational Picture.
 *
 * <pre>
 *   raw-telemetry ──▶ filter(valid) ──▶ re-key(assetId) ──▶ join(asset-reference KTable)
 *                                                                │
 *                       ┌────────────────────────────────────────┼───────────────────────────┐
 *                       ▼                                        ▼                           ▼
 *                 enriched-events                     groupByKey + tumbling window     threshold filters
 *                      (sink)                              aggregate                    (speed / fuel)
 *                                                             │                              │
 *                                                             ▼                              ▼
 *                                                     asset-aggregates                    alerts
 *                                                            (sink)                        (sink)
 * </pre>
 */
public final class TelemetryTopology {

    public static final String ASSET_REFERENCE_STORE = "asset-reference-store";
    public static final String ASSET_AGGREGATE_STORE = "asset-aggregate-store";

    private TelemetryTopology() {
    }

    public static StreamsBuilder build(TopologyConfig cfg) {
        StreamsBuilder builder = new StreamsBuilder();

        // Reference data as a KTable: latest value per asset id (log-compacted source).
        KTable<String, AssetReference> references = builder.table(
                ASSET_REFERENCE,
                Consumed.with(JsonSerdes.string(), JsonSerdes.reference()),
                Materialized.as(ASSET_REFERENCE_STORE));

        // Extract: consume raw telemetry.
        KStream<String, AssetTelemetry> raw = builder.stream(
                RAW_TELEMETRY,
                Consumed.with(JsonSerdes.string(), JsonSerdes.telemetry()));

        // Transform: validate, re-key by asset id (partition key), enrich via KTable join.
        KStream<String, EnrichedAssetEvent> enriched = raw
                .filter((id, t) -> t != null && t.assetId() != null && t.eventTime() > 0)
                .selectKey((id, t) -> t.assetId())
                .join(references, EnrichedAssetEvent::of,
                        Joined.with(Serdes.String(), JsonSerdes.telemetry(), JsonSerdes.reference()));

        // Load: sink enriched, query-ready events for downstream consumers / Pinot.
        enriched.to(ENRICHED_EVENTS, Produced.with(JsonSerdes.string(), JsonSerdes.enriched()));

        // Transform: per-asset tumbling-window aggregation.
        TimeWindows windows = TimeWindows.ofSizeWithNoGrace(Duration.ofSeconds(cfg.windowSizeSeconds()));
        enriched
                .groupByKey(Grouped.with(Serdes.String(), JsonSerdes.enriched()))
                .windowedBy(windows)
                .aggregate(AssetAggregate::empty,
                        (key, evt, agg) -> agg.accumulate(key, evt),
                        Materialized.<String, AssetAggregate, WindowStore<Bytes, byte[]>>as(ASSET_AGGREGATE_STORE)
                                .withKeySerde(Serdes.String())
                                .withValueSerde(JsonSerdes.aggregate()))
                .toStream()
                .map((windowedKey, agg) -> KeyValue.pair(
                        windowedKey.key(),
                        agg.withWindow(windowedKey.window().start(), windowedKey.window().end())))
                .to(ASSET_AGGREGATES, Produced.with(JsonSerdes.string(), JsonSerdes.aggregate()));

        // Detect: threshold-based anomaly detection -> alerts.
        enriched.filter((id, e) -> e.speedKph() > cfg.speedThresholdKph())
                .mapValues(e -> AssetAlert.speed(e.assetId(), e, cfg.speedThresholdKph()))
                .to(ALERTS, Produced.with(JsonSerdes.string(), JsonSerdes.alert()));

        enriched.filter((id, e) -> e.fuelPct() < cfg.fuelThresholdPct())
                .mapValues(e -> AssetAlert.fuel(e.assetId(), e, cfg.fuelThresholdPct()))
                .to(ALERTS, Produced.with(JsonSerdes.string(), JsonSerdes.alert()));

        return builder;
    }
}
