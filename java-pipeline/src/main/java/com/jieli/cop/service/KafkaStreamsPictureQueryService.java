package com.jieli.cop.service;

import com.jieli.cop.model.AssetAggregate;
import com.jieli.cop.streams.TelemetryTopology;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;

/**
 * Kafka Streams implementation of {@link PictureQueryService}: serves the operational
 * picture straight from the windowed state store via interactive queries (no external
 * database, no network hop).
 */
@Service
public class KafkaStreamsPictureQueryService implements PictureQueryService {

    private final KafkaStreams streams;

    public KafkaStreamsPictureQueryService(KafkaStreams streams) {
        this.streams = streams;
    }

    @Override
    public List<AssetAggregate> picture() {
        if (streams.state() != KafkaStreams.State.RUNNING) {
            return List.of();
        }
        try {
            ReadOnlyWindowStore<String, AssetAggregate> store = windowStore();
            Map<String, AssetAggregate> latest = new TreeMap<>();
            try (KeyValueIterator<Windowed<String>, AssetAggregate> it = store.all()) {
                while (it.hasNext()) {
                    var entry = it.next();
                    String assetId = entry.key.key();
                    AssetAggregate agg = entry.value;
                    AssetAggregate existing = latest.get(assetId);
                    if (existing == null || agg.windowStart() > existing.windowStart()) {
                        latest.put(assetId, agg);
                    }
                }
            }
            return new ArrayList<>(latest.values());
        } catch (InvalidStateStoreException e) {
            // Store not yet available (still rebalancing). Return empty rather than fail.
            return List.of();
        }
    }

    @Override
    public Optional<AssetAggregate> asset(String assetId) {
        if (streams.state() != KafkaStreams.State.RUNNING) {
            return Optional.empty();
        }
        try {
            ReadOnlyWindowStore<String, AssetAggregate> store = windowStore();
            AssetAggregate latest = null;
            try (WindowStoreIterator<AssetAggregate> it = store.fetch(assetId, Instant.EPOCH, Instant.MAX)) {
                while (it.hasNext()) {
                    AssetAggregate agg = it.next().value;
                    if (latest == null || agg.windowStart() > latest.windowStart()) {
                        latest = agg;
                    }
                }
            }
            return Optional.ofNullable(latest);
        } catch (InvalidStateStoreException e) {
            return Optional.empty();
        }
    }

    private ReadOnlyWindowStore<String, AssetAggregate> windowStore() {
        return streams.store(StoreQueryParameters.fromNameAndType(
                TelemetryTopology.ASSET_AGGREGATE_STORE,
                QueryableStoreTypes.windowStore()));
    }
}
