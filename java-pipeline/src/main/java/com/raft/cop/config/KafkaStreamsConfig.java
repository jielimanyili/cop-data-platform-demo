package com.raft.cop.config;

import com.raft.cop.streams.EventTimeExtractor;
import com.raft.cop.streams.TelemetryTopology;
import com.raft.cop.streams.TopologyConfig;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Properties;

/**
 * Builds and starts the {@link KafkaStreams} app.
 *
 * <p>Notable choices (worth explaining in an interview):</p>
 * <ul>
 *   <li>{@code COMMIT_INTERVAL_MS_CONFIG} lowered so sink topics update promptly.</li>
 *   <li>{@code CACHE_MAX_BYTES_BUFFERING_CONFIG = 0} disables record caching so the
 *       aggregation store is immediately readable via interactive queries
 *       ({@code GET /api/picture}). In a high-volume production system you would
 *       instead keep caching on and add {@code suppress()} to emit only final
 *       window results.</li>
 *   <li>{@code STATE_DIR_CONFIG} is explicit; in Kubernetes this would be a
 *       persistent volume (the chart sets this up as a StatefulSet concern).</li>
 * </ul>
 */
@Configuration
public class KafkaStreamsConfig {

    @Bean
    public TopologyConfig topologyConfig(
            @Value("${app.thresholds.speed-kph:100}") double speedKph,
            @Value("${app.thresholds.fuel-pct:20}") double fuelPct,
            @Value("${app.window-size-seconds:10}") long windowSeconds) {
        return new TopologyConfig(speedKph, fuelPct, windowSeconds);
    }

    @Bean
    public StreamsBuilder streamsBuilder(TopologyConfig cfg) {
        return TelemetryTopology.build(cfg);
    }

    @Bean(destroyMethod = "close")
    public KafkaStreams kafkaStreams(
            StreamsBuilder builder,
            @Value("${spring.kafka.bootstrap-servers:localhost:9092}") String bootstrapServers,
            @Value("${app.streams.application-id:cop-data-platform}") String applicationId) {

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        // Event-time windowing driven by the payload, not the consume timestamp.
        props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, EventTimeExtractor.class.getName());
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000);
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        props.put(StreamsConfig.STATE_DIR_CONFIG, "./data/kafka-streams");

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();
        return streams;
    }
}
