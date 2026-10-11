package com.jieli.cop.bench;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Load generator: produces valid telemetry JSON into {@code cop.raw-telemetry} as fast
 * as possible (or at a fixed rate), measuring producer-side throughput with a
 * {@link RateMeter}. Tuned for throughput: batching, linger, and lz4 compression.
 */
public final class LoadGenerator implements AutoCloseable {

    private static final String[] ASSET_IDS =
            {"V-1024", "V-2031", "UAS-07", "TANK-04", "SENSOR-88"};

    private final KafkaProducer<String, String> producer;
    private final ObjectMapper mapper = new ObjectMapper();
    private final Random random = new Random();
    private final RateMeter meter = new RateMeter();
    private final String topic;

    public LoadGenerator(String bootstrapServers, String topic, boolean acksAll) {
        this.topic = topic;
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // acks=all trades latency for durability; acks=1 is the realistic throughput default.
        props.put(ProducerConfig.ACKS_CONFIG, acksAll ? "all" : "1");
        props.put(ProducerConfig.LINGER_MS_CONFIG, 5);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 32 * 1024);
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");
        this.producer = new KafkaProducer<>(props);
    }

    /**
     * Produce {@code records} events. {@code ratePerSecond <= 0} means "max rate"
     * (no throttle); otherwise the loop paces itself to that target.
     */
    public long produce(long records, double ratePerSecond) throws Exception {
        long start = System.nanoTime();
        for (long i = 0; i < records; i++) {
            String key = ASSET_IDS[(int) (i % ASSET_IDS.length)];
            producer.send(new ProducerRecord<>(topic, key, buildPayload(i)));
            meter.mark();

            if (ratePerSecond > 0) {
                long targetNanos = (long) ((i + 1) * 1_000_000_000.0 / ratePerSecond);
                long sleep = targetNanos - (System.nanoTime() - start);
                if (sleep > 0) {
                    TimeUnit.NANOSECONDS.sleep(sleep);
                }
            }
        }
        producer.flush();
        return records;
    }

    public RateMeter meter() {
        return meter;
    }

    private String buildPayload(long i) throws Exception {
        BenchTelemetry t = new BenchTelemetry(
                "bench-" + i,
                ASSET_IDS[(int) (i % ASSET_IDS.length)],
                System.currentTimeMillis(),
                39.7 + random.nextDouble(),
                -84.1 + random.nextDouble(),
                30 + random.nextDouble() * 100, // occasionally > 100 kph (alerts)
                random.nextDouble() * 360,
                5 + random.nextDouble() * 90,   // occasionally < 20% fuel (alerts)
                "ACTIVE");
        return mapper.writeValueAsString(t);
    }

    @Override
    public void close() {
        producer.close();
    }
}
