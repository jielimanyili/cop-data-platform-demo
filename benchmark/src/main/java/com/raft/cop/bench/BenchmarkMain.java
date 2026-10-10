package com.raft.cop.bench;

import java.util.HashMap;
import java.util.Map;

/**
 * CLI entrypoint for the throughput benchmark.
 *
 * <pre>
 *   java -cp benchmark/target/benchmark-0.0.1-SNAPSHOT.jar com.raft.cop.bench.BenchmarkMain \
 *        --records 200000 --phase both
 * </pre>
 *
 * Phases: {@code produce} (blast N records), {@code monitor} (watch pipeline), or
 * {@code both} (default: produce at max rate while sampling the pipeline, then summarize).
 */
public final class BenchmarkMain {

    private BenchmarkMain() {
    }

    public static void main(String[] args) throws Exception {
        Args a = Args.parse(args);
        switch (a.phase()) {
            case "produce" -> runProduce(a);
            case "monitor" -> runMonitor(a);
            default -> runBoth(a);
        }
    }

    private static void runProduce(Args a) throws Exception {
        try (LoadGenerator gen = new LoadGenerator(a.bootstrapServers(), a.inputTopic(), a.acksAll())) {
            long t0 = System.nanoTime();
            long sent = gen.produce(a.records(), a.rate());
            double seconds = (System.nanoTime() - t0) / 1e9;
            System.out.printf("Produced %,d records in %.2fs (producer rate %.0f events/sec)%n",
                    sent, seconds, sent / seconds);
        }
    }

    private static void runMonitor(Args a) throws Exception {
        try (PipelineMonitor mon = new PipelineMonitor(a.bootstrapServers(), a.inputTopic(), a.groupId())) {
            long prevOut = mon.topicEndOffset(a.outputTopic());
            long prevT = System.nanoTime();
            long deadline = System.currentTimeMillis() + a.durationSec() * 1000;
            long elapsed = 0;
            while (System.currentTimeMillis() < deadline) {
                Thread.sleep(1000);
                long out = mon.topicEndOffset(a.outputTopic());
                long lag = mon.lag();
                double outRate = (out - prevOut) * 1e9 / (System.nanoTime() - prevT);
                System.out.printf("t=%3ds  output_total=%,d  lag=%,d  pipeline_rate=%.0f/s%n",
                        ++elapsed, out, lag, outRate);
                prevOut = out;
                prevT = System.nanoTime();
            }
        }
    }

    private static void runBoth(Args a) throws Exception {
        try (LoadGenerator gen = new LoadGenerator(a.bootstrapServers(), a.inputTopic(), a.acksAll());
             PipelineMonitor mon = new PipelineMonitor(a.bootstrapServers(), a.inputTopic(), a.groupId())) {

            long outStart = mon.topicEndOffset(a.outputTopic());
            long t0 = System.nanoTime();

            Thread producer = new Thread(() -> {
                try {
                    gen.produce(a.records(), a.rate());
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }, "load-generator");
            producer.start();

            long prevOut = outStart;
            long prevT = t0;
            long drainDeadline = Long.MAX_VALUE;
            while (true) {
                Thread.sleep(1000);
                long out = mon.topicEndOffset(a.outputTopic());
                long lag = mon.lag();
                double outRate = (out - prevOut) * 1e9 / (System.nanoTime() - prevT);
                System.out.printf("output_total=%,d  lag=%,d  pipeline_rate=%.0f/s%n", out, lag, outRate);
                prevOut = out;
                prevT = System.nanoTime();

                if (!producer.isAlive()) {
                    if (drainDeadline == Long.MAX_VALUE) {
                        drainDeadline = System.currentTimeMillis() + a.drainSec() * 1000;
                    }
                    if (lag == 0 || System.currentTimeMillis() > drainDeadline) {
                        break;
                    }
                }
            }
            producer.join();

            long outEnd = mon.topicEndOffset(a.outputTopic());
            double totalSeconds = (System.nanoTime() - t0) / 1e9;

            System.out.printf("%n=== SUMMARY ===%n");
            System.out.printf("records produced:     %,d%n", gen.meter().total());
            System.out.printf("producer rate:        %.0f events/sec%n", gen.meter().ratePerSecond());
            System.out.printf("pipeline output rate: %.0f events/sec (enriched)%n", (outEnd - outStart) / totalSeconds);
            System.out.printf("final input lag:      %,d%n", mon.lag());
            System.out.printf("verdict:              %s%n", mon.lag() == 0
                    ? "pipeline kept up at producer rate"
                    : "pipeline fell behind (lag > 0) — that rate exceeds the platform's capacity");
        }
    }

    record Args(String bootstrapServers, String inputTopic, String outputTopic, String groupId,
                long records, double rate, long durationSec, long drainSec, String phase, boolean acksAll) {

        static Args parse(String[] args) {
            Map<String, String> m = new HashMap<>();
            for (int i = 0; i < args.length; i += 2) {
                if (i + 1 < args.length) {
                    m.put(args[i], args[i + 1]);
                }
            }
            return new Args(
                    m.getOrDefault("--bootstrap-servers", "localhost:9092"),
                    m.getOrDefault("--input-topic", "cop.raw-telemetry"),
                    m.getOrDefault("--output-topic", "cop.enriched-events"),
                    m.getOrDefault("--group", "cop-data-platform"),
                    Long.parseLong(m.getOrDefault("--records", "100000")),
                    Double.parseDouble(m.getOrDefault("--rate", "0")),
                    Long.parseLong(m.getOrDefault("--duration", "30")),
                    Long.parseLong(m.getOrDefault("--drain", "60")),
                    m.getOrDefault("--phase", "both"),
                    Boolean.parseBoolean(m.getOrDefault("--acks-all", "false")));
        }
    }
}
