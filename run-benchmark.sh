#!/usr/bin/env bash
set -euo pipefail

# Throughput benchmark for the COP data platform.
#
# Prereqs:
#   1. Kafka running:        docker compose up -d
#   2. Pipeline running:     mvn -pl java-pipeline spring-boot:run
#
# Usage:
#   ./run-benchmark.sh [--records 200000] [--rate 0] [--phase both]
#
# The harness produces telemetry at max rate into cop.raw-telemetry and samples the
# pipeline's output rate + consumer lag each second, then prints a summary verdict.
#
# To find the platform's events/sec ceiling, sweep these knobs and re-run:
#   * partitions on cop.raw-telemetry (default 3)  — more partitions = more parallelism
#   * num.stream.threads in java-pipeline (default 1)
#   * CACHE_MAX_BYTES_BUFFERING_CONFIG (demo = 0 for fresh interactive queries;
#     re-enable caching + suppress() for throughput)

cd "$(dirname "$0")"

RECORDS="${RECORDS:-200000}"
RATE="${RATE:-0}"
PHASE="${PHASE:-both}"
BOOTSTRAP="${KAFKA_BOOTSTRAP_SERVERS:-localhost:9092}"

echo "==> Building benchmark harness"
mvn -q -pl benchmark -am package -DskipTests

echo "==> Running benchmark (records=${RECORDS}, rate=${RATE}, phase=${PHASE})"
java -cp benchmark/target/benchmark-0.0.1-SNAPSHOT.jar com.jieli.cop.bench.BenchmarkMain \
  --bootstrap-servers "${BOOTSTRAP}" \
  --records "${RECORDS}" \
  --rate "${RATE}" \
  --phase "${PHASE}"
