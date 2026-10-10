# COP Data Platform — Kafka + Kafka Streams Demo

A runnable **Common Operational Picture (COP)** data platform built to mirror the
**Raft Senior Engineer (Data Platform)** job description: ingest heterogeneous
operational data, enrich and aggregate it in real time, and expose it as a queryable
picture for time-critical decisions.

Built as a **polyglot JVM platform** — a **Java 17 + Spring Boot** Kafka Streams
ETL stage plus a **Kotlin 2.4** Kafka Streams risk-scoring stage — and packaged for
**Docker Compose** and **Kubernetes (Helm)**.

---

## What this demonstrates

| Raft JD requirement | Where it lives here |
|---|---|
| Build data infrastructure with **Kafka / Kafka Streams** | `TelemetryTopology` — KStream, KTable, join, windowing, state stores |
| Build **ETL pipelines** | Extract (raw telemetry topic) → Transform (filter/re-key/enrich/aggregate) → Load (sink topics + interactive query) |
| **Large-scale databases** for low latency | Aggregate "operational picture" served from a local **RocksDB state store** via interactive queries; README documents Pinot/Elasticsearch as the production serving layer |
| **Java** | Core ETL stage is Java 17 (records, streams DSL) |
| **Kotlin** (recruiter-flagged) | Risk-scoring stage in idiomatic Kotlin — data classes, `when` expressions, SAM serdes |
| **gRPC / protobuf** | Typed `.proto` query contract (`OperationalPictureService`) alongside REST — gRPC for internal calls, REST for tooling |
| **Software design & architecture patterns** | Producer/consumer, partition-key design, event-time vs processing-time, fault-tolerance notes |
| **Cloud-native environments** | Dockerfile, Docker Compose, Helm chart, health/liveness/readiness probes |
| **Package managers (Maven)** | Maven `pom.xml` |
| **Kubernetes** (highly preferred) | `helm/raft-cop` chart (Deployment + Service + Prometheus scrape annotations) |
| **Monitoring: Prometheus/Grafana** (highly preferred) | Micrometer + Prometheus registry, Grafana dashboard with consumer-lag & throughput panels |
| **Helm Charts** (highly preferred) | `helm/raft-cop` with `values.yaml`, helpers, deployment & service templates |

---

## Architecture

```
                         ┌─────────────────────────────────────────────┐
  Sensor / asset feeds ─▶│                Kafka (3 partitions)          │
  (TelemetrySimulator or │  cop.raw-telemetry   cop.asset-reference     │
   POST /api/telemetry)  └──────────────┬──────────────┬────────────────┘
                                        │              │
                         ┌──────────────▼──────────────▼────────────────┐
                         │            Kafka Streams (this app)          │
                         │   filter(valid) → re-key(assetId)            │
                         │      └─ join(KTable: asset-reference)        │
                         │             │                                │
                         │    ┌────────┼─────────────────┐              │
                         │    ▼        ▼                 ▼              │
                         │ enriched  groupByKey       threshold         │
                         │ -events   + 10s window     filters           │
                         │  (sink)    aggregate        (speed/fuel)     │
                         │              │                 │             │
                         │              ▼                 ▼             │
                         │      asset-aggregates       alerts (sink)    │
                         │      (sink) + state store                   │
                         └──────────────┬──────────────────────────────┘
                                        │ interactive query
                                        ▼
                        GET /api/picture  ──  Common Operational Picture
```

**One-line narrative:** sources produce raw telemetry into Kafka; the **Java** stage
validates, enriches against reference data, aggregates in tumbling windows, and
flags anomalies; the operator reads the latest per-asset aggregate straight from
the stream state store. A downstream **Kotlin** stage consumes `cop.enriched-events`
and publishes a per-event threat score to `cop.risk-scores`:

```
cop.enriched-events ──▶ [ Kotlin: RiskScoringTopology ] ──▶ cop.risk-scores
```

Both stages are Kafka Streams apps sharing one cluster and a JSON contract, written
in different JVM languages — a realistic polyglot pattern (and the recruiter flagged
Kotlin, so this shows it directly).

The operational picture is served over **two transports**: REST (`/api/picture`) for
browsers/tooling and **gRPC** (a `.proto` `OperationalPictureService`) for typed,
low-latency internal calls — the classic "Kafka for async events, gRPC for sync
requests" pattern.

---

## Repository layout

```
├── pom.xml                                # parent (multi-module, polyglot JVM)
├── java-pipeline/                         # Java 17 + Spring Boot Kafka Streams ETL
│   ├── pom.xml
│   ├── Dockerfile
│   └── src/main/
│       ├── proto/cop/v1/operational_picture.proto   # gRPC contract
│       └── java/com/raft/cop/
│           ├── CopDataPlatformApplication.java
│           ├── model/                          # records: telemetry, reference, enriched, aggregate, alert
│           ├── streams/TelemetryTopology.java  # ingest → enrich → aggregate → alert
│           ├── config/                         # KafkaStreams + producer + gRPC server
│           ├── service/                        # PictureQueryService (shared REST + gRPC)
│           ├── grpc/                           # gRPC service impl + proto converter
│           ├── producer/TelemetrySimulator.java
│           └── web/CopController.java          # REST ingest + GET /api/picture
├── kotlin-pipeline/                       # Kotlin 2.4 Kafka Streams risk scoring
│   ├── pom.xml
│   ├── Dockerfile
│   └── src/main/kotlin/com/raft/cop/risk/
│       ├── RiskModel.kt                    # data classes + enum
│       ├── RiskScorer.kt                   # idiomatic scoring logic
│       ├── JsonSerdes.kt                   # SAM-converted serdes
│       ├── RiskScoringTopology.kt          # enriched-events → risk-scores
│       └── RiskScoringApp.kt               # headless main()
├── benchmark/                             # throughput harness (load generator + lag monitor)
│   ├── pom.xml
│   └── src/main/java/com/raft/cop/bench/  # BenchmarkMain, LoadGenerator, PipelineMonitor
├── run-benchmark.sh                       # one-command benchmark runner
├── docker-compose.yml                      # Kafka + Zookeeper + Prometheus + Grafana (+ apps profile)
├── monitoring/                             # Prometheus scrape config + Grafana dashboard
└── helm/raft-cop/                          # Helm chart (Deployment, Service, helpers)
```

---

## Quickstart

### 1. Start Kafka + observability stack

```bash
docker compose up -d
# wait for "=== topics ===" from kafka-setup, or:
docker compose logs -f kafka-setup
```

### 2. Run the Java ETL stage

```bash
mvn -pl java-pipeline spring-boot:run
```

The simulator seeds 5 reference assets and emits telemetry every 2 seconds. Some
events intentionally exceed the speed threshold (100 kph) or drop below the fuel
threshold (20%) to trigger alerts.

### 3. Run the Kotlin risk-scoring stage (second terminal)

```bash
mvn -pl kotlin-pipeline exec:java
# or from the shaded jar:
# java -cp kotlin-pipeline/target/kotlin-pipeline-0.0.1-SNAPSHOT.jar com.raft.cop.risk.RiskScoringAppKt
```

### 4. Observe

```bash
# Common operational picture (latest per-asset aggregate from the state store)
curl -s localhost:8080/api/picture | jq

# Manually ingest a telemetry event
curl -s -X POST localhost:8080/api/telemetry \
  -H 'Content-Type: application/json' \
  -d '{"eventId":"e-manual","assetId":"V-1024","eventTime":'$(date +%s000)',"latitude":39.7,"longitude":-84.1,"speedKph":95,"headingDeg":45,"fuelPct":55,"status":"ACTIVE"}'

# Manually upsert reference data
curl -s -X POST localhost:8080/api/assets \
  -H 'Content-Type: application/json' \
  -d '{"assetId":"V-1024","assetType":"GROUND_VEHICLE","unit":"3rd Cavalry","callsign":"Ghost 7","classification":"UNCLASSIFIED"}'

# Watch topics
docker compose exec kafka kafka-console-consumer --bootstrap-server localhost:9092 \
  --topic cop.enriched-events --from-beginning
docker compose exec kafka kafka-console-consumer --bootstrap-server localhost:9092 \
  --topic cop.alerts --from-beginning
docker compose exec kafka kafka-console-consumer --bootstrap-server localhost:9092 \
  --topic cop.risk-scores --from-beginning
```

### 5. Grafana

- Grafana: http://localhost:3000 (admin / admin) — "COP Data Platform Overview"
- Prometheus: http://localhost:9090

### 6. gRPC

The operational picture is also served over gRPC (port 6565) from a `.proto` contract
(`cop.v1.OperationalPictureService`) — gRPC for typed internal calls, REST for tooling:

```bash
# Point query for one asset (requires grpcurl: brew install grpcurl)
grpcurl -plaintext -d '{"assetId":"V-1024"}' localhost:6565 cop.v1.OperationalPictureService/GetAsset

# Full picture (server-streaming)
grpcurl -plaintext -d '{}' localhost:6565 cop.v1.OperationalPictureService/GetPicture
```

To run both pipelines in containers instead: `docker compose --profile apps up -d`.

---

## The ETL pipeline, step by step

1. **Extract** — `cop.raw-telemetry` is consumed as a `KStream<String, AssetTelemetry>`.
2. **Transform (validate + re-key)** — malformed records are filtered out, then the
   stream is re-keyed by `assetId` (the partition key) so related events share a
   partition and preserve per-entity ordering.
3. **Transform (enrich)** — a `KStream–KTable` join against `cop.asset-reference`
   (a log-compacted `KTable`) adds `unit`, `callsign`, `assetType`, and
   `classification` to each event.
4. **Load (enriched)** — enriched events sink to `cop.enriched-events`, ready for a
   Pinot table or Elasticsearch index in production.
5. **Transform (aggregate)** — events are grouped by asset and folded into a
   **10-second tumbling window** aggregate (`AssetAggregate`), persisted in a
   **RocksDB windowed state store**.
6. **Load (operational picture)** — `GET /api/picture` reads the state store via an
   **interactive query**, returning the latest aggregate per asset with zero extra
   infrastructure.
7. **Detect** — threshold filters emit `cop.alerts` for speed and fuel anomalies.
8. **Score (Kotlin)** — `kotlin-pipeline` consumes `cop.enriched-events`, computes a
   0–100 threat score per event (`RiskScorer`), and sinks `cop.risk-scores`.

---

## Kafka Streams concepts demonstrated

- **KStream vs KTable** — raw telemetry is an append-only `KStream`; asset reference
  data is a changelog `KTable` (latest value per key).
- **Partition key design** — events are keyed by `assetId`, co-locating related
  events on one partition (per-entity ordering).
- **Stream–table join** — enrich fast-moving telemetry with slowly-changing reference data.
- **Tumbling windows** — `TimeWindows.ofSizeWithNoGrace` aggregates per asset per 10s.
- **State stores + interactive queries** — the aggregate store is queryable directly.
- **Event-time extraction** — `EventTimeExtractor` drives windowing from payload
  event time, not consume time.
- **Exactly-once vs at-least-once** — `acks=all` on the producer for durability;
  production notes below discuss the full trade-off.

---

## Testing

```bash
mvn test          # runs all modules' test suites
```

- **java-pipeline** — `TelemetryTopologyTest` (TopologyTestDriver) asserts the join
  enrichment, windowed aggregation, threshold alerts, and malformed-record drop;
  `OperationalPictureGrpcServiceTest` (in-process gRPC) asserts the protobuf contract:
  point query, `NOT_FOUND` handling, and server-streaming.
- **kotlin-pipeline** — `RiskScoringTopologyTest` (TopologyTestDriver) asserts the
  fast-tank → SEVERE scoring, normal-vehicle → LOW, and fuel-factor contribution.
- **benchmark** — `BenchTelemetryTest` guards the wire-format contract against the
  pipeline's `AssetTelemetry`; `RateMeterTest` validates the throughput meter.

---

## Benchmarking throughput (events/sec)

The `benchmark` module measures how many events/sec the platform can sustain — a real
number instead of an estimate.

```bash
# 1. Kafka up + the pipeline running
docker compose up -d
mvn -pl java-pipeline spring-boot:run

# 2. Run the harness (produces at max rate, samples lag, prints a verdict)
./run-benchmark.sh --records 200000
# or directly:
java -cp benchmark/target/benchmark-0.0.1-SNAPSHOT.jar com.raft.cop.bench.BenchmarkMain \
  --records 200000 --phase both
```

It produces telemetry into `cop.raw-telemetry` at max rate and samples, every second:

```
output_total=1,234  lag=0  pipeline_rate=12,340/s
...
=== SUMMARY ===
records produced:     200,000
producer rate:        40,000 events/sec
pipeline output rate: 12,340 events/sec (enriched)
final input lag:      58,000
verdict:              pipeline fell behind (lag > 0) — that rate exceeds the platform's capacity
```

- **`lag == 0` at the end** → the pipeline kept up; the sustained `pipeline output
  rate` is your measured events/sec.
- **`lag > 0`** → the producer outpaced the pipeline; the number is the *ceiling* you
  just exceeded.

To find the ceiling, sweep the knobs (documented in `run-benchmark.sh`): partitions
on `cop.raw-telemetry` (default 3), `num.stream.threads`, and
`CACHE_MAX_BYTES_BUFFERING_CONFIG` (demo sets 0 for fresh queries; re-enable caching
+ `suppress()` for throughput).

---

## Production hardening (what I would do next)

- **Exactly-once** — idempotent producer + Kafka transactions, or accept
  at-least-once and dedupe on `eventId` downstream.
- **Window finalization** — re-enable record caching and add
  `suppress(Suppressed.untilWindowCloses(...))` to emit only final window results
  instead of every running update.
- **Serving layer** — stream `cop.enriched-events` into **Apache Pinot** (real-time
  OLAP) via Pinot's Kafka ingestion, or **Elasticsearch** via Kafka Connect, so the
  operational picture scales to high concurrent query QPS.
- **Late/out-of-order data** — add watermarks and grace periods; route late events to
  a side channel.
- **Service mesh** — run behind **Istio** for mTLS and zero-trust between services.
- **Stateful storage** — in Kubernetes, back the `STATE_DIR` with a persistent volume
  (StatefulSet) so local state survives pod restarts.
- **Security** — secure Kafka with SASL/TLS, classify data, and honor air-gapped/DDIL
  deployment constraints.

---

## Why this matches Raft

Raft is "building a data platform for the DoD, aimed at enhancing operators'
awareness of critical data … enriched, rendered queryable, and presented as a common
operational picture." This repo is exactly that flow — Kafka ingest → Kafka Streams
enrich/aggregate/score → queryable operational picture — in the Java **and Kotlin**
JVM stack the team uses, with the observability and Kubernetes packaging the role
calls for.


