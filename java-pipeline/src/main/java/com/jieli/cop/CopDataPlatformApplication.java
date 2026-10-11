package com.jieli.cop;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * Common Operational Picture (COP) data platform demo.
 *
 * <p>Demonstrates a Kafka + Kafka Streams ETL pipeline: raw telemetry is ingested,
 * enriched against reference data, aggregated in tumbling windows, and exposed as a
 * queryable "common operational picture" — mirroring Raft's DoD data platform.</p>
 */
@SpringBootApplication
@EnableScheduling
public class CopDataPlatformApplication {

    public static void main(String[] args) {
        SpringApplication.run(CopDataPlatformApplication.class, args);
    }
}
