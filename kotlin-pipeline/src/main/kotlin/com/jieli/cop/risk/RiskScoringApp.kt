package com.jieli.cop.risk

import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsConfig
import java.util.Properties
import java.util.concurrent.CountDownLatch

/**
 * Headless Kafka Streams entrypoint for the Kotlin risk-scoring stage.
 * Run with: `mvn -pl kotlin-pipeline exec:java`
 * (or `java -jar kotlin-pipeline/target/kotlin-pipeline-0.0.1-SNAPSHOT.jar` after `mvn package`).
 */
fun main() {
    val props = Properties().apply {
        put(StreamsConfig.APPLICATION_ID_CONFIG, System.getenv("STREAMS_APPLICATION_ID") ?: "cop-risk-scoring")
        put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, System.getenv("KAFKA_BOOTSTRAP_SERVERS") ?: "localhost:9092")
        put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().javaClass.name)
        put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().javaClass.name)
        put(StreamsConfig.STATE_DIR_CONFIG, "./data/kotlin-kafka-streams")
    }

    val streams = KafkaStreams(RiskScoringTopology.build().build(), props)
    val latch = CountDownLatch(1)

    Runtime.getRuntime().addShutdownHook(Thread {
        streams.close()
        latch.countDown()
    })

    streams.start()
    latch.await()
}
