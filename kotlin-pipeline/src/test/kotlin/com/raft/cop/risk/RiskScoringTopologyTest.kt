package com.raft.cop.risk

import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.TopologyTestDriver
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.nio.file.Path
import java.util.Properties
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * Unit tests for the Kotlin topology using [TopologyTestDriver] (no broker required).
 * The topology is stateless, so assertions are read straight off the output topic.
 */
class RiskScoringTopologyTest {

    @TempDir
    lateinit var tempDir: Path

    private fun testProps(): Properties = Properties().apply {
        put(StreamsConfig.APPLICATION_ID_CONFIG, "cop-risk-test")
        put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092")
        put(StreamsConfig.STATE_DIR_CONFIG, tempDir.resolve("kafka-streams").toString())
    }

    private fun <T> withDriver(block: (TopologyTestDriver) -> T): T {
        val driver = TopologyTestDriver(RiskScoringTopology.build().build(), testProps())
        return try {
            block(driver)
        } finally {
            driver.close()
        }
    }

    private fun enrichedEvent(
        assetId: String = "V-1024",
        speedKph: Double = 60.0,
        fuelPct: Double = 50.0,
        assetType: String = "GROUND_VEHICLE"
    ) = EnrichedEvent(
        eventId = "e1",
        assetId = assetId,
        eventTime = 1_000L,
        processedAt = 1_000L,
        latitude = 39.7,
        longitude = -84.1,
        speedKph = speedKph,
        headingDeg = 90.0,
        fuelPct = fuelPct,
        status = "ACTIVE",
        assetType = assetType,
        unit = "3rd Cavalry",
        callsign = "Ghost 7",
        classification = "UNCLASSIFIED"
    )

    @Test
    fun `fast tank scores SEVERE`() = withDriver { driver ->
        val input = driver.createInputTopic(
            RiskScoringTopology.ENRICHED_SOURCE,
            Serdes.String().serializer(),
            JsonSerdes.enriched().serializer()
        )
        val output = driver.createOutputTopic(
            RiskScoringTopology.RISK_SCORES,
            Serdes.String().deserializer(),
            JsonSerdes.riskScore().deserializer()
        )

        input.pipeInput("TANK-04", enrichedEvent(assetId = "TANK-04", speedKph = 180.0, assetType = "MAIN_BATTLE_TANK"))

        val scores = output.readValuesToList()
        assertEquals(1, scores.size)
        assertEquals(RiskLevel.SEVERE, scores[0].level)
        assertTrue(scores[0].score >= 80)
        assertTrue(scores[0].factors.any { it.contains("speed") })
    }

    @Test
    fun `normal vehicle scores LOW`() = withDriver { driver ->
        val input = driver.createInputTopic(
            RiskScoringTopology.ENRICHED_SOURCE,
            Serdes.String().serializer(),
            JsonSerdes.enriched().serializer()
        )
        val output = driver.createOutputTopic(
            RiskScoringTopology.RISK_SCORES,
            Serdes.String().deserializer(),
            JsonSerdes.riskScore().deserializer()
        )

        input.pipeInput("V-1024", enrichedEvent())

        val scores = output.readValuesToList()
        assertEquals(1, scores.size)
        assertEquals(RiskLevel.LOW, scores[0].level)
    }

    @Test
    fun `low fuel contributes a fuel factor`() = withDriver { driver ->
        val input = driver.createInputTopic(
            RiskScoringTopology.ENRICHED_SOURCE,
            Serdes.String().serializer(),
            JsonSerdes.enriched().serializer()
        )
        val output = driver.createOutputTopic(
            RiskScoringTopology.RISK_SCORES,
            Serdes.String().deserializer(),
            JsonSerdes.riskScore().deserializer()
        )

        input.pipeInput("V-1024", enrichedEvent(fuelPct = 8.0))

        val scores = output.readValuesToList()
        assertEquals(1, scores.size)
        assertTrue(scores[0].factors.any { it.contains("fuel") })
    }
}
