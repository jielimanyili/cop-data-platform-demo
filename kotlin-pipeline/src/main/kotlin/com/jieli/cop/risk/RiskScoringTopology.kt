package com.jieli.cop.risk

import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.kstream.Consumed
import org.apache.kafka.streams.kstream.KStream
import org.apache.kafka.streams.kstream.Produced

/**
 * Kotlin stage of the COP data platform: consumes enriched events produced by the Java
 * pipeline, assigns each a threat score, and sinks the result to `cop.risk-scores`.
 */
object RiskScoringTopology {

    const val ENRICHED_SOURCE = "cop.enriched-events"
    const val RISK_SCORES = "cop.risk-scores"

    fun build(): StreamsBuilder {
        val builder = StreamsBuilder()

        val enriched: KStream<String, EnrichedEvent> = builder.stream(
            ENRICHED_SOURCE,
            Consumed.with(Serdes.String(), JsonSerdes.enriched())
        )

        enriched
            .mapValues { evt -> RiskScorer.score(evt) }
            .to(RISK_SCORES, Produced.with(Serdes.String(), JsonSerdes.riskScore()))

        return builder
    }
}
