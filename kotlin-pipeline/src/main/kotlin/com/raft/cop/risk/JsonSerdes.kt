package com.raft.cop.risk

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import org.apache.kafka.common.serialization.Deserializer
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.common.serialization.Serializer

/**
 * Typed JSON serdes backed by Jackson with the Kotlin module registered. Uses Kotlin's
 * SAM conversion to implement the single-method Kafka [Serializer]/[Deserializer]
 * interfaces inline.
 */
object JsonSerdes {

    private val mapper = jacksonObjectMapper()

    fun enriched(): Serde<EnrichedEvent> = serde(EnrichedEvent::class.java)

    fun riskScore(): Serde<RiskScore> = serde(RiskScore::class.java)

    private fun <T : Any> serde(type: Class<T>): Serde<T> = object : Serde<T> {
        override fun serializer(): Serializer<T> = Serializer { _, data ->
            if (data == null) null else mapper.writeValueAsBytes(data)
        }

        override fun deserializer(): Deserializer<T> = Deserializer { _, data ->
            if (data == null) null else mapper.readValue(data, type)
        }
    }
}
