package com.jieli.cop.risk

/**
 * The enriched event produced by the Java pipeline (topic `cop.enriched-events`).
 * Property names deliberately mirror the Java `EnrichedAssetEvent` record so the two
 * services share a JSON contract without sharing code — the realistic pattern for
 * polyglot services that talk over Kafka.
 */
data class EnrichedEvent(
    val eventId: String,
    val assetId: String,
    val eventTime: Long,
    val processedAt: Long,
    val latitude: Double,
    val longitude: Double,
    val speedKph: Double,
    val headingDeg: Double,
    val fuelPct: Double,
    val status: String,
    val assetType: String?,
    val unit: String?,
    val callsign: String?,
    val classification: String?
)

enum class RiskLevel { LOW, ELEVATED, HIGH, SEVERE }

/** A per-event threat score derived from the enriched telemetry. */
data class RiskScore(
    val assetId: String,
    val eventTime: Long,
    val score: Int,
    val level: RiskLevel,
    val factors: List<String>
)
