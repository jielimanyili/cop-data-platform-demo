package com.raft.cop.risk

import kotlin.math.roundToInt

/**
 * Computes a 0-100 threat score from an enriched event. Written in idiomatic Kotlin:
 * `object` singleton, `when` expressions, `coerceAtLeast`/`coerceIn`, string templates,
 * and expression-bodied functions.
 */
object RiskScorer {

    private const val SPEED_LIMIT_KPH = 100.0
    private const val FUEL_LIMIT_PCT = 20.0

    fun score(e: EnrichedEvent): RiskScore {
        val factors = mutableListOf<String>()
        var base = 0.0

        val overSpeed = (e.speedKph - SPEED_LIMIT_KPH).coerceAtLeast(0.0)
        if (overSpeed > 0) {
            base += overSpeed * 0.6
            factors += "speed ${e.speedKph.roundToInt()} kph"
        }

        val fuelShort = (FUEL_LIMIT_PCT - e.fuelPct).coerceAtLeast(0.0)
        if (fuelShort > 0) {
            base += fuelShort * 2.0
            factors += "fuel ${e.fuelPct.roundToInt()}%"
        }

        // High-value platforms amplify anomalies via a multiplier (not a flat floor),
        // so a perfectly normal tank still scores LOW.
        val multiplier = when (e.assetType) {
            "MAIN_BATTLE_TANK", "UNMANNED_AIRCRAFT" -> 2.0
            "GROUND_VEHICLE" -> 1.5
            else -> 1.0
        }

        val score = (base * multiplier).toInt()

        val level = when {
            score >= 80 -> RiskLevel.SEVERE
            score >= 60 -> RiskLevel.HIGH
            score >= 30 -> RiskLevel.ELEVATED
            else -> RiskLevel.LOW
        }

        return RiskScore(
            assetId = e.assetId,
            eventTime = e.eventTime,
            score = score.coerceIn(0, 100),
            level = level,
            factors = factors
        )
    }
}
