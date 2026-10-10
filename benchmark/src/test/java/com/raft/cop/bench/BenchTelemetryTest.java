package com.raft.cop.bench;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class BenchTelemetryTest {

    private final ObjectMapper mapper = new ObjectMapper();

    @Test
    void serializesToPipelineWireFormat() throws Exception {
        BenchTelemetry t = new BenchTelemetry(
                "bench-1", "V-1024", 1_000L, 39.7, -84.1, 60.0, 90.0, 50.0, "ACTIVE");

        JsonNode json = mapper.readTree(mapper.writeValueAsString(t));

        // Field names MUST match the pipeline's AssetTelemetry record (JsonSerde contract).
        assertThat(json.get("eventId").asText()).isEqualTo("bench-1");
        assertThat(json.get("assetId").asText()).isEqualTo("V-1024");
        assertThat(json.get("eventTime").asLong()).isEqualTo(1_000L);
        assertThat(json.get("latitude").asDouble()).isEqualTo(39.7);
        assertThat(json.get("longitude").asDouble()).isEqualTo(-84.1);
        assertThat(json.get("speedKph").asDouble()).isEqualTo(60.0);
        assertThat(json.get("headingDeg").asDouble()).isEqualTo(90.0);
        assertThat(json.get("fuelPct").asDouble()).isEqualTo(50.0);
        assertThat(json.get("status").asText()).isEqualTo("ACTIVE");
    }
}
