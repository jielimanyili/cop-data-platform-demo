package com.raft.cop.producer;

import com.raft.cop.model.AssetReference;
import com.raft.cop.model.AssetTelemetry;
import com.raft.cop.streams.Topics;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Random;
import java.util.UUID;

/**
 * Seeds reference data once and then emits simulated telemetry on a fixed schedule,
 * standing in for real sensor/system feeds. Disable with {@code SIMULATOR_ENABLED=false}
 * (or {@code app.simulator.enabled=false}) to drive the pipeline via the REST API only.
 */
@Component
public class TelemetrySimulator {

    private static final List<AssetReference> REFERENCE_DATA = List.of(
            new AssetReference("V-1024", "GROUND_VEHICLE", "3rd Cavalry", "Ghost 7", "UNCLASSIFIED"),
            new AssetReference("V-2031", "GROUND_VEHICLE", "3rd Cavalry", "Ghost 9", "UNCLASSIFIED"),
            new AssetReference("UAS-07", "UNMANNED_AIRCRAFT", "ISR Squadron", "Raven 3", "UNCLASSIFIED"),
            new AssetReference("TANK-04", "MAIN_BATTLE_TANK", "1st Armored", "Iron 1", "UNCLASSIFIED"),
            new AssetReference("SENSOR-88", "REMOTE_SENSOR", "Signals Detachment", "Picket 4", "UNCLASSIFIED"));

    private final KafkaTemplate<String, Object> template;
    private final Random random = new Random();
    private final boolean enabled;

    public TelemetrySimulator(
            KafkaTemplate<String, Object> template,
            @Value("${app.simulator.enabled:true}") boolean enabled) {
        this.template = template;
        this.enabled = enabled;
    }

    @EventListener(ApplicationReadyEvent.class)
    public void seedReferences() {
        if (!enabled) {
            return;
        }
        REFERENCE_DATA.forEach(r -> template.send(Topics.ASSET_REFERENCE, r.assetId(), r));
    }

    @Scheduled(fixedDelayString = "${app.simulator.interval-ms:2000}")
    public void emitTelemetry() {
        if (!enabled) {
            return;
        }
        AssetReference ref = REFERENCE_DATA.get(random.nextInt(REFERENCE_DATA.size()));
        AssetTelemetry t = new AssetTelemetry(
                UUID.randomUUID().toString(),
                ref.assetId(),
                System.currentTimeMillis(),
                round(39.6 + random.nextDouble() * 0.8), // Dayton-area latitude
                round(-84.3 + random.nextDouble() * 0.8), // longitude
                round(30 + random.nextDouble() * 130),    // speed, occasionally > 100 kph
                round(random.nextDouble() * 360),         // heading
                round(5 + random.nextDouble() * 90),      // fuel, occasionally < 20%
                "ACTIVE");
        template.send(Topics.RAW_TELEMETRY, t.assetId(), t);
    }

    private static double round(double v) {
        return Math.round(v * 100.0) / 100.0;
    }
}
