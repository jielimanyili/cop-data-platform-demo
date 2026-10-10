package com.raft.cop.web;

import com.raft.cop.model.AssetAggregate;
import com.raft.cop.model.AssetReference;
import com.raft.cop.model.AssetTelemetry;
import com.raft.cop.service.PictureQueryService;
import com.raft.cop.streams.Topics;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

/** REST surface: ingest telemetry/reference data and read the operational picture. */
@RestController
@RequestMapping("/api")
public class CopController {

    private final KafkaTemplate<String, Object> template;
    private final PictureQueryService pictureQuery;

    public CopController(KafkaTemplate<String, Object> template, PictureQueryService pictureQuery) {
        this.template = template;
        this.pictureQuery = pictureQuery;
    }

    /** Ingest a single telemetry event (drives the pipeline without the simulator). */
    @PostMapping("/telemetry")
    public ResponseEntity<Map<String, String>> ingestTelemetry(@RequestBody AssetTelemetry t) {
        template.send(Topics.RAW_TELEMETRY, t.assetId(), t);
        return ResponseEntity.accepted().body(Map.of("status", "accepted", "eventId", t.eventId()));
    }

    /** Upsert reference data for an asset (updates the KTable). */
    @PostMapping("/assets")
    public ResponseEntity<Map<String, String>> ingestReference(@RequestBody AssetReference r) {
        template.send(Topics.ASSET_REFERENCE, r.assetId(), r);
        return ResponseEntity.accepted().body(Map.of("status", "accepted", "assetId", r.assetId()));
    }

    /**
     * The full operational picture: latest per-asset aggregate. Backed by the shared
     * {@link PictureQueryService} (also served over gRPC).
     */
    @GetMapping("/picture")
    public List<AssetAggregate> picture() {
        return pictureQuery.picture();
    }

    /** Point query for a single asset's latest aggregate. */
    @GetMapping("/assets/{assetId}")
    public ResponseEntity<AssetAggregate> asset(@PathVariable String assetId) {
        return pictureQuery.asset(assetId)
                .map(ResponseEntity::ok)
                .orElseGet(() -> ResponseEntity.notFound().build());
    }
}

