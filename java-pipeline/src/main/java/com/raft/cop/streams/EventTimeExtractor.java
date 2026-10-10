package com.raft.cop.streams;

import com.raft.cop.model.AssetTelemetry;
import com.raft.cop.model.EnrichedAssetEvent;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;

/**
 * Extracts event time from the record payload so windowing is based on when the
 * event actually happened, not when it was consumed. Falls back to the record's
 * embedded timestamp for unknown payloads. This is what keeps late/out-of-order
 * events in the correct window (watermark/grace handling is a follow-on concern).
 */
public class EventTimeExtractor implements TimestampExtractor {

    @Override
    public long extract(ConsumerRecord<Object, Object> record, long partitionTime) {
        Object value = record.value();
        if (value instanceof AssetTelemetry t) {
            return t.eventTime();
        }
        if (value instanceof EnrichedAssetEvent e) {
            return e.eventTime();
        }
        return record.timestamp();
    }
}
