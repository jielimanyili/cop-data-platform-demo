package com.raft.cop.streams;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.io.IOException;
import java.util.Map;

/**
 * Minimal Jackson-backed {@link Serde}. Keeping this small (rather than reaching for
 * a heavier serialization framework) makes the serde behavior identical in
 * {@code TopologyTestDriver} tests and in production.
 */
public class JsonSerde<T> implements Serde<T> {

    private final ObjectMapper mapper;
    private final Class<T> type;

    public JsonSerde(Class<T> type, ObjectMapper mapper) {
        this.type = type;
        this.mapper = mapper;
    }

    @Override
    public Serializer<T> serializer() {
        return new Serializer<>() {
            @Override
            public void configure(Map<String, ?> configs, boolean isKey) {
            }

            @Override
            public byte[] serialize(String topic, T data) {
                if (data == null) {
                    return null;
                }
                try {
                    return mapper.writeValueAsBytes(data);
                } catch (JsonProcessingException e) {
                    throw new SerializationException("Error serializing " + type.getSimpleName(), e);
                }
            }
        };
    }

    @Override
    public Deserializer<T> deserializer() {
        return new Deserializer<>() {
            @Override
            public void configure(Map<String, ?> configs, boolean isKey) {
            }

            @Override
            public T deserialize(String topic, byte[] data) {
                if (data == null) {
                    return null;
                }
                try {
                    return mapper.readValue(data, type);
                } catch (IOException e) {
                    throw new SerializationException("Error deserializing " + type.getSimpleName(), e);
                }
            }
        };
    }
}
