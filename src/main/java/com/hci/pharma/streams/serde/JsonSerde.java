package com.hci.pharma.streams.serde;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.io.IOException;
import java.util.Map;

/**
 * Generic Jackson-based {@link Serde} for POJO types used as Kafka Streams
 * output models that do not have Avro schemas.
 *
 * <p>Used for:
 * <ul>
 *   <li>{@code EnrichedAlert} → {@code hci.alerts.enriched.v1}</li>
 *   <li>{@code PriceSurgeNotification} → {@code hci.prices.surge-notifications.v1}</li>
 * </ul>
 *
 * <h2>Usage</h2>
 * <pre>{@code
 * Serde<EnrichedAlert> serde = new JsonSerde<>(EnrichedAlert.class);
 * }</pre>
 */
public class JsonSerde<T> implements Serde<T> {

    private static final ObjectMapper MAPPER = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

    private final Class<T> targetClass;

    public JsonSerde(Class<T> targetClass) {
        this.targetClass = targetClass;
    }

    @Override
    public Serializer<T> serializer() {
        return new Serializer<>() {
            @Override
            public void configure(Map<String, ?> configs, boolean isKey) {}

            @Override
            public byte[] serialize(String topic, T data) {
                if (data == null) return null;
                try {
                    return MAPPER.writeValueAsBytes(data);
                } catch (Exception e) {
                    throw new SerializationException("Failed to serialize to JSON: " + e.getMessage(), e);
                }
            }
        };
    }

    @Override
    public Deserializer<T> deserializer() {
        return new Deserializer<>() {
            @Override
            public void configure(Map<String, ?> configs, boolean isKey) {}

            @Override
            public T deserialize(String topic, byte[] data) {
                if (data == null) return null;
                try {
                    return MAPPER.readValue(data, targetClass);
                } catch (IOException e) {
                    throw new SerializationException("Failed to deserialize from JSON: " + e.getMessage(), e);
                }
            }
        };
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {}

    @Override
    public void close() {}
}
