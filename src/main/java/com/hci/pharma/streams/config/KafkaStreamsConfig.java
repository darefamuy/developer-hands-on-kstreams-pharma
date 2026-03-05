package com.hci.pharma.streams.config;

import com.hci.pharma.streams.dlq.DlqDeserializationExceptionHandler;
import com.hci.pharma.streams.dlq.DlqProductionExceptionHandler;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;

import java.util.Properties;

/**
 * Builds the {@link Properties} object that configures a Kafka Streams instance.
 *
 * <p>Key design decisions documented inline:
 * <ul>
 *   <li>Default Serde is {@code SpecificAvroSerde} — all topics carry Avro messages.</li>
 *   <li>Deserialization failures route to the DLQ handler, not LogAndContinue,
 *       so poison pills are never silently dropped.</li>
 *   <li>Production exception handler retries transient errors and DLQs persistent ones.</li>
 *   <li>exactly-once-v2 is preferred for pharmaceutical data integrity; falls back
 *       to at-least-once when the broker count is 1 (local dev).</li>
 * </ul>
 */
public final class KafkaStreamsConfig {

    private KafkaStreamsConfig() {}

    /**
     * Build the full Kafka Streams {@link Properties} from an {@link AppConfig}.
     *
     * @param config application configuration
     * @param exactlyOnce when {@code true}, sets processing.guarantee to
     *                    exactly_once_v2 (requires ≥3 brokers)
     */
    public static Properties build(AppConfig config, boolean exactlyOnce) {
        Properties props = new Properties();

        // ── Identity ──────────────────────────────────────────────────────────
        props.put(StreamsConfig.APPLICATION_ID_CONFIG,
                config.applicationId());
        props.put(StreamsConfig.CLIENT_ID_CONFIG,
                config.get("streams.client.id"));
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,
                config.bootstrapServers());

        // ── Serdes ────────────────────────────────────────────────────────────
        // String keys (GTIN), Avro values
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,
                Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,
                SpecificAvroSerde.class.getName());

        // Schema Registry URL must be available to all Avro serdes
        props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
                config.schemaRegistryUrl());

        // ── Reliability ───────────────────────────────────────────────────────
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG,
                exactlyOnce
                        ? StreamsConfig.EXACTLY_ONCE_V2
                        : StreamsConfig.AT_LEAST_ONCE);

        props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG,
                config.getInt("streams.replication.factor"));

        // ── Exception Handlers ────────────────────────────────────────────────
        // Deserialization failures: route bad bytes to DLQ topic (do NOT use
        // LogAndContinue in production — silent data loss is unacceptable).
        props.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG,
                DlqDeserializationExceptionHandler.class.getName());

        // Production failures: attempt retries, then DLQ persistent errors.
        props.put(StreamsConfig.DEFAULT_PRODUCTION_EXCEPTION_HANDLER_CLASS_CONFIG,
                DlqProductionExceptionHandler.class.getName());

        // ── Threading ─────────────────────────────────────────────────────────
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG,
                config.getInt("streams.num.stream.threads"));

        // ── Consumer tuning ───────────────────────────────────────────────────
        // Reset to earliest for a fresh application; change to latest in
        // production once steady-state is reached.
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // ── Producer tuning ───────────────────────────────────────────────────
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        props.put(ProducerConfig.LINGER_MS_CONFIG, "5");
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, "131072"); // 128 KB

        // ── State store ───────────────────────────────────────────────────────
        // Commit interval: 100ms for low-latency pharmacovigilance pipelines
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, "100");

        // Inject Schema Registry URL into DLQ handler config (it needs its
        // own producer to write DLQ envelopes)
        props.put("dlq.schema.registry.url", config.schemaRegistryUrl());
        props.put("dlq.bootstrap.servers", config.bootstrapServers());
        props.put("dlq.max.retries", String.valueOf(config.dlqMaxRetries()));

        return props;
    }

    /**
     * Schema Registry client config map for use when constructing Serdes manually.
     */
    public static java.util.Map<String, String> schemaRegistryConfig(AppConfig config) {
        return java.util.Map.of(
                AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
                config.schemaRegistryUrl()
        );
    }
}
