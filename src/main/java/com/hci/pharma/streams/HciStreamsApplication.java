package com.hci.pharma.streams;

import com.hci.pharma.streams.config.AppConfig;
import com.hci.pharma.streams.config.KafkaStreamsConfig;
import com.hci.pharma.streams.dlq.DlqProducer;
import com.hci.pharma.streams.topology.AlertEnrichmentTopology;
import com.hci.pharma.streams.topology.AlertFrequencyTopology;
import com.hci.pharma.streams.topology.PriceSurgeDetectionTopology;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

/**
 * HCI Pharma Kafka Streams — Application Entry Point
 *
 * <h2>Architecture overview</h2>
 * This application runs three independent processing topologies within a
 * single Kafka Streams instance:
 *
 * <ol>
 *   <li><strong>Alert Enrichment</strong> ({@link AlertEnrichmentTopology})
 *       — Stateful KTable join: enriches drug alerts with product data.
 *       Narcotic alerts are split to a dedicated high-priority topic.</li>
 *
 *   <li><strong>Price Surge Detection</strong> ({@link PriceSurgeDetectionTopology})
 *       — Stateful join with confirmed price baseline: emits notifications when
 *       a public price increases by more than the configured threshold.</li>
 *
 *   <li><strong>Alert Frequency Window</strong> ({@link AlertFrequencyTopology})
 *       — Windowed aggregation: counts alerts per GTIN within tumbling time
 *       windows and emits when a frequency threshold is exceeded.</li>
 * </ol>
 *
 * <h2>DLQ strategy</h2>
 * Three DLQ layers protect every stage of the pipeline:
 * <ol>
 *   <li>{@code DlqDeserializationExceptionHandler} — catches corrupt wire bytes</li>
 *   <li>{@code DlqProductionExceptionHandler} — catches output broker failures</li>
 *   <li>{@code ProcessingExceptionRouter} — catches in-topology validation failures</li>
 * </ol>
 *
 * <h2>Graceful shutdown</h2>
 * The application registers a JVM shutdown hook that calls
 * {@code KafkaStreams.close()} with a 30-second drain window. This ensures
 * all in-flight state store commits complete before the process exits.
 */
public class HciStreamsApplication {

    private static final Logger log = LoggerFactory.getLogger(HciStreamsApplication.class);

    public static void main(String[] args) {
        log.info("=== HCI Pharma Kafka Streams — starting ===");

        // ── 1. Load configuration ──────────────────────────────────────────────
        AppConfig config = new AppConfig();
        log.info("Bootstrap servers: {}", config.bootstrapServers());
        log.info("Schema Registry:   {}", config.schemaRegistryUrl());
        log.info("Application ID:    {}", config.applicationId());

        // ── 2. Build Streams properties ────────────────────────────────────────
        // Use exactly-once-v2 in production (requires ≥ 3 brokers).
        // For local dev with a single-broker Docker setup, use AT_LEAST_ONCE.
        boolean exactlyOnce = config.getBoolean("streams.exactly.once", false);
        Properties streamsProps = KafkaStreamsConfig.build(config, exactlyOnce);

        // Schema Registry config map for Avro serde construction
        Map<String, String> srConfig = Map.of(
                AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
                config.schemaRegistryUrl()
        );

        // ── 3. Shared DLQ producer ─────────────────────────────────────────────
        // One DLqProducer is shared across all topologies to avoid opening
        // multiple TCP connections to the Kafka broker from the DLQ path.
        DlqProducer dlqProducer = new DlqProducer(config.bootstrapServers());

        // ── 4. Build topology ──────────────────────────────────────────────────
        StreamsBuilder builder = new StreamsBuilder();

        // Register each topology's sub-graph onto the shared builder.
        // All three sub-graphs are compiled into a single Topology by Streams.
        var productCatalog = new AlertEnrichmentTopology(config, srConfig, dlqProducer)
                .buildTopology(builder);

        new PriceSurgeDetectionTopology(config, srConfig)
                .buildTopology(builder, productCatalog);

        new AlertFrequencyTopology(config, srConfig)
                .buildTopology(builder);

        Topology topology = builder.build();
        log.info("Compiled topology:\n{}", topology.describe());

        // ── 5. Create and start Kafka Streams ──────────────────────────────────
        KafkaStreams streams = new KafkaStreams(topology, streamsProps);

        // State change listener: logs state transitions for observability.
        streams.setStateListener((newState, oldState) ->
                log.info("Kafka Streams state transition: {} → {}", oldState, newState));

        // Uncaught exception handler: restarts the failing stream thread.
        // For catastrophic failures, escalate to REPLACE (restart affected task)
        // rather than SHUTDOWN_APPLICATION (kills everything).
        streams.setUncaughtExceptionHandler(exception -> {
            log.error("Uncaught exception in Streams thread — replacing thread", exception);
            return org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler
                    .StreamThreadExceptionResponse.REPLACE_THREAD;
        });

        // ── 6. Shutdown hook ───────────────────────────────────────────────────
        CountDownLatch shutdownLatch = new CountDownLatch(1);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Shutdown signal received — closing Kafka Streams gracefully...");
            // Allow 30 seconds for in-flight records to complete processing
            // and all state store commits to flush.
            streams.close(java.time.Duration.ofSeconds(30));
            dlqProducer.close();
            log.info("Kafka Streams closed. Goodbye.");
            shutdownLatch.countDown();
        }, "streams-shutdown-hook"));

        // ── 7. Start ───────────────────────────────────────────────────────────
        try {
            streams.start();
            log.info("=== HCI Pharma Kafka Streams — running ===");
            shutdownLatch.await(); // Block main thread until shutdown
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.warn("Main thread interrupted");
        } catch (Exception e) {
            log.error("Fatal error starting Kafka Streams", e);
            streams.close();
            dlqProducer.close();
            System.exit(1);
        }
    }
}
