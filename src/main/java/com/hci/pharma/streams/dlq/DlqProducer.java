package com.hci.pharma.streams.dlq;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * A dedicated Kafka producer that writes {@link DlqEnvelope} JSON to DLQ topics.
 *
 * <h2>Design notes</h2>
 * <ul>
 *   <li>The DLQ producer is intentionally <em>separate</em> from the Streams
 *       internal producer.  Using the Streams producer for DLQ writes inside an
 *       exception handler is unsafe — it may already be in an error state.</li>
 *   <li>DLQ envelopes are serialised as <strong>plain JSON</strong> (not Avro)
 *       so they remain inspectable by humans and tooling even when the Schema
 *       Registry or Avro stack is part of the failure.</li>
 *   <li>The producer uses {@code acks=all} and {@code enable.idempotence=true}
 *       to ensure DLQ records are not silently dropped.</li>
 *   <li>A synchronous send (with timeout) is used for deserialization/production
 *       handlers because they run synchronously inside the Streams thread; async
 *       sends could be lost if the JVM exits before the callback fires.</li>
 * </ul>
 *
 * <h2>DLQ topic naming convention</h2>
 * <pre>
 *   hci.dlq.price-updates.avro     ← price update failures
 *   hci.dlq.drug-alerts.avro     ← drug alert failures
 *   hci.dlq.medicinal-products.avro   ← product catalogue failures
 * </pre>
 */
public class DlqProducer implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(DlqProducer.class);

    private static final int  SEND_TIMEOUT_SECONDS = 10;
    private static final long MAX_BLOCK_MS         = 5_000L;

    private final KafkaProducer<String, String> producer;
    private final ObjectMapper                  mapper;

    public DlqProducer(String bootstrapServers) {
        this.producer = new KafkaProducer<>(buildProducerProps(bootstrapServers));
        this.mapper   = new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        log.info("DlqProducer initialised, bootstrap={}", bootstrapServers);
    }

    /**
     * Synchronously send a {@link DlqEnvelope} to the given DLQ topic.
     *
     * <p>The Kafka message key is set to {@code originalTopic/partition/offset}
     * so that DLQ messages for the same source record are always co-located
     * in the same DLQ partition (enabling ordered replay).
     *
     * @param dlqTopic   target DLQ topic name
     * @param envelope   the envelope to write
     */
    public void send(String dlqTopic, DlqEnvelope envelope) {
        try {
            String json = mapper.writeValueAsString(envelope);
            // Key: "sourceTopic/partition/offset" for deterministic partitioning
            String key  = envelope.getOriginalTopic()
                    + "/" + envelope.getPartition()
                    + "/" + envelope.getOffset();

            ProducerRecord<String, String> record = new ProducerRecord<>(dlqTopic, key, json);

            producer.send(record).get(SEND_TIMEOUT_SECONDS, TimeUnit.SECONDS);

            log.warn("DLQ: routed failed record to {}. key={} error={}",
                    dlqTopic, key, envelope.getErrorMessage());

        } catch (JsonProcessingException e) {
            log.error("DLQ: failed to serialise DlqEnvelope to JSON — this is a bug", e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("DLQ: send interrupted for topic {}", dlqTopic, e);
        } catch (ExecutionException e) {
            log.error("DLQ: Kafka send failed for topic {} — DLQ record LOST", dlqTopic, e);
        } catch (TimeoutException e) {
            log.error("DLQ: Kafka send timed out for topic {} after {}s — DLQ record LOST",
                    dlqTopic, SEND_TIMEOUT_SECONDS, e);
        }
    }

    /**
     * Convenience overload: build a minimal envelope from raw bytes and exception.
     */
    public void send(String dlqTopic,
                     String sourceTopic,
                     int    partition,
                     long   offset,
                     byte[] keyBytes,
                     byte[] valueBytes,
                     Throwable cause,
                     DlqEnvelope.ErrorType errorType) {

        DlqEnvelope envelope = DlqEnvelope.builder()
                .originalTopic(sourceTopic)
                .partition(partition)
                .offset(offset)
                .errorType(errorType)
                .fromException(cause)
                .originalKeyBase64(keyBytes   != null ? java.util.Base64.getEncoder().encodeToString(keyBytes)   : null)
                .originalValueBase64(valueBytes != null ? java.util.Base64.getEncoder().encodeToString(valueBytes) : null)
                .build();

        send(dlqTopic, envelope);
    }

    @Override
    public void close() {
        log.info("Closing DlqProducer...");
        producer.flush();
        producer.close();
    }

    // ── Producer configuration ────────────────────────────────────────────────

    private static Properties buildProducerProps(String bootstrapServers) {
        Properties p = new Properties();
        p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,       bootstrapServers);
        p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,    StringSerializer.class.getName());
        p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,  StringSerializer.class.getName());
        p.put(ProducerConfig.ACKS_CONFIG,                    "all");
        p.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,      "true");
        p.put(ProducerConfig.RETRIES_CONFIG,                 "5");
        p.put(ProducerConfig.MAX_BLOCK_MS_CONFIG,            MAX_BLOCK_MS);
        p.put(ProducerConfig.CLIENT_ID_CONFIG,               "hci-dlq-producer");
        return p;
    }
}
