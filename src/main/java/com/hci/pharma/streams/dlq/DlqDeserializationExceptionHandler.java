package com.hci.pharma.streams.dlq;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.errors.DeserializationExceptionHandler;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Kafka Streams deserialization exception handler that routes poison-pill
 * messages to a dead-letter queue topic instead of failing or silently dropping.
 *
 * <h2>Why not {@code LogAndContinueExceptionHandler}?</h2>
 * Kafka's built-in {@code LogAndContinueExceptionHandler} swallows the bad
 * record and logs a warning.  In the HCI pharmaceutical context this is
 * unacceptable:
 * <ul>
 *   <li>A corrupt {@code DrugAlert} could be a CLASS_I recall that must not
 *       be silently dropped.</li>
 *   <li>A corrupt {@code PriceUpdate} could mask a BAG-regulated price
 *       correction that affects reimbursement calculations.</li>
 * </ul>
 *
 * <h2>DLQ topic routing</h2>
 * The handler inspects the source topic name to pick the correct DLQ topic:
 * <pre>
 *   hci.drug-alerts.avro   → hci.dlq.drug-alerts.avro
 *   hci.price-updates.avro   → hci.dlq.price-updates.avro
 *   hci.medicinal-products.avro → hci.dlq.medicinal-products.avro
 *   (unknown)       → hci.dlq.unknown.v1
 * </pre>
 *
 * <h2>Configuration keys</h2>
 * The handler reads the following keys from the Streams config map passed to
 * {@link #configure(Map, boolean)}:
 * <ul>
 *   <li>{@code dlq.bootstrap.servers}</li>
 * </ul>
 *
 * <p>These are injected by {@code KafkaStreamsConfig.build()}.
 */
public class DlqDeserializationExceptionHandler implements DeserializationExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(DlqDeserializationExceptionHandler.class);

    private DlqProducer dlqProducer;

    @Override
    public void configure(Map<String, ?> configs) {
        String bootstrapServers = (String) configs.get("dlq.bootstrap.servers");
        if (bootstrapServers == null) {
            // Fall back to the main bootstrap servers config
            bootstrapServers = (String) configs.get("bootstrap.servers");
        }
        this.dlqProducer = new DlqProducer(bootstrapServers);
        log.info("DlqDeserializationExceptionHandler configured");
    }

    @Override
    public DeserializationHandlerResponse handle(
            ProcessorContext context,
            ConsumerRecord<byte[], byte[]> record,
            Exception exception) {

        String dlqTopic = resolveDlqTopic(record.topic());

        log.error("Deserialization failure on topic={} partition={} offset={} — routing to {}",
                record.topic(), record.partition(), record.offset(), dlqTopic, exception);

        dlqProducer.send(
                dlqTopic,
                record.topic(),
                record.partition(),
                record.offset(),
                record.key(),
                record.value(),
                exception,
                DlqEnvelope.ErrorType.DESERIALIZATION
        );

        // CONTINUE: skip this record and carry on — the DLQ has captured it.
        // Use FAIL here if you want the application to halt on any bad record.
        return DeserializationHandlerResponse.CONTINUE;
    }

    /**
     * Map a source topic name to its corresponding DLQ topic.
     * Follows the HCI naming convention: insert "dlq." after the "hci." prefix.
     */
    static String resolveDlqTopic(String sourceTopic) {
        if (sourceTopic == null) return "hci.dlq.unknown.v1";
        if (sourceTopic.startsWith("hci.")) {
            return "hci.dlq." + sourceTopic.substring("hci.".length());
        }
        return "hci.dlq." + sourceTopic;
    }
}
