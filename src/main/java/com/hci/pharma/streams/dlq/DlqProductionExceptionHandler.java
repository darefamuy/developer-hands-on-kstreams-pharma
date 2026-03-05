package com.hci.pharma.streams.dlq;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.streams.errors.ProductionExceptionHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Handles failures when the Kafka Streams internal producer cannot write
 * an output record to a topic.
 *
 * <h2>Production failure categories</h2>
 * <ul>
 *   <li><strong>Transient</strong> (network blip, leader election, throttle):
 *       Kafka's producer retries handle these automatically before this handler
 *       is ever called.  If they still reach here, we FAIL the Streams task —
 *       Kafka Streams will rebalance and retry from the last committed offset.</li>
 *   <li><strong>Persistent</strong> (message too large, serialization failure,
 *       topic does not exist): retrying will never help.  Route to DLQ and
 *       CONTINUE so the pipeline does not halt for one bad record.</li>
 * </ul>
 *
 * <h2>Decision logic</h2>
 * <pre>
 *   RecordTooLargeException      → DLQ + CONTINUE
 *   SerializationException       → DLQ + CONTINUE
 *   UnknownTopicOrPartition      → FAIL (ops must create the topic)
 *   Everything else              → FAIL (let Streams retry from checkpoint)
 * </pre>
 */
public class DlqProductionExceptionHandler implements ProductionExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(DlqProductionExceptionHandler.class);

    private DlqProducer dlqProducer;

    @Override
    public void configure(Map<String, ?> configs) {
        String bootstrapServers = (String) configs.get("dlq.bootstrap.servers");
        if (bootstrapServers == null) {
            bootstrapServers = (String) configs.get("bootstrap.servers");
        }
        this.dlqProducer = new DlqProducer(bootstrapServers);
        log.info("DlqProductionExceptionHandler configured");
    }

    @Override
    public ProductionExceptionHandlerResponse handle(
            ProducerRecord<byte[], byte[]> record,
            Exception exception) {

        String exClass = exception.getClass().getName();

        // Persistent failures: the record itself is bad and will never succeed.
        // Route to DLQ and continue — do not halt the pipeline.
        if (isPersistentFailure(exClass)) {
            String dlqTopic = DlqDeserializationExceptionHandler.resolveDlqTopic(record.topic());
            log.error("Production failure (persistent) on topic={} — routing to DLQ {}",
                    record.topic(), dlqTopic, exception);

            dlqProducer.send(
                    dlqTopic,
                    record.topic(),
                    /* partition */ record.partition() != null ? record.partition() : -1,
                    /* offset — unknown at produce time */ -1L,
                    record.key(),
                    record.value(),
                    exception,
                    DlqEnvelope.ErrorType.PRODUCTION
            );

            return ProductionExceptionHandlerResponse.CONTINUE;
        }

        // Transient or unknown failures: signal FAIL so Streams rebalances
        // and retries from the last checkpoint.  This is the safe default for
        // pharmaceutical data where silent data loss is unacceptable.
        log.error("Production failure (transient/unknown) on topic={} — signalling FAIL for Streams retry",
                record.topic(), exception);
        return ProductionExceptionHandlerResponse.FAIL;
    }

    private boolean isPersistentFailure(String exceptionClass) {
        return exceptionClass.contains("RecordTooLargeException")
                || exceptionClass.contains("SerializationException")
                || exceptionClass.contains("InvalidTopicException")
                || exceptionClass.contains("CorruptRecordException");
    }
}
