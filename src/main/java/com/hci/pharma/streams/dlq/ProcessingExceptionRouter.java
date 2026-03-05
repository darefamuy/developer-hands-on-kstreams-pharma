package com.hci.pharma.streams.dlq;

import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * In-topology dead-letter routing for business-logic processing failures.
 *
 * <p>The Kafka Streams deserialization and production handlers cover failures
 * <em>outside</em> the topology (bad wire bytes, Kafka broker errors).  This
 * class covers failures <em>inside</em> the topology — validation errors and
 * runtime exceptions thrown during actual processing steps.
 *
 * <h2>Usage pattern</h2>
 * <pre>{@code
 * // Inside a Processor.process() method:
 * try {
 *     validateAndProcess(record);
 * } catch (ValidationException e) {
 *     dlqRouter.routeValidationFailure(context, record, dlqTopic, e, "PriceAnomalyProcessor");
 * } catch (Exception e) {
 *     dlqRouter.routeProcessingFailure(context, record, dlqTopic, e, "PriceAnomalyProcessor");
 * }
 * }</pre>
 *
 * <p>Because this runs inside the Streams topology, it uses the same {@link DlqProducer}
 * to write to the DLQ out-of-band (not via a Streams {@code to()} call), avoiding
 * the risk of the DLQ write itself triggering another exception handler loop.
 */
public class ProcessingExceptionRouter {

    private static final Logger log = LoggerFactory.getLogger(ProcessingExceptionRouter.class);

    private final DlqProducer dlqProducer;

    public ProcessingExceptionRouter(DlqProducer dlqProducer) {
        this.dlqProducer = dlqProducer;
    }

    /**
     * Route a record that failed business validation to the DLQ.
     *
     * @param context       the current processor context (for offset/partition metadata)
     * @param record        the record that failed validation
     * @param dlqTopic      the destination DLQ topic name
     * @param cause         the validation exception
     * @param processorName the name of the processor that caught the failure
     * @param contextData   optional key-value metadata (e.g. GTIN, field name)
     */
    public <K, V> void routeValidationFailure(
            ProcessorContext<?, ?> context,
            Record<K, V> record,
            String dlqTopic,
            Exception cause,
            String processorName,
            Map<String, String> contextData) {

        log.warn("Validation failure in {} — routing to DLQ {}: {}",
                processorName, dlqTopic, cause.getMessage());

        DlqEnvelope envelope = DlqEnvelope.builder()
                .originalTopic(context.recordMetadata().map(m -> m.topic()).orElse("unknown"))
                .partition(context.recordMetadata().map(m -> m.partition()).orElse(-1))
                .offset(context.recordMetadata().map(m -> m.offset()).orElse(-1L))
                .errorType(DlqEnvelope.ErrorType.VALIDATION)
                .fromException(cause)
                .processorName(processorName)
                .context(contextData)
                .build();

        dlqProducer.send(dlqTopic, envelope);
    }

    /**
     * Route a record that caused a runtime processing exception to the DLQ.
     */
    public <K, V> void routeProcessingFailure(
            ProcessorContext<?, ?> context,
            Record<K, V> record,
            String dlqTopic,
            Exception cause,
            String processorName,
            Map<String, String> contextData) {

        log.error("Processing failure in {} — routing to DLQ {}: {}",
                processorName, dlqTopic, cause.getMessage(), cause);

        DlqEnvelope envelope = DlqEnvelope.builder()
                .originalTopic(context.recordMetadata().map(m -> m.topic()).orElse("unknown"))
                .partition(context.recordMetadata().map(m -> m.partition()).orElse(-1))
                .offset(context.recordMetadata().map(m -> m.offset()).orElse(-1L))
                .errorType(DlqEnvelope.ErrorType.PROCESSING)
                .fromException(cause)
                .processorName(processorName)
                .context(contextData)
                .build();

        dlqProducer.send(dlqTopic, envelope);
    }
}
