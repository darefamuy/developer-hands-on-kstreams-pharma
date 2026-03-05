package com.hci.pharma.streams.dlq;

import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

/**
 * Unit tests for DLQ topic resolution and exception handler logic.
 */
class DlqDeserializationExceptionHandlerTest {

    @Test
    void resolveDlqTopic_hciPrefixedTopic_insertsHciDlq() {
        assertThat(DlqDeserializationExceptionHandler.resolveDlqTopic("hci.drug-alerts.avro"))
                .isEqualTo("hci.dlq.drug-alerts.avro");

        assertThat(DlqDeserializationExceptionHandler.resolveDlqTopic("hci.price-updates.avro"))
                .isEqualTo("hci.dlq.price-updates.avro");

        assertThat(DlqDeserializationExceptionHandler.resolveDlqTopic("hci.medicinal-products.avro"))
                .isEqualTo("hci.dlq.medicinal-products.avro");
    }

    @Test
    void resolveDlqTopic_nonHciTopic_prefixesDlq() {
        assertThat(DlqDeserializationExceptionHandler.resolveDlqTopic("external.topic"))
                .isEqualTo("hci.dlq.external.topic");
    }

    @Test
    void resolveDlqTopic_nullTopic_returnsUnknown() {
        assertThat(DlqDeserializationExceptionHandler.resolveDlqTopic(null))
                .isEqualTo("hci.dlq.unknown.v1");
    }

    @Test
    void dlqEnvelope_builder_capturesAllFields() {
        Exception cause = new RuntimeException("test failure");

        DlqEnvelope envelope = DlqEnvelope.builder()
                .originalTopic("hci.drug-alerts.avro")
                .partition(2)
                .offset(1234L)
                .errorType(DlqEnvelope.ErrorType.DESERIALIZATION)
                .fromException(cause)
                .processorName("TestProcessor")
                .context(java.util.Map.of("gtin", "7680123456789"))
                .build();

        assertThat(envelope.getOriginalTopic()).isEqualTo("hci.drug-alerts.avro");
        assertThat(envelope.getPartition()).isEqualTo(2);
        assertThat(envelope.getOffset()).isEqualTo(1234L);
        assertThat(envelope.getErrorType()).isEqualTo(DlqEnvelope.ErrorType.DESERIALIZATION);
        assertThat(envelope.getErrorMessage()).isEqualTo("test failure");
        assertThat(envelope.getExceptionClass()).contains("RuntimeException");
        assertThat(envelope.getStackTrace()).isNotBlank();
        assertThat(envelope.getProcessorName()).isEqualTo("TestProcessor");
        assertThat(envelope.getContext()).containsEntry("gtin", "7680123456789");
        assertThat(envelope.getFailedAt()).isNotNull(); // auto-set in constructor
    }

    @Test
    void dlqEnvelope_toString_includesKeyFields() {
        DlqEnvelope envelope = DlqEnvelope.builder()
                .originalTopic("hci.price-updates.avro")
                .partition(0)
                .offset(99L)
                .errorType(DlqEnvelope.ErrorType.VALIDATION)
                .errorMessage("price inversion")
                .build();

        String str = envelope.toString();
        assertThat(str).contains("hci.price-updates.avro");
        assertThat(str).contains("VALIDATION");
    }
}
