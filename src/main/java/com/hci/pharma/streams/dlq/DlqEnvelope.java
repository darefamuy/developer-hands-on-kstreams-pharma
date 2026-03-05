package com.hci.pharma.streams.dlq;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

import java.time.Instant;
import java.util.Map;

/**
 * Dead-Letter Queue envelope.
 *
 * <p>Every message that cannot be processed is wrapped in this envelope and
 * written to the corresponding DLQ topic.  The envelope captures everything
 * an operator needs to understand, replay, or discard the failed message:
 *
 * <ul>
 *   <li>{@code originalTopic} / {@code partition} / {@code offset} — locate the
 *       original message in the source topic for manual inspection.</li>
 *   <li>{@code errorType} — broad category (DESERIALIZATION, VALIDATION,
 *       PROCESSING, PRODUCTION) for routing/alerting rules.</li>
 *   <li>{@code errorMessage} / {@code stackTrace} — full diagnostic detail.</li>
 *   <li>{@code retryCount} — number of processing attempts before DLQ routing.</li>
 *   <li>{@code originalKeyBytes} / {@code originalValueBytes} — base-64 encoded
 *       raw bytes so the message can be replayed after the bug is fixed.</li>
 *   <li>{@code context} — optional key-value metadata added by the handler
 *       (e.g. GTIN, alertId, processor name).</li>
 * </ul>
 *
 * <p>The envelope itself is serialised as JSON so it remains human-readable
 * in the DLQ topic regardless of what broke in the Avro path.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class DlqEnvelope {

    public enum ErrorType {
        DESERIALIZATION,   // Avro / wire-format error
        VALIDATION,        // Business rule / domain constraint violation
        PROCESSING,        // Runtime exception in a processor
        PRODUCTION         // Kafka producer failure on output
    }

    // ── Source context ────────────────────────────────────────────────────────
    private String  originalTopic;
    private int     partition;
    private long    offset;

    // ── Error detail ─────────────────────────────────────────────────────────
    private ErrorType errorType;
    private String    errorMessage;
    private String    exceptionClass;
    private String    stackTrace;
    private int       retryCount;

    // ── Original payload (base-64 encoded raw bytes) ─────────────────────────
    private String originalKeyBase64;
    private String originalValueBase64;

    // ── Metadata ─────────────────────────────────────────────────────────────
    private String              failedAt;   // ISO-8601 UTC timestamp
    private String              processorName;
    private Map<String, String> context;    // Additional key-value metadata

    // ── Constructors ─────────────────────────────────────────────────────────

    public DlqEnvelope() {}

    private DlqEnvelope(Builder builder) {
        this.originalTopic        = builder.originalTopic;
        this.partition            = builder.partition;
        this.offset               = builder.offset;
        this.errorType            = builder.errorType;
        this.errorMessage         = builder.errorMessage;
        this.exceptionClass       = builder.exceptionClass;
        this.stackTrace           = builder.stackTrace;
        this.retryCount           = builder.retryCount;
        this.originalKeyBase64    = builder.originalKeyBase64;
        this.originalValueBase64  = builder.originalValueBase64;
        this.failedAt             = Instant.now().toString();
        this.processorName        = builder.processorName;
        this.context              = builder.context;
    }

    // ── Builder ──────────────────────────────────────────────────────────────

    public static Builder builder() { return new Builder(); }

    public static class Builder {
        private String              originalTopic;
        private int                 partition;
        private long                offset;
        private ErrorType           errorType    = ErrorType.PROCESSING;
        private String              errorMessage;
        private String              exceptionClass;
        private String              stackTrace;
        private int                 retryCount   = 0;
        private String              originalKeyBase64;
        private String              originalValueBase64;
        private String              processorName;
        private Map<String, String> context;

        public Builder originalTopic(String t)        { this.originalTopic = t; return this; }
        public Builder partition(int p)               { this.partition = p; return this; }
        public Builder offset(long o)                 { this.offset = o; return this; }
        public Builder errorType(ErrorType et)        { this.errorType = et; return this; }
        public Builder errorMessage(String m)         { this.errorMessage = m; return this; }
        public Builder exceptionClass(String c)       { this.exceptionClass = c; return this; }
        public Builder stackTrace(String st)          { this.stackTrace = st; return this; }
        public Builder retryCount(int rc)             { this.retryCount = rc; return this; }
        public Builder originalKeyBase64(String k)    { this.originalKeyBase64 = k; return this; }
        public Builder originalValueBase64(String v)  { this.originalValueBase64 = v; return this; }
        public Builder processorName(String pn)       { this.processorName = pn; return this; }
        public Builder context(Map<String, String> c) { this.context = c; return this; }

        public Builder fromException(Throwable ex) {
            this.errorMessage   = ex.getMessage();
            this.exceptionClass = ex.getClass().getName();
            this.stackTrace     = stackTraceToString(ex);
            return this;
        }

        public DlqEnvelope build() { return new DlqEnvelope(this); }

        private static String stackTraceToString(Throwable ex) {
            java.io.StringWriter sw = new java.io.StringWriter();
            ex.printStackTrace(new java.io.PrintWriter(sw));
            return sw.toString();
        }
    }

    // ── Getters / Setters (required for Jackson) ─────────────────────────────

    public String getOriginalTopic()       { return originalTopic; }
    public void setOriginalTopic(String v) { this.originalTopic = v; }

    public int getPartition()              { return partition; }
    public void setPartition(int v)        { this.partition = v; }

    public long getOffset()                { return offset; }
    public void setOffset(long v)          { this.offset = v; }

    public ErrorType getErrorType()        { return errorType; }
    public void setErrorType(ErrorType v)  { this.errorType = v; }

    public String getErrorMessage()        { return errorMessage; }
    public void setErrorMessage(String v)  { this.errorMessage = v; }

    public String getExceptionClass()      { return exceptionClass; }
    public void setExceptionClass(String v){ this.exceptionClass = v; }

    public String getStackTrace()          { return stackTrace; }
    public void setStackTrace(String v)    { this.stackTrace = v; }

    public int getRetryCount()             { return retryCount; }
    public void setRetryCount(int v)       { this.retryCount = v; }

    public String getOriginalKeyBase64()   { return originalKeyBase64; }
    public void setOriginalKeyBase64(String v) { this.originalKeyBase64 = v; }

    public String getOriginalValueBase64() { return originalValueBase64; }
    public void setOriginalValueBase64(String v) { this.originalValueBase64 = v; }

    public String getFailedAt()            { return failedAt; }
    public void setFailedAt(String v)      { this.failedAt = v; }

    public String getProcessorName()       { return processorName; }
    public void setProcessorName(String v) { this.processorName = v; }

    public Map<String, String> getContext()              { return context; }
    public void setContext(Map<String, String> context)  { this.context = context; }

    @Override
    public String toString() {
        return "DlqEnvelope{topic=" + originalTopic
                + ", partition=" + partition
                + ", offset=" + offset
                + ", errorType=" + errorType
                + ", exception=" + exceptionClass
                + ", failedAt=" + failedAt + '}';
    }
}
