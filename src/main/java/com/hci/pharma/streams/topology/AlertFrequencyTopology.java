package com.hci.pharma.streams.topology;

import com.hci.pharma.streams.config.AppConfig;
import hci.DrugAlert;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Map;

/**
 * Alert Frequency Topology — Use Case 3
 *
 * <h2>What it does</h2>
 * Counts the number of drug alerts per GTIN within a sliding time window
 * (default 60 minutes) and emits a rolling count to the
 * {@code hci.products.alert-summary.v1} topic.
 *
 * <h2>Why this matters</h2>
 * A product receiving multiple alerts in a short window is a strong signal of
 * an active pharmacovigilance incident (e.g. a quality defect affecting multiple
 * batches, or a cascade of shortage notifications from multiple distributors).
 * Downstream systems use this count to:
 * <ul>
 *   <li>Escalate to the HCI pharmacovigilance team for manual review</li>
 *   <li>Trigger an automatic hold on new purchase orders in the ERP</li>
 *   <li>Notify hospital pharmacy heads via the compendium.ch notification service</li>
 * </ul>
 *
 * <h2>Why it's stateful</h2>
 * This topology uses a <strong>tumbling time window</strong> backed by an
 * in-memory (with persistent changelog) windowed state store.  Each window
 * segment holds the running count of alerts per GTIN within the window period.
 *
 * <h2>Topology diagram</h2>
 * <pre>
 *   hci.drug-alerts.avro
 *       │
 *       ▼
 *   KStream[DrugAlert]
 *       │
 *       │ groupByKey (already keyed by GTIN)
 *       ▼
 *   KGroupedStream
 *       │
 *       │ windowedBy(TumblingWindow, 60 min)
 *       ▼
 *   TimeWindowedKStream
 *       │
 *       │ count()
 *       ▼
 *   KTable<Windowed<GTIN>, Long>
 *       │
 *       │ suppress(untilWindowCloses)  ← emit only on window close, not on every event
 *       ▼
 *   KStream<Windowed<GTIN>, Long>
 *       │
 *       │ filter(count >= 2)  ← only report when 2+ alerts in window
 *       ▼
 *   hci.products.alert-summary.v1
 * </pre>
 *
 * <h2>Windowing strategy</h2>
 * We use {@code TumblingWindows} (non-overlapping fixed windows) rather than
 * {@code SlidingWindows} to keep the state store size bounded and the output
 * predictable.  Each window emits exactly once on close.
 *
 * <p>The window duration is configurable via {@code alert.window.duration.minutes}
 * in {@code application.properties}.
 *
 * <h2>Output record format</h2>
 * Key: {@code "GTIN@windowStart-windowEnd"} (string representation of the windowed key)
 * Value: {@code Long} — the count of alerts in the window
 */
public class AlertFrequencyTopology {

    private static final Logger log = LoggerFactory.getLogger(AlertFrequencyTopology.class);

    /**
     * Minimum alert count within a window before emitting to the summary topic.
     * A count of 1 is normal; 2+ in the same window warrants attention.
     */
    private static final long MIN_ALERT_COUNT_THRESHOLD = 2L;

    private final AppConfig           config;
    private final Map<String, String> srConfig;

    public AlertFrequencyTopology(AppConfig config, Map<String, String> srConfig) {
        this.config   = config;
        this.srConfig = srConfig;
    }

    public void buildTopology(StreamsBuilder builder) {

        // ── Serdes ────────────────────────────────────────────────────────────
        SpecificAvroSerde<DrugAlert> alertSerde = buildAvroSerde();

        Duration windowSize = Duration.ofMinutes(config.alertWindowMinutes());

        // ── Grace period ──────────────────────────────────────────────────────
        // Allow events up to 10 minutes late before closing the window.
        // Late drug alerts can arrive due to Swissmedic publication delays or
        // network partitions in upstream ingestion pipelines.
        Duration gracePeriod = Duration.ofMinutes(10);

        log.info("Alert frequency window: {} min, grace: {} min",
                config.alertWindowMinutes(), gracePeriod.toMinutes());

        // ── 1. Read alerts ────────────────────────────────────────────────────
        KStream<String, DrugAlert> alertStream = builder
                .stream(config.drugAlertsTopic(),
                        Consumed.with(Serdes.String(), alertSerde)
                                // Use event-time timestamps from issuedByUtc field,
                                // not Kafka ingestion time.
                                .withTimestampExtractor(new AlertTimestampExtractor()));

        // ── 2. Group by GTIN, window, and count ───────────────────────────────
        KTable<Windowed<String>, Long> alertCounts = alertStream
                // Already keyed by GTIN — no re-key needed
                .groupByKey(Grouped.with(Serdes.String(), alertSerde))
                // Tumbling window: non-overlapping, fixed-size
                .windowedBy(TimeWindows
                        .ofSizeAndGrace(windowSize, gracePeriod))
                // Count alerts per GTIN per window
                .count(Materialized.as(config.alertCountsStore()));

        // ── 3. Suppress: emit only when the window closes ────────────────────
        // Without suppress(), Kafka Streams emits an updated count on EVERY
        // incoming alert (eager updates).  For alert frequency monitoring,
        // we prefer a clean single emission per window close — this reduces
        // downstream noise and makes the count authoritative.
        KStream<Windowed<String>, Long> windowedCounts = alertCounts
                .suppress(Suppressed.untilWindowCloses(
                        Suppressed.BufferConfig.unbounded()))
                .toStream();

        // ── 4. Filter: only forward if threshold exceeded ─────────────────────
        windowedCounts
                .filter((windowedGtin, count) -> count >= MIN_ALERT_COUNT_THRESHOLD)
                // Re-key: include window boundaries in the key for traceability
                .map((windowedGtin, count) -> {
                    String newKey = windowedGtin.key()
                            + "@" + windowedGtin.window().startTime()
                            + "/" + windowedGtin.window().endTime();
                    log.info("Alert frequency threshold exceeded: GTIN={} count={} window=[{},{}]",
                            windowedGtin.key(), count,
                            windowedGtin.window().startTime(),
                            windowedGtin.window().endTime());
                    return KeyValue.pair(newKey, count);
                })
                .to(config.alertSummaryTopic(),
                        Produced.with(Serdes.String(), Serdes.Long()));

        log.info("AlertFrequencyTopology registered: {} → {} (window={}min)",
                config.drugAlertsTopic(), config.alertSummaryTopic(),
                config.alertWindowMinutes());
    }

    // ── Custom timestamp extractor ────────────────────────────────────────────

    /**
     * Extracts event-time from the {@code issuedByUtc} field of a {@link DrugAlert}.
     *
     * <p>Using event-time rather than Kafka ingestion time is important here
     * because Swissmedic alerts are sometimes published in batches.  Ingestion
     * time would cause all alerts in the batch to fall into the same window even
     * if they were issued across multiple hours.
     */
    public static class AlertTimestampExtractor
            implements org.apache.kafka.streams.processor.TimestampExtractor {

        @Override
        public long extract(
                org.apache.kafka.clients.consumer.ConsumerRecord<Object, Object> record,
                long partitionTime) {
            if (record.value() instanceof DrugAlert alert) {
                return alert.getIssuedByUtc().toEpochMilli();
            }
            // Fall back to Kafka ingestion time if the record is not a DrugAlert
            return partitionTime;
        }
    }

    @SuppressWarnings("unchecked")
    private <T extends org.apache.avro.specific.SpecificRecord> SpecificAvroSerde<T> buildAvroSerde() {
        SpecificAvroSerde<T> serde = new SpecificAvroSerde<>();
        serde.configure(srConfig, false);
        return serde;
    }
}
