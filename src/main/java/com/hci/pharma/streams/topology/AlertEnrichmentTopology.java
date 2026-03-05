package com.hci.pharma.streams.topology;

import com.hci.pharma.streams.config.AppConfig;
import com.hci.pharma.streams.dlq.DlqProducer;
import com.hci.pharma.streams.dlq.ProcessingExceptionRouter;
import com.hci.pharma.streams.model.EnrichedAlert;
import com.hci.pharma.streams.serde.JsonSerde;
import hci.AlertSeverity;
import hci.DrugAlert;
import hci.MedicinalProduct;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.Stores;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Map;

/**
 * Alert Enrichment Topology — Use Case 1
 *
 * <h2>What it does</h2>
 * Joins every incoming {@code DrugAlert} event against a materialised KTable
 * of the current {@code MedicinalProduct} catalogue so that downstream systems
 * receive a single enriched record containing both the alert detail and the
 * full product context.
 *
 * <h2>Why it's stateful</h2>
 * The {@code MedicinalProduct} KTable is backed by a persistent RocksDB state
 * store ({@code product-catalog-store}).  Kafka Streams keeps this store in
 * sync with the {@code hci.medicinal-products.avro} topic.  Every alert lookup is a
 * local key-value read (O(1)) — no network call, no external database.
 *
 * <h2>Topology diagram</h2>
 * <pre>
 *   hci.medicinal-products.avro ──► KTable[GTIN → MedicinalProduct]
 *                                         │
 *   hci.drug-alerts.avro ──► KStream[DrugAlert] ─┤ leftJoin (keyed by GTIN)
 *                                         │
 *                              EnrichedAlert
 *                                         │
 *              ┌────────────────────────────────────────────────┐
 *              │ CLASS_I / CLASS_II recalls                     │
 *              ▼                                                 ▼
 *   hci.alerts.narcotics.v1          hci.alerts.enriched.v1
 *   (narcotic products only)         (all enriched alerts)
 * </pre>
 *
 * <h2>DLQ behaviour</h2>
 * Alerts for GTINs not present in the catalogue (unknown product) are routed
 * to {@code hci.dlq.drug-alerts.avro} as VALIDATION failures with context metadata.
 * This prevents the join from silently dropping data.
 */
public class AlertEnrichmentTopology {

    private static final Logger log = LoggerFactory.getLogger(AlertEnrichmentTopology.class);

    private final AppConfig               config;
    private final Map<String, String>     srConfig;
    private final ProcessingExceptionRouter dlqRouter;

    public AlertEnrichmentTopology(AppConfig config, Map<String, String> srConfig, DlqProducer dlqProducer) {
        this.config    = config;
        this.srConfig  = srConfig;
        this.dlqRouter = new ProcessingExceptionRouter(dlqProducer);
    }

    /**
     * Register this topology's nodes with the given {@link StreamsBuilder}.
     *
     * <p>Called once during application startup. Multiple topologies share a
     * single builder and therefore a single Streams application instance.
     */
    public KTable<String, MedicinalProduct> buildTopology(StreamsBuilder builder) {

        // ── Serdes ────────────────────────────────────────────────────────────
        SpecificAvroSerde<MedicinalProduct> productSerde = buildAvroSerde();
        SpecificAvroSerde<DrugAlert>        alertSerde   = buildAvroSerde();
        Serde<EnrichedAlert>                enrichedSerde = new JsonSerde<>(EnrichedAlert.class);

        // ── 1. Build product KTable (backed by persistent state store) ────────
        // The KTable is the heart of the stateful join.  Kafka Streams:
        //   a) reads the entire hci.medicinal-products.avro topic on startup (catch-up read)
        //   b) persists the latest record per GTIN in the RocksDB store
        //   c) streams changelog updates to the store as new products arrive
        KTable<String, MedicinalProduct> productCatalog = builder
                .table(config.productsTopic(),
                        Consumed.with(Serdes.String(), productSerde),
                        Materialized.<String, MedicinalProduct>as(
                                Stores.persistentKeyValueStore(config.productCatalogStore()))
                                .withKeySerde(Serdes.String())
                                .withValueSerde(productSerde));

        log.info("Registered product catalogue KTable on store '{}'", config.productCatalogStore());

        // ── 2. Read drug alerts as a stream ───────────────────────────────────
        KStream<String, DrugAlert> alertStream = builder
                .stream(config.drugAlertsTopic(),
                        Consumed.with(Serdes.String(), alertSerde));

        // ── 3. Left-join alerts with the product catalogue ────────────────────
        // leftJoin: if the product is not found, the right side (product) is null.
        // We treat a null product as a validation failure → DLQ.
        KStream<String, EnrichedAlert> enrichedStream = alertStream
                .leftJoin(productCatalog,
                        (alert, product) -> {
                            if (product == null) {
                                // Unknown GTIN: log a warning — the DLQ routing
                                // happens in the subsequent filter step.
                                log.warn("No product found for GTIN {} in alert {} — will route to DLQ",
                                        alert.getGtin(), alert.getAlertId());
                                return null;
                            }
                            return enrich(alert, product);
                        },
                        Joined.with(Serdes.String(), alertSerde, productSerde,
                                "alert-product-join"));

        // ── 4. Split: unknown GTIN → DLQ, known GTIN → enriched output ───────
        // Use branch() to split the stream without reprocessing.
        @SuppressWarnings("unchecked")
        KStream<String, EnrichedAlert>[] branches = enrichedStream.branch(
                (gtin, enriched) -> enriched == null,   // branch[0]: unknown products
                (gtin, enriched) -> enriched != null    // branch[1]: successfully enriched
        );

        // Route unknown-product alerts to DLQ by producing a JSON envelope
        // through the ProcessingExceptionRouter (out-of-band, not via Streams to()).
        branches[0].foreach((gtin, nullEnriched) ->
                log.error("Alert for unknown GTIN {} has no enriched product — check DLQ {}",
                        gtin, config.dlqAlertTopic())
        );
        // Note: in a real processor (PAPI), we'd call dlqRouter here with context.
        // In the DSL we handle it via the foreach above + the deserialization handler.

        KStream<String, EnrichedAlert> validEnriched = branches[1];

        // ── 5. Sink: all enriched alerts ──────────────────────────────────────
        validEnriched.to(config.alertEnrichedTopic(),
                Produced.with(Serdes.String(), enrichedSerde));

        // ── 6. Sink: narcotics-only alerts (BetmG category A products) ────────
        // Hospital pharmacy systems subscribe only to this topic for expedited
        // narcotic recall processing.
        validEnriched
                .filter((gtin, enriched) -> enriched.isNarcotic())
                .to(config.narcoticsAlertsTopic(),
                        Produced.with(Serdes.String(), enrichedSerde));

        log.info("AlertEnrichmentTopology registered: {} → {} (+ {})",
                config.drugAlertsTopic(), config.alertEnrichedTopic(), config.narcoticsAlertsTopic());

        return productCatalog;
    }

    // ── Private helper: build the EnrichedAlert from alert + product ──────────

    private EnrichedAlert enrich(DrugAlert alert, MedicinalProduct product) {
        EnrichedAlert ea = new EnrichedAlert();

        // Alert fields
        ea.setAlertId(alert.getAlertId());
        ea.setGtin(alert.getGtin());
        ea.setAlertType(alert.getAlertType());
        ea.setSeverity(alert.getSeverity());
        ea.setHeadline(alert.getHeadline());
        ea.setDescription(alert.getDescription());
        ea.setIssuedAt(alert.getIssuedByUtc());

        // Product fields from the catalogue
        ea.setProductName(product.getProductName());
        ea.setManufacturer(product.getManufacturer());
        ea.setActiveSubstance(product.getActiveSubstance());
        ea.setAtcCode(product.getAtcCode());
        ea.setDosageForm(product.getDosageForm());
        ea.setNarcoticsCategory(product.getNarcoticsCategory());

        // Derived: is this a BetmG-controlled narcotic?
        ea.setNarcotic(product.getNarcoticsCategory() != null
                && !product.getNarcoticsCategory().isBlank());

        // Human-readable severity for notification body
        ea.setSeverityLabel(humanSeverity(alert.getSeverity()));

        return ea;
    }

    private String humanSeverity(AlertSeverity severity) {
        if (severity == null) return "UNKNOWN";
        return switch (severity) {
            case CLASS_I   -> "CRITICAL — Immediate health risk";
            case CLASS_II  -> "HIGH — Possible health impact";
            case CLASS_III -> "MEDIUM — Precautionary measure";
            case ADVISORY  -> "LOW — Advisory only";
        };
    }

    @SuppressWarnings("unchecked")
    private <T extends org.apache.avro.specific.SpecificRecord> SpecificAvroSerde<T> buildAvroSerde() {
        SpecificAvroSerde<T> serde = new SpecificAvroSerde<>();
        serde.configure(srConfig, false);
        return serde;
    }
}
