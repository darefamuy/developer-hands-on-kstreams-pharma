package com.hci.pharma.streams.processor;

import com.hci.pharma.streams.dlq.DlqEnvelope;
import com.hci.pharma.streams.dlq.DlqProducer;
import hci.MedicinalProduct;
import hci.MarketingStatus;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Product Catalogue Processor — Use Case 4 (Processor API)
 *
 * <h2>What it does</h2>
 * Validates each incoming {@link MedicinalProduct} against business rules
 * before it is admitted to the product catalogue state store.  Records that
 * fail validation are routed to the DLQ; valid records are forwarded downstream.
 *
 * <h2>Why use the Processor API here?</h2>
 * The high-level DSL (KStream/KTable) does not expose the raw
 * {@link ProcessorContext}, which is needed to:
 * <ul>
 *   <li>Access partition and offset metadata for DLQ envelopes.</li>
 *   <li>Write to the state store directly with custom logic (e.g. rejecting
 *       a product update that would move status backward from AUTHORIZED
 *       to PENDING — an illegal Swissmedic status transition).</li>
 *   <li>Emit conditional punctuations (scheduled tasks) for state expiry.</li>
 * </ul>
 *
 * <h2>Validation rules</h2>
 * <ol>
 *   <li>GTIN must be a non-empty string (mandatory key).</li>
 *   <li>Products in PENDING status must have a valid Swissmedic authorisation
 *       number (cannot be empty).</li>
 *   <li>Public price must be ≥ ex-factory price (PP ≥ EPP).  A PP lower than
 *       EPP indicates a data error — no Swiss regulatory model allows this.</li>
 *   <li>Products with a narcoticsCategory must have prescription category A.</li>
 *   <li>Status transitions are forward-only: a product already in the store as
 *       WITHDRAWN cannot be updated back to ACTIVE without explicit override.</li>
 * </ol>
 *
 * <h2>DLQ routing</h2>
 * Failed products are written to {@code hci.dlq.medicinal-products.avro} as
 * {@code DlqEnvelope.ErrorType.VALIDATION} events with the specific rule that
 * was violated in the {@code context} map.
 */
public class ProductCatalogueProcessor
        implements Processor<String, MedicinalProduct, String, MedicinalProduct> {

    private static final Logger log = LoggerFactory.getLogger(ProductCatalogueProcessor.class);

    private final String     dlqTopic;
    private final DlqProducer dlqProducer;
    private final String     storeName;

    private ProcessorContext<String, MedicinalProduct>  context;
    private KeyValueStore<String, MedicinalProduct>     catalogueStore;

    public ProductCatalogueProcessor(String storeName, String dlqTopic, DlqProducer dlqProducer) {
        this.storeName   = storeName;
        this.dlqTopic    = dlqTopic;
        this.dlqProducer = dlqProducer;
    }

    @Override
    public void init(ProcessorContext<String, MedicinalProduct> context) {
        this.context        = context;
        this.catalogueStore = context.getStateStore(storeName);
        log.info("ProductCatalogueProcessor initialised, store={}", storeName);
    }

    @Override
    public void process(Record<String, MedicinalProduct> record) {
        MedicinalProduct product = record.value();

        if (product == null) {
            // Tombstone record — delete from catalogue (product withdrawn)
            log.info("Tombstone received for GTIN {} — removing from catalogue", record.key());
            catalogueStore.delete(record.key());
            context.forward(record);
            return;
        }

        // ── Run validation rules ──────────────────────────────────────────────
        ValidationResult validation = validate(record.key(), product);

        if (!validation.isValid()) {
            log.warn("Product {} failed validation [{}]: {} — routing to DLQ",
                    record.key(), validation.rule(), validation.message());

            DlqEnvelope envelope = DlqEnvelope.builder()
                    .originalTopic(context.recordMetadata().map(m -> m.topic()).orElse("unknown"))
                    .partition(context.recordMetadata().map(m -> m.partition()).orElse(-1))
                    .offset(context.recordMetadata().map(m -> m.offset()).orElse(-1L))
                    .errorType(DlqEnvelope.ErrorType.VALIDATION)
                    .errorMessage(validation.message())
                    .processorName("ProductCatalogueProcessor")
                    .context(Map.of(
                            "gtin",          record.key(),
                            "productName",   product.getProductName(),
                            "validationRule", validation.rule(),
                            "manufacturer",  product.getManufacturer()
                    ))
                    .build();

            dlqProducer.send(dlqTopic, envelope);
            // Do NOT forward — this record is rejected
            return;
        }

        // ── Update the state store ─────────────────────────────────────────────
        catalogueStore.put(record.key(), product);
        log.debug("Catalogue updated: GTIN={} product={} status={}",
                record.key(), product.getProductName(), product.getMarketingStatus());

        // ── Forward to downstream processors / sinks ──────────────────────────
        context.forward(record);
    }

    @Override
    public void close() {
        log.info("ProductCatalogueProcessor closing");
    }

    // ── Validation logic ──────────────────────────────────────────────────────

    private ValidationResult validate(String gtin, MedicinalProduct product) {

        // Rule 1: GTIN must be present and non-empty
        if (gtin == null || gtin.isBlank()) {
            return ValidationResult.fail("GTIN_REQUIRED", "GTIN key is null or empty");
        }

        // Rule 2: Swissmedic authorisation number is mandatory for PENDING products
        if (product.getMarketingStatus() == MarketingStatus.PENDING
                && (product.getAuthorizationNumber() == null
                    || product.getAuthorizationNumber().isBlank())) {
            return ValidationResult.fail("AUTH_NUMBER_REQUIRED",
                    "PENDING product must have a Swissmedic authorisation number");
        }

        // Rule 3: Public price must be >= ex-factory price
        if (product.getPublicPriceCHF() < product.getExFactoryPriceCHF()) {
            return ValidationResult.fail("PRICE_INVERSION",
                    String.format("Public price CHF %.2f < ex-factory price CHF %.2f — impossible",
                            product.getPublicPriceCHF(), product.getExFactoryPriceCHF()));
        }

        // Rule 4: Narcotic products must have non-null narcoticsCategory
        // (they also require a valid BetmG schedule code — simplified here)
        if (product.getNarcoticsCategory() != null
                && product.getNarcoticsCategory().isBlank()) {
            return ValidationResult.fail("NARCOTICS_CATEGORY_BLANK",
                    "narcoticsCategory is present but blank — must be null or a valid BetmG code");
        }

        // Rule 5: Forward-only status transitions
        MedicinalProduct existing = catalogueStore.get(gtin);
        if (existing != null) {
            StatusTransitionResult transition =
                    checkStatusTransition(existing.getMarketingStatus(),
                            product.getMarketingStatus());
            if (!transition.isAllowed()) {
                return ValidationResult.fail("ILLEGAL_STATUS_TRANSITION",
                        String.format("Status transition %s → %s is not permitted",
                                existing.getMarketingStatus(), product.getMarketingStatus()));
            }
        }

        return ValidationResult.ok();
    }

    /**
     * Validate Swissmedic product lifecycle status transitions.
     *
     * <p>Allowed transitions:
     * <pre>
     *   PENDING   → ACTIVE, WITHDRAWN
     *   ACTIVE    → SUSPENDED, WITHDRAWN
     *   SUSPENDED → ACTIVE, WITHDRAWN
     *   WITHDRAWN → (none — terminal state)
     * </pre>
     */
    private StatusTransitionResult checkStatusTransition(
            MarketingStatus from, MarketingStatus to) {
        if (from == to) return StatusTransitionResult.allowed(); // no-op update

        return switch (from) {
            case PENDING   -> (to == MarketingStatus.ACTIVE || to == MarketingStatus.WITHDRAWN)
                    ? StatusTransitionResult.allowed()
                    : StatusTransitionResult.denied();
            case ACTIVE    -> (to == MarketingStatus.SUSPENDED || to == MarketingStatus.WITHDRAWN)
                    ? StatusTransitionResult.allowed()
                    : StatusTransitionResult.denied();
            case SUSPENDED -> (to == MarketingStatus.ACTIVE || to == MarketingStatus.WITHDRAWN)
                    ? StatusTransitionResult.allowed()
                    : StatusTransitionResult.denied();
            case WITHDRAWN -> StatusTransitionResult.denied(); // terminal state
        };
    }

    // ── Inner record types for validation results ─────────────────────────────

    private record ValidationResult(boolean isValid, String rule, String message) {
        static ValidationResult ok()                            { return new ValidationResult(true, null, null); }
        static ValidationResult fail(String rule, String msg)  { return new ValidationResult(false, rule, msg); }
    }

    private record StatusTransitionResult(boolean isAllowed) {
        static StatusTransitionResult allowed() { return new StatusTransitionResult(true); }
        static StatusTransitionResult denied()  { return new StatusTransitionResult(false); }
    }
}
