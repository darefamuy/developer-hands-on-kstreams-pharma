package com.hci.pharma.streams.topology;

import com.hci.pharma.streams.config.AppConfig;
import com.hci.pharma.streams.model.PriceSurgeNotification;
import com.hci.pharma.streams.serde.JsonSerde;
import hci.MedicinalProduct;
import hci.PriceUpdate;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Map;

/**
 * Price Surge Detection Topology — Use Case 2
 *
 * <h2>What it does</h2>
 * Detects when a product's public price increases by more than the configured
 * threshold (default 15%) in a single BAG-cycle price update event, and emits
 * a {@code PriceSurgeNotification} to a dedicated output topic.
 *
 * <h2>Why it's stateful</h2>
 * Price updates carry both {@code previousPublicPriceCHF} and
 * {@code newPublicPriceCHF}, so in theory we could compute the change inline
 * without state.  However, the incoming {@code previousPublicPriceCHF} is
 * populated by the <em>source system</em> — which can sometimes lag or contain
 * stale values.  This topology maintains its own verified state store of the
 * last <em>confirmed</em> public price per GTIN (derived from the product
 * catalogue KTable) to provide an independent, authoritative baseline for
 * surge detection.
 *
 * <h2>Topology diagram</h2>
 * <pre>
 *   hci.medicinal-products.avro ──► KTable[GTIN → lastConfirmedPrice (double)]
 *                                         │
 *   hci.price-updates.avro ──► KStream[PriceUpdate] ─┤ leftJoin (keyed by GTIN)
 *                                         │
 *                                  (basePrice, update)
 *                                         │
 *                              compute changePercent
 *                                         │
 *                           changePercent > threshold?
 *                                 YES │         NO │
 *                                     ▼             └─► (discarded)
 *                   hci.prices.surge-notifications.v1
 * </pre>
 *
 * <h2>Surge threshold</h2>
 * Configured via {@code price.surge.threshold.percent} in
 * {@code application.properties} (default 15.0).  This value is deliberately
 * conservative for the Swiss pharmaceutical market where BAG price reviews
 * typically result in small incremental changes.  A 15%+ single-event increase
 * is a strong signal of either a genuine extraordinary price increase or a
 * data error requiring investigation.
 */
public class PriceSurgeDetectionTopology {

    private static final Logger log = LoggerFactory.getLogger(PriceSurgeDetectionTopology.class);

    /**
     * State store name for the confirmed last-known public price per GTIN.
     * This store is populated from the product catalogue (not from price events)
     * to provide an independent baseline.
     */
    private static final String CONFIRMED_PRICE_STORE = "confirmed-public-price-store";

    private final AppConfig           config;
    private final Map<String, String> srConfig;

    public PriceSurgeDetectionTopology(AppConfig config, Map<String, String> srConfig) {
        this.config   = config;
        this.srConfig = srConfig;
    }

    public void buildTopology(StreamsBuilder builder, KTable<String, MedicinalProduct> productCatalog) {

        // ── Serdes ────────────────────────────────────────────────────────────
        SpecificAvroSerde<MedicinalProduct>    productSerde       = buildAvroSerde();
        SpecificAvroSerde<PriceUpdate>         priceUpdateSerde   = buildAvroSerde();
        Serde<PriceSurgeNotification>          surgeSerde         = new JsonSerde<>(PriceSurgeNotification.class);

        // ── 1. Materialise the last confirmed public price per GTIN ───────────
        // We extract only the publicPriceCHF from each MedicinalProduct record
        // and store it as a KTable<GTIN, Double>.
        KTable<String, Double> confirmedPriceTable = productCatalog
                // Map product → last known public price (Double)
                .mapValues(product -> product != null ? product.getPublicPriceCHF() : null,
                        Materialized.<String, Double, KeyValueStore<org.apache.kafka.common.utils.Bytes, byte[]>>as(
                                CONFIRMED_PRICE_STORE)
                                .withKeySerde(Serdes.String())
                                .withValueSerde(Serdes.Double()));

        log.info("Registered confirmed price state store '{}'", CONFIRMED_PRICE_STORE);

        // ── 2. Stream price update events ────────────────────────────────────
        KStream<String, PriceUpdate> priceStream = builder
                .stream(config.priceUpdatesTopic(),
                        Consumed.with(Serdes.String(), priceUpdateSerde));

        // ── 3. Enrich price updates with the confirmed baseline price ─────────
        // leftJoin: if the product is not in the catalogue, confirmedPrice is null.
        // Fall back to the value embedded in the event itself in that case.
        KStream<String, PriceSurgeNotification> surgeStream = priceStream
                .leftJoin(confirmedPriceTable,
                        (update, confirmedPrice) -> detectSurge(update, confirmedPrice),
                        Joined.with(Serdes.String(), priceUpdateSerde, Serdes.Double()))
                // Filter: only forward events that exceed the surge threshold
                .filter((gtin, notification) -> notification != null);

        // ── 4. Sink: emit surge notifications ─────────────────────────────────
        surgeStream.to(config.priceSurgeTopic(),
                Produced.with(Serdes.String(), surgeSerde));

        log.info("PriceSurgeDetectionTopology registered: {} → {} (threshold {}%)",
                config.priceUpdatesTopic(), config.priceSurgeTopic(),
                config.priceSurgeThreshold());
    }

    // ── Surge detection logic ─────────────────────────────────────────────────

    /**
     * Compare the new public price against the confirmed baseline.
     *
     * @param update         incoming price update event
     * @param confirmedPrice last confirmed public price from the catalogue state store
     *                       (may be null if the GTIN is not yet in the catalogue)
     * @return a {@link PriceSurgeNotification} if the change exceeds the threshold,
     *         or {@code null} if the change is within normal bounds
     */
    private PriceSurgeNotification detectSurge(PriceUpdate update, Double confirmedPrice) {
        // Use the confirmed store baseline; fall back to the event's own previous price
        double baseline = (confirmedPrice != null && confirmedPrice > 0.0)
                ? confirmedPrice
                : update.getPreviousPublicPriceCHF();

        if (baseline <= 0.0) {
            // Can't compute a meaningful percentage from zero baseline
            log.debug("Skipping surge check for GTIN {} — baseline price is zero", update.getGtin());
            return null;
        }

        double newPrice     = update.getNewPublicPriceCHF();
        double changeAbs    = newPrice - baseline;
        double changePct    = (changeAbs / baseline) * 100.0;

        if (changePct <= config.priceSurgeThreshold()) {
            // Normal price movement — no action needed
            return null;
        }

        // Surge detected!
        log.warn("Price SURGE detected for GTIN {}: CHF {:.2f} → {:.2f} ({:+.1f}%)",
                update.getGtin(), baseline, newPrice, changePct);

        PriceSurgeNotification notification = new PriceSurgeNotification();
        notification.setGtin(update.getGtin());
        notification.setPreviousPublicPriceCHF(baseline);
        notification.setNewPublicPriceCHF(newPrice);
        notification.setChangePercent(Math.round(changePct * 100.0) / 100.0);
        notification.setChangeReason(update.getChangeReason());
        notification.setEffectiveDate(update.getEffectiveDateUtc());
        notification.setDetectedAt(Instant.now());
        // productName will be null here (no product join in this topology — kept simple).
        // A more complete implementation would join with the product KTable for the name.

        return notification;
    }

    @SuppressWarnings("unchecked")
    private <T extends org.apache.avro.specific.SpecificRecord> SpecificAvroSerde<T> buildAvroSerde() {
        SpecificAvroSerde<T> serde = new SpecificAvroSerde<>();
        serde.configure(srConfig, false);
        return serde;
    }
}
