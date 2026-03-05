package com.hci.pharma.streams.model;

import java.time.Instant;

/**
 * Notification record produced when a price update exceeds the surge threshold.
 *
 * <p>The HCI price surge detector watches the {@code hci.price-updates.avro} topic and
 * emits a {@code PriceSurgeNotification} to {@code hci.prices.surge-notifications.v1}
 * whenever a product's public price increases by more than the configured
 * threshold (default 15%) in a single {@code PriceUpdate} event.
 *
 * <p>Downstream consumers of this topic include:
 * <ul>
 *   <li>BAG compliance monitoring (large price jumps may trigger a review)</li>
 *   <li>Hospital procurement systems (budget impact alerts)</li>
 *   <li>Wholesale ERP systems (inventory revaluation)</li>
 * </ul>
 */
public class PriceSurgeNotification {

    private String  gtin;
    private String  productName;
    private double  previousPublicPriceCHF;
    private double  newPublicPriceCHF;
    private double  changePercent;
    private String  changeReason;
    private Instant effectiveDate;
    private Instant detectedAt;

    public PriceSurgeNotification() {}

    // ── Getters / setters ────────────────────────────────────────────────────

    public String getGtin()                     { return gtin; }
    public void setGtin(String v)               { this.gtin = v; }

    public String getProductName()              { return productName; }
    public void setProductName(String v)        { this.productName = v; }

    public double getPreviousPublicPriceCHF()   { return previousPublicPriceCHF; }
    public void setPreviousPublicPriceCHF(double v) { this.previousPublicPriceCHF = v; }

    public double getNewPublicPriceCHF()        { return newPublicPriceCHF; }
    public void setNewPublicPriceCHF(double v)  { this.newPublicPriceCHF = v; }

    public double getChangePercent()            { return changePercent; }
    public void setChangePercent(double v)      { this.changePercent = v; }

    public String getChangeReason()             { return changeReason; }
    public void setChangeReason(String v)       { this.changeReason = v; }

    public Instant getEffectiveDate()           { return effectiveDate; }
    public void setEffectiveDate(Instant v)     { this.effectiveDate = v; }

    public Instant getDetectedAt()              { return detectedAt; }
    public void setDetectedAt(Instant v)        { this.detectedAt = v; }

    @Override
    public String toString() {
        return String.format("PriceSurgeNotification{gtin=%s, %+.1f%%, CHF %.2f→%.2f}",
                gtin, changePercent, previousPublicPriceCHF, newPublicPriceCHF);
    }
}
