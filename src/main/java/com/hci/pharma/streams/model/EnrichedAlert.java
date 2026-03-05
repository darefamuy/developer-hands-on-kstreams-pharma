package com.hci.pharma.streams.model;

import hci.AlertSeverity;
import hci.AlertType;

import java.time.Instant;

/**
 * Enriched drug alert — a {@code DrugAlert} joined with its corresponding
 * {@code MedicinalProduct} from the product catalogue state store.
 *
 * <p>This is the output record of the {@code AlertEnrichmentTopology}.
 * It is serialised as JSON to {@code hci.alerts.enriched.v1} because:
 * <ul>
 *   <li>Downstream consumers (notification services, PharmaVista) need a
 *       self-contained record that does not require a Schema Registry lookup.</li>
 *   <li>Adding product fields to the DrugAlert Avro schema would couple the
 *       pharmacovigilance schema to the product catalogue schema — a bad
 *       schema design pattern.</li>
 * </ul>
 */
public class EnrichedAlert {

    // ── Alert fields ──────────────────────────────────────────────────────────
    private String      alertId;
    private String      gtin;
    private AlertType   alertType;
    private AlertSeverity severity;
    private String      headline;
    private String      description;
    private Instant     issuedAt;

    // ── Enriched product fields ───────────────────────────────────────────────
    private String productName;
    private String manufacturer;
    private String activeSubstance;
    private String atcCode;
    private String dosageForm;
    private String narcoticsCategory;

    // ── Derived fields ────────────────────────────────────────────────────────
    /** True when the product is a narcotic (BetmG category A). */
    private boolean isNarcotic;

    /** Human-readable severity label for notification systems. */
    private String severityLabel;

    // ── Constructors ─────────────────────────────────────────────────────────

    public EnrichedAlert() {}

    // ── Getters / setters ────────────────────────────────────────────────────

    public String getAlertId()               { return alertId; }
    public void setAlertId(String v)         { this.alertId = v; }

    public String getGtin()                  { return gtin; }
    public void setGtin(String v)            { this.gtin = v; }

    public AlertType getAlertType()          { return alertType; }
    public void setAlertType(AlertType v)    { this.alertType = v; }

    public AlertSeverity getSeverity()             { return severity; }
    public void setSeverity(AlertSeverity v)       { this.severity = v; }

    public String getHeadline()              { return headline; }
    public void setHeadline(String v)        { this.headline = v; }

    public String getDescription()           { return description; }
    public void setDescription(String v)     { this.description = v; }

    public Instant getIssuedAt()             { return issuedAt; }
    public void setIssuedAt(Instant v)       { this.issuedAt = v; }

    public String getProductName()           { return productName; }
    public void setProductName(String v)     { this.productName = v; }

    public String getManufacturer()          { return manufacturer; }
    public void setManufacturer(String v)    { this.manufacturer = v; }

    public String getActiveSubstance()       { return activeSubstance; }
    public void setActiveSubstance(String v) { this.activeSubstance = v; }

    public String getAtcCode()               { return atcCode; }
    public void setAtcCode(String v)         { this.atcCode = v; }

    public String getDosageForm()            { return dosageForm; }
    public void setDosageForm(String v)      { this.dosageForm = v; }

    public String getNarcoticsCategory()     { return narcoticsCategory; }
    public void setNarcoticsCategory(String v) { this.narcoticsCategory = v; }

    public boolean isNarcotic()              { return isNarcotic; }
    public void setNarcotic(boolean v)       { this.isNarcotic = v; }

    public String getSeverityLabel()         { return severityLabel; }
    public void setSeverityLabel(String v)   { this.severityLabel = v; }

    @Override
    public String toString() {
        return "EnrichedAlert{alertId=" + alertId
                + ", gtin=" + gtin
                + ", type=" + alertType
                + ", severity=" + severity
                + ", product=" + productName + '}';
    }
}
