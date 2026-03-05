package com.hci.pharma.streams.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Central configuration loader for the HCI Streams application.
 *
 * <p>Loads {@code application.properties} from the classpath, then overlays any
 * matching environment variables (e.g. {@code KAFKA_BOOTSTRAP_SERVERS} overrides
 * {@code kafka.bootstrap.servers}) so that the same artefact can run locally,
 * in Docker, and on Confluent Cloud without recompilation.
 */
public final class AppConfig {

    private static final Logger log = LoggerFactory.getLogger(AppConfig.class);

    private final Properties props;

    public AppConfig() {
        props = new Properties();
        try (InputStream in = getClass().getClassLoader()
                .getResourceAsStream("application.properties")) {
            if (in == null) {
                throw new IllegalStateException("application.properties not found on classpath");
            }
            props.load(in);
            log.info("Loaded application.properties from classpath");
        } catch (IOException e) {
            throw new IllegalStateException("Failed to load application.properties", e);
        }
        applyEnvironmentOverrides();
    }

    /**
     * Environment variable override: converts the property key to UPPER_SNAKE_CASE
     * (dots → underscores) and checks if a matching env var is set.
     * Example: {@code kafka.bootstrap.servers} → {@code KAFKA_BOOTSTRAP_SERVERS}
     */
    private void applyEnvironmentOverrides() {
        for (String key : props.stringPropertyNames()) {
            String envKey = key.toUpperCase().replace('.', '_');
            String envVal = System.getenv(envKey);
            if (envVal != null && !envVal.isBlank()) {
                log.info("Config override from environment: {}={}", key, envKey);
                props.setProperty(key, envVal);
            }
        }
    }

    public String get(String key) {
        String value = props.getProperty(key);
        if (value == null) {
            throw new IllegalArgumentException("Missing required configuration key: " + key);
        }
        return value;
    }

    public String get(String key, String defaultValue) {
        return props.getProperty(key, defaultValue);
    }

    public int getInt(String key) {
        return Integer.parseInt(get(key));
    }

    public double getDouble(String key) {
        return Double.parseDouble(get(key));
    }

    public boolean getBoolean(String key, boolean defaultValue) {
        String val = props.getProperty(key);
        return val != null ? Boolean.parseBoolean(val) : defaultValue;
    }

    /** Expose all properties (e.g. to pass directly to Kafka Streams builder). */
    public Properties toProperties() {
        Properties copy = new Properties();
        copy.putAll(props);
        return copy;
    }

    // ── Convenience accessors for the most-used config keys ──────────────────

    public String bootstrapServers()     { return get("kafka.bootstrap.servers"); }
    public String schemaRegistryUrl()    { return get("schema.registry.url"); }
    public String applicationId()        { return get("streams.application.id"); }
    public String productsTopic()        { return get("topic.medicinal.products"); }
    public String priceUpdatesTopic()    { return get("topic.price.updates"); }
    public String drugAlertsTopic()      { return get("topic.drug.alerts"); }
    public String priceAnomaliesTopic()  { return get("topic.price.anomalies"); }
    public String alertEnrichedTopic()   { return get("topic.alert.enriched"); }
    public String narcoticsAlertsTopic() { return get("topic.narcotics.alerts"); }
    public String alertSummaryTopic()    { return get("topic.product.alert.summary"); }
    public String priceSurgeTopic()      { return get("topic.price.surge.notifications"); }
    public String dlqPriceTopic()        { return get("topic.dlq.price.updates"); }
    public String dlqAlertTopic()        { return get("topic.dlq.drug.alerts"); }
    public String dlqProductTopic()      { return get("topic.dlq.medicinal.products"); }
    public String productCatalogStore()  { return get("store.product.catalog"); }
    public String alertCountsStore()     { return get("store.alert.counts"); }
    public String priceHistoryStore()    { return get("store.price.history"); }
    public double priceSurgeThreshold()  { return getDouble("price.surge.threshold.percent"); }
    public int    alertWindowMinutes()   { return getInt("alert.window.duration.minutes"); }
    public int    dlqMaxRetries()        { return getInt("dlq.max.retries"); }
}
