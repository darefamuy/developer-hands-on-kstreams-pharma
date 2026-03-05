package com.hci.pharma.streams.topology;

import com.hci.pharma.streams.config.AppConfig;
import com.hci.pharma.streams.dlq.DlqProducer;
import com.hci.pharma.streams.model.EnrichedAlert;
import com.hci.pharma.streams.serde.JsonSerde;
import hci.*;
import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.junit.jupiter.api.*;
import org.mockito.Mockito;

import java.time.Instant;
import java.util.Map;
import java.util.Properties;

import static org.assertj.core.api.Assertions.*;

/**
 * Unit tests for {@link AlertEnrichmentTopology} using Kafka Streams
 * {@link TopologyTestDriver} — no running Kafka broker required.
 *
 * <p>The test driver replays events synchronously through the topology and
 * makes the output topics available as {@link TestOutputTopic}s.
 */
class AlertEnrichmentTopologyTest {

    private static final String SCHEMA_REGISTRY_SCOPE = "alert-enrichment-test";
    private static final String MOCK_SR_URL = "mock://" + SCHEMA_REGISTRY_SCOPE;

    private TopologyTestDriver driver;

    private TestInputTopic<String, MedicinalProduct> productInput;
    private TestInputTopic<String, DrugAlert>        alertInput;
    private TestOutputTopic<String, EnrichedAlert>   enrichedOutput;
    private TestOutputTopic<String, EnrichedAlert>   narcoticsOutput;

    private AppConfig    config;
    private DlqProducer  mockDlqProducer;

    @BeforeEach
    void setUp() {
        // Use mock Schema Registry — no network calls
        Map<String, String> srConfig = Map.of(
                AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, MOCK_SR_URL,
                AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS, "true"
        );

        config          = new AppConfig();
        mockDlqProducer = Mockito.mock(DlqProducer.class);

        // Build topology under test
        StreamsBuilder builder  = new StreamsBuilder();
        new AlertEnrichmentTopology(config, srConfig, mockDlqProducer)
                .buildTopology(builder);
        Topology topology = builder.build();

        // Test driver properties
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG,       "alert-enrichment-test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,    "dummy:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,   Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
        props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, MOCK_SR_URL);

        driver = new TopologyTestDriver(topology, props);

        // Build Avro serdes backed by the mock registry
        SpecificAvroSerde<MedicinalProduct> productSerde = new SpecificAvroSerde<>();
        productSerde.configure(srConfig, false);

        SpecificAvroSerde<DrugAlert> alertSerde = new SpecificAvroSerde<>();
        alertSerde.configure(srConfig, false);

        // Wire up test topics
        productInput   = driver.createInputTopic(config.productsTopic(),
                Serdes.String().serializer(), productSerde.serializer());
        alertInput     = driver.createInputTopic(config.drugAlertsTopic(),
                Serdes.String().serializer(), alertSerde.serializer());
        enrichedOutput = driver.createOutputTopic(config.alertEnrichedTopic(),
                Serdes.String().deserializer(), new JsonSerde<>(EnrichedAlert.class).deserializer());
        narcoticsOutput = driver.createOutputTopic(config.narcoticsAlertsTopic(),
                Serdes.String().deserializer(), new JsonSerde<>(EnrichedAlert.class).deserializer());
    }

    @AfterEach
    void tearDown() {
        driver.close();
        MockSchemaRegistry.dropScope(SCHEMA_REGISTRY_SCOPE);
    }

    // ─────────────────────────────────────────────────────────────────────────

    @Test
    @DisplayName("Alert for known product is enriched and emitted to enriched topic")
    void alertForKnownProduct_isEnriched() {
        // Arrange — seed the product catalogue KTable
        MedicinalProduct product = buildProduct("7680123450001", "Amoxicillin 500mg", false);
        productInput.pipeInput("7680123450001", product);

        DrugAlert alert = buildAlert("alert-001", "7680123450001", AlertType.RECALL, AlertSeverity.CLASS_I);

        // Act
        alertInput.pipeInput("7680123450001", alert);

        // Assert
        assertThat(enrichedOutput.isEmpty()).isFalse();
        EnrichedAlert enriched = enrichedOutput.readValue();

        assertThat(enriched.getAlertId()).isEqualTo("alert-001");
        assertThat(enriched.getGtin()).isEqualTo("7680123450001");
        assertThat(enriched.getProductName()).isEqualTo("Amoxicillin 500mg");
        assertThat(enriched.getManufacturer()).isEqualTo("Test Pharma AG");
        assertThat(enriched.isNarcotic()).isFalse();
        assertThat(enriched.getSeverityLabel()).contains("CRITICAL");
    }

    @Test
    @DisplayName("Alert for narcotic product is emitted to both enriched and narcotics topics")
    void alertForNarcoticProduct_emittedToBothTopics() {
        // Arrange — narcotic product (BetmG category)
        MedicinalProduct narcoticProduct = buildProduct("7680999000001", "Morphin 10mg", true);
        productInput.pipeInput("7680999000001", narcoticProduct);

        DrugAlert alert = buildAlert("alert-narco-001", "7680999000001",
                AlertType.RECALL, AlertSeverity.CLASS_I);

        // Act
        alertInput.pipeInput("7680999000001", alert);

        // Assert — enriched topic
        assertThat(enrichedOutput.isEmpty()).isFalse();
        EnrichedAlert enrichedFromMain = enrichedOutput.readValue();
        assertThat(enrichedFromMain.isNarcotic()).isTrue();
        assertThat(enrichedFromMain.getNarcoticsCategory()).isEqualTo("BetmG-A");

        // Assert — narcotics topic also receives it
        assertThat(narcoticsOutput.isEmpty()).isFalse();
        EnrichedAlert enrichedFromNarcotics = narcoticsOutput.readValue();
        assertThat(enrichedFromNarcotics.getAlertId()).isEqualTo("alert-narco-001");
    }

    @Test
    @DisplayName("Non-narcotic product does NOT appear on narcotics topic")
    void alertForNonNarcoticProduct_notOnNarcoticsTopic() {
        MedicinalProduct product = buildProduct("7680123450002", "Paracetamol 500mg", false);
        productInput.pipeInput("7680123450002", product);

        alertInput.pipeInput("7680123450002",
                buildAlert("alert-002", "7680123450002", AlertType.SHORTAGE, AlertSeverity.ADVISORY));

        assertThat(enrichedOutput.isEmpty()).isFalse();
        assertThat(narcoticsOutput.isEmpty()).isTrue();
    }

    @Test
    @DisplayName("Alert for unknown GTIN produces null enriched record (no product in catalogue)")
    void alertForUnknownGtin_producesNullEnrichedRecord() {
        // Do NOT seed the catalogue — GTIN has no product
        alertInput.pipeInput("7680UNKNOWN001",
                buildAlert("alert-unknown", "7680UNKNOWN001", AlertType.RECALL, AlertSeverity.CLASS_II));

        // Null-join result should be filtered — no output on enriched topic
        assertThat(enrichedOutput.isEmpty()).isTrue();
        assertThat(narcoticsOutput.isEmpty()).isTrue();
    }

    @Test
    @DisplayName("Severity labels are correctly mapped for all alert classes")
    void severityLabels_areMappedCorrectly() {
        MedicinalProduct product = buildProduct("7680SEVER00001", "Test Product", false);
        productInput.pipeInput("7680SEVER00001", product);

        for (AlertSeverity severity : AlertSeverity.values()) {
            alertInput.pipeInput("7680SEVER00001",
                    buildAlert("sev-" + severity, "7680SEVER00001", AlertType.RECALL, severity));
        }

        int count = 0;
        while (!enrichedOutput.isEmpty()) {
            EnrichedAlert ea = enrichedOutput.readValue();
            assertThat(ea.getSeverityLabel()).isNotBlank();
            count++;
        }
        assertThat(count).isEqualTo(AlertSeverity.values().length);
    }

    // ── Test data builders ────────────────────────────────────────────────────

    private MedicinalProduct buildProduct(String gtin, String name, boolean isNarcotic) {
        MedicinalProduct p = new MedicinalProduct();
        p.setGtin(gtin);
        p.setAuthorizationNumber("68000");
        p.setProductName(name);
        p.setAtcCode("N02BE01");
        p.setDosageForm("Tablette");
        p.setManufacturer("Test Pharma AG");
        p.setActiveSubstance("Amoxicillin");
        p.setMarketingStatus(MarketingStatus.ACTIVE);
        p.setExFactoryPriceCHF(5.50);
        p.setPublicPriceCHF(12.80);
        p.setLastUpdatedUtc(Instant.now());
        p.setDataSource("swissmedic-test");
        p.setPackageSize("30 Tabletten");
        p.setNarcoticsCategory(isNarcotic ? "BetmG-A" : null);
        return p;
    }

    private DrugAlert buildAlert(String alertId, String gtin,
                                  AlertType type, AlertSeverity severity) {
        DrugAlert a = new DrugAlert();
        a.setAlertId(alertId);
        a.setGtin(gtin);
        a.setAlertType(type);
        a.setSeverity(severity);
        a.setHeadline("Test alert: " + type);
        a.setDescription("Test description for " + gtin);
        a.setAffectedLots(java.util.List.of());
        a.setIssuedByUtc(Instant.now());
        a.setExpiresUtc(null);
        a.setSourceUrl("https://www.swissmedic.ch/test/" + alertId);
        return a;
    }
}
