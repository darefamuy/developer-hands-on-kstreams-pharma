package com.hci.pharma.streams.topology;

import com.hci.pharma.streams.config.AppConfig;
import com.hci.pharma.streams.dlq.DlqProducer;
import com.hci.pharma.streams.model.PriceSurgeNotification;
import com.hci.pharma.streams.serde.JsonSerde;
import hci.MarketingStatus;
import hci.MedicinalProduct;
import hci.PriceUpdate;
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
 * Unit tests for {@link PriceSurgeDetectionTopology}.
 */
class PriceSurgeDetectionTopologyTest {

    private static final String SR_SCOPE  = "price-surge-test";
    private static final String MOCK_SR   = "mock://" + SR_SCOPE;

    private TopologyTestDriver driver;
    private TestInputTopic<String, MedicinalProduct>   productInput;
    private TestInputTopic<String, PriceUpdate>        priceInput;
    private TestOutputTopic<String, PriceSurgeNotification> surgeOutput;
    private AppConfig config;

    @BeforeEach
    void setUp() {
        Map<String, String> srConfig = Map.of(
                AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, MOCK_SR,
                AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS, "true"
        );

        config = new AppConfig();

        StreamsBuilder builder = new StreamsBuilder();
        DlqProducer mockDlqProducer = Mockito.mock(DlqProducer.class);
        var productCatalog = new AlertEnrichmentTopology(config, srConfig, mockDlqProducer)
                .buildTopology(builder);

        new PriceSurgeDetectionTopology(config, srConfig)
                .buildTopology(builder, productCatalog);

        Topology topology = builder.build();

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG,    "price-surge-test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,   Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
        props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, MOCK_SR);

        driver = new TopologyTestDriver(topology, props);

        SpecificAvroSerde<MedicinalProduct> productSerde = new SpecificAvroSerde<>();
        productSerde.configure(srConfig, false);
        SpecificAvroSerde<PriceUpdate> priceSerde = new SpecificAvroSerde<>();
        priceSerde.configure(srConfig, false);

        productInput = driver.createInputTopic(config.productsTopic(),
                Serdes.String().serializer(), productSerde.serializer());
        priceInput   = driver.createInputTopic(config.priceUpdatesTopic(),
                Serdes.String().serializer(), priceSerde.serializer());
        surgeOutput  = driver.createOutputTopic(config.priceSurgeTopic(),
                Serdes.String().deserializer(),
                new JsonSerde<>(PriceSurgeNotification.class).deserializer());
    }

    @AfterEach
    void tearDown() {
        driver.close();
        MockSchemaRegistry.dropScope(SR_SCOPE);
    }

    // ─────────────────────────────────────────────────────────────────────────

    @Test
    @DisplayName("Price increase above threshold emits surge notification")
    void priceIncrease_aboveThreshold_emitsSurgeNotification() {
        // Seed product with a baseline public price of CHF 100.00
        productInput.pipeInput("7680100000001", buildProduct("7680100000001", 80.0, 100.0));

        // Send a 25% price increase (above the 15% threshold)
        PriceUpdate update = buildPriceUpdate("7680100000001", 100.0, 125.0, "BAG_REVIEW");
        priceInput.pipeInput("7680100000001", update);

        assertThat(surgeOutput.isEmpty()).isFalse();
        PriceSurgeNotification notification = surgeOutput.readValue();
        assertThat(notification.getGtin()).isEqualTo("7680100000001");
        assertThat(notification.getChangePercent()).isGreaterThan(15.0);
        assertThat(notification.getNewPublicPriceCHF()).isEqualTo(125.0);
        assertThat(notification.getChangeReason()).isEqualTo("BAG_REVIEW");
    }

    @Test
    @DisplayName("Price increase below threshold does NOT emit notification")
    void priceIncrease_belowThreshold_noOutput() {
        productInput.pipeInput("7680100000002", buildProduct("7680100000002", 40.0, 50.0));

        // 6% increase — below 15% threshold
        priceInput.pipeInput("7680100000002",
                buildPriceUpdate("7680100000002", 50.0, 53.0, "MANUFACTURER_REQUEST"));

        assertThat(surgeOutput.isEmpty()).isTrue();
    }

    @Test
    @DisplayName("Price decrease never emits a surge notification")
    void priceDecrease_neverEmitsSurge() {
        productInput.pipeInput("7680100000003", buildProduct("7680100000003", 40.0, 50.0));

        priceInput.pipeInput("7680100000003",
                buildPriceUpdate("7680100000003", 50.0, 30.0, "GENERIC_ENTRY"));

        assertThat(surgeOutput.isEmpty()).isTrue();
    }

    @Test
    @DisplayName("Surge notification contains correct change percentage")
    void surgeNotification_changePercentIsAccurate() {
        productInput.pipeInput("7680200000001", buildProduct("7680200000001", 80.0, 200.0));
        // 50% increase
        priceInput.pipeInput("7680200000001",
                buildPriceUpdate("7680200000001", 200.0, 300.0, "BAG_REVIEW"));

        PriceSurgeNotification n = surgeOutput.readValue();
        assertThat(n.getChangePercent()).isCloseTo(50.0, within(0.01));
    }

    // ── Test data builders ────────────────────────────────────────────────────

    private MedicinalProduct buildProduct(String gtin, double epf, double pp) {
        MedicinalProduct p = new MedicinalProduct();
        p.setGtin(gtin);
        p.setAuthorizationNumber("68001");
        p.setProductName("Test Product " + gtin);
        p.setAtcCode("A01AA01");
        p.setDosageForm("Tablette");
        p.setManufacturer("TestPharma AG");
        p.setActiveSubstance("Testum");
        p.setMarketingStatus(MarketingStatus.ACTIVE);
        p.setExFactoryPriceCHF(epf);
        p.setPublicPriceCHF(pp);
        p.setLastUpdatedUtc(Instant.now());
        p.setDataSource("test");
        p.setPackageSize(null);
        p.setNarcoticsCategory(null);
        return p;
    }

    private PriceUpdate buildPriceUpdate(String gtin, double prevPP, double newPP, String reason) {
        PriceUpdate u = new PriceUpdate();
        u.setGtin(gtin);
        u.setPreviousExFactoryPriceCHF(prevPP * 0.7);
        u.setNewExFactoryPriceCHF(newPP * 0.7);
        u.setPreviousPublicPriceCHF(prevPP);
        u.setNewPublicPriceCHF(newPP);
        u.setEffectiveDateUtc(Instant.now());
        u.setChangeReason(reason);
        u.setChangedBy("test-system");
        u.setApprovalReference(null);
        return u;
    }
}
