# Pharma — Kafka Streams Application
## Use Cases

This application runs three independent processing topologies within a single Kafka Streams instance:

### 1. Alert Enrichment (`AlertEnrichmentTopology`)
**Pattern: Stateful KTable Join**

Joins every incoming `DrugAlert` event against a materialised `KTable` of the current `MedicinalProduct` catalogue. Downstream systems receive a single enriched JSON record containing both the alert detail and full product context — no additional database lookups required at consumption time.

- Narcotic (BetmG category A) alerts are split to a dedicated `hci.alerts.narcotics.v1` topic for hospital pharmacy expedited processing.
- Alerts for unknown GTINs (product not in catalogue) route to DLQ as `VALIDATION` failures.

### 2. Price Surge Detection (`PriceSurgeDetectionTopology`)
**Pattern: Stateful Join with Independent Baseline**

Detects when a product's public price increases by more than the configured threshold (default **15%**) in a single BAG price-cycle event. Maintains its own confirmed last-known price store (derived from the product catalogue) as an independent baseline — not trusting the `previousPublicPriceCHF` value in the price update event which can sometimes lag.

Emits a `PriceSurgeNotification` to `hci.prices.surge-notifications.v1` for:
- BAG compliance monitoring (large single-event price jumps may trigger a Spezialitätenliste review)
- Hospital procurement systems (budget impact alerts)
- Wholesale ERP systems (inventory revaluation)

### 3. Alert Frequency Windowing (`AlertFrequencyTopology`)
**Pattern: Tumbling Window Aggregation**

Counts drug alerts per GTIN within configurable tumbling time windows (default **60 minutes**). Emits the window count to `hci.products.alert-summary.v1` when a product exceeds 2 alerts within a single window — a signal of an active pharmacovigilance incident.

- Uses **event-time** timestamps (extracted from `issuedByUtc`) rather than Kafka ingestion time, so batched Swissmedic bulletin publications are windowed correctly.
- 10-minute grace period for late-arriving alerts.
- `suppress(untilWindowCloses)` ensures one authoritative emission per window, not a stream of incremental updates.

### 4. Product Catalogue Validation (`ProductCatalogueProcessor`)
**Pattern: Processor API with DLQ routing**

Uses the low-level Processor API (rather than the high-level DSL) to access raw processor context for precise DLQ envelope metadata. Validates incoming `MedicinalProduct` records against five business rules before admitting them to the catalogue state store:

1. GTIN key must be non-empty
2. `PENDING` products must have a Swissmedic authorisation number
3. Public price (PP) must be ≥ Ex-factory price (EPP)
4. `narcoticsCategory` must be null or a valid BetmG code (not blank)
5. Swissmedic lifecycle status transitions are forward-only (WITHDRAWN is terminal)

---

## DLQ Strategy

Three independent DLQ layers ensure no event is silently dropped:

| Layer | Handler | Error Types | Output |
|---|---|---|---|
| Deserialization | `DlqDeserializationExceptionHandler` | Corrupt Avro bytes, wire format errors | `hci.dlq.<topic>` |
| Production | `DlqProductionExceptionHandler` | RecordTooLarge, broker errors | `hci.dlq.<topic>` |
| In-topology | `ProcessingExceptionRouter` | Validation failures, runtime exceptions | `hci.dlq.<topic>` |

DLQ envelopes are written as **plain JSON** (not Avro) using a separate `DlqProducer` instance. This means DLQ records remain inspectable even when the Schema Registry or Avro stack is part of the failure.

**DLQ topic naming convention:**
```
hci.drug-alerts.avro   →  hci.dlq.drug-alerts.avro
hci.price-updates.avro   →  hci.dlq.price-updates.avro
hci.medicinal-products.avro →  hci.dlq.medicinal-products.avro
```

Each DLQ envelope contains: source topic, partition, offset, error type, exception class, full stack trace, retry count, base-64 encoded original key/value bytes, processor name, and domain context metadata (e.g. GTIN, validation rule violated).

---

## Project Structure

```
developer-hands-on-kstreams-pharma/
├── build.gradle
├── settings.gradle
├── src/
│   ├── main/
│   │   ├── avro/                          # Avro schema files (.avsc)
│   │   │   ├── hci_drug-alert_avro.avsc
│   │   │   ├── hci_medicinal-product_avro.avsc
│   │   │   └── hci_price-update_avro.avsc
│   │   ├── java/com/hci/pharma/streams/
│   │   │   ├── HciStreamsApplication.java         # Main entry point
│   │   │   ├── config/
│   │   │   │   ├── AppConfig.java                 # Configuration loader
│   │   │   │   └── KafkaStreamsConfig.java         # Streams Properties builder
│   │   │   ├── dlq/
│   │   │   │   ├── DlqEnvelope.java               # DLQ wrapper record (JSON)
│   │   │   │   ├── DlqProducer.java               # Dedicated DLQ Kafka producer
│   │   │   │   ├── DlqDeserializationExceptionHandler.java
│   │   │   │   ├── DlqProductionExceptionHandler.java
│   │   │   │   └── ProcessingExceptionRouter.java # In-topology DLQ routing
│   │   │   ├── topology/
│   │   │   │   ├── AlertEnrichmentTopology.java   # Use case 1: KTable join
│   │   │   │   ├── PriceSurgeDetectionTopology.java # Use case 2: price baseline
│   │   │   │   └── AlertFrequencyTopology.java    # Use case 3: windowed count
│   │   │   ├── processor/
│   │   │   │   └── ProductCatalogueProcessor.java # Use case 4: Processor API
│   │   │   ├── model/
│   │   │   │   ├── EnrichedAlert.java
│   │   │   │   └── PriceSurgeNotification.java
│   │   │   └── serde/
│   │   │       └── JsonSerde.java                 # Generic Jackson Serde
│   │   └── resources/
│   │       ├── application.properties
│   │       └── logback.xml
│   └── test/
│       └── java/com/hci/pharma/streams/
│           ├── topology/
│           │   ├── AlertEnrichmentTopologyTest.java
│           │   └── PriceSurgeDetectionTopologyTest.java
│           └── dlq/
│               └── DlqDeserializationExceptionHandlerTest.java
```

---

## Topics

### Input Topics

| Topic | Key | Value | Description |
|---|---|---|---|
| `hci.medicinal-products.avro` | GTIN | `MedicinalProduct` (Avro) | Product catalogue events from Swissmedic/HCI master |
| `hci.price-updates.avro` | GTIN | `PriceUpdate` (Avro) | BAG price update events |
| `hci.drug-alerts.avro` | GTIN | `DrugAlert` (Avro) | Swissmedic drug alert events |

### Output Topics

| Topic | Key | Value | Description |
|---|---|---|---|
| `hci.alerts.enriched.v1` | GTIN | `EnrichedAlert` (JSON) | Alerts joined with product data |
| `hci.alerts.narcotics.v1` | GTIN | `EnrichedAlert` (JSON) | Narcotics-only alerts |
| `hci.prices.surge-notifications.v1` | GTIN | `PriceSurgeNotification` (JSON) | Price surge events |
| `hci.products.alert-summary.v1` | `GTIN@windowStart/windowEnd` | `Long` | Alert frequency counts |

### DLQ Topics

| Topic | Contents |
|---|---|
| `hci.dlq.drug-alerts.avro` | Failed drug alert processing |
| `hci.dlq.price-updates.avro` | Failed price update processing |
| `hci.dlq.medicinal-products.avro` | Failed product catalogue updates |

---

## Configuration

All configuration lives in `src/main/resources/application.properties`. Any property can be overridden with an environment variable: dots become underscores in uppercase.

```bash
# Example: override broker for Confluent Cloud
export KAFKA_BOOTSTRAP_SERVERS=abc123.europe-west1.gcp.confluent.cloud:9092
export SCHEMA_REGISTRY_URL=https://xyz.europe-west1.gcp.confluent.cloud
export SCHEMA_REGISTRY_BASIC_AUTH_USER_INFO=KEY:SECRET
```

Key tunable parameters:

| Property | Default | Description |
|---|---|---|
| `price.surge.threshold.percent` | `15.0` | % increase in public price that triggers a surge notification |
| `alert.window.duration.minutes` | `60` | Tumbling window size for alert frequency counting |
| `streams.num.stream.threads` | `2` | Number of Kafka Streams processing threads |
| `dlq.max.retries` | `3` | Retries before routing to DLQ (production handler) |

---

## Building and Running

### Prerequisites

- Java 17+
- Gradle 8+ (or use the wrapper: `./gradlew`)
- A running Kafka broker + Schema Registry (local or Confluent Cloud)

### Local development with Docker Compose

```bash
# Start local Kafka + Schema Registry
docker compose up -d

# Create required topics
./scripts/create-topics.sh

# Build and run
./gradlew shadowJar
java -jar build/libs/hci-kafka-streams-1.0.0.jar
```

### Build

```bash
./gradlew build             # Compile, generate Avro classes, run tests
./gradlew shadowJar         # Build fat JAR for deployment
./gradlew test              # Run unit tests only
./gradlew test --info       # Verbose test output
```

### Run tests

```bash
./gradlew test
```

Tests use `TopologyTestDriver` — no running Kafka broker or Schema Registry required. The Mock Schema Registry (`io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry`) is used for Avro serde in tests.

---

## Key Concepts Reference

| Concept | Where used in this project |
|---|---|
| **KTable** | Product catalogue in `AlertEnrichmentTopology` and `PriceSurgeDetectionTopology` |
| **KStream-KTable join** | Alert enrichment and price surge baseline lookups |
| **Tumbling window** | Alert frequency counting in `AlertFrequencyTopology` |
| **suppress()** | Emit once per closed window, not on every update |
| **Event-time timestamping** | `AlertTimestampExtractor` in `AlertFrequencyTopology` |
| **Processor API** | `ProductCatalogueProcessor` — direct state store access |
| **DLQ (3 layers)** | Deserialization, Production, and in-topology processing handlers |
| **State store** | RocksDB persistent stores: `product-catalog-store`, `alert-counts-store` |
| **Exactly-once** | Configurable via `streams.exactly.once=true` in application.properties |
| **Schema Registry** | All Avro serdes use `SpecificAvroSerde` with `AutoRegisterSchemas=false` |
