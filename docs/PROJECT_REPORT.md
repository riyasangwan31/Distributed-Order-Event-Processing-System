# Project Report

## Distributed Order Event Processing System

**Course:** Distributed Systems (BTech CSE)
**Academic Year:** 2024–25
**Technology Stack:** Java 21 · Apache Kafka · Docker · HTML5 · JUnit 5 · GitHub Actions

---

## 1. Introduction

Modern software systems increasingly rely on distributed architectures to achieve scalability, fault tolerance, and maintainability. This project implements a **Distributed Order Event Processing System** that demonstrates core distributed-systems concepts through a practical, end-to-end working application.

The system allows users to submit orders through a web dashboard. Each order is published as an event to Apache Kafka, consumed by a dedicated consumer, processed by an event processor, and made available again through a REST API.

---

## 2. Problem Statement

Traditional monolithic systems process orders synchronously — the HTTP request blocks until the entire processing pipeline completes. This approach has several weaknesses:

- **Tight coupling**: The order creation component must directly call the processing component.
- **Single point of failure**: If the processor is unavailable, order creation fails.
- **Poor scalability**: The entire system must be scaled together even if only one component is overloaded.

An event-driven architecture addresses these problems by introducing a message broker between producer and consumer.

---

## 3. Objectives

1. Implement a real Kafka-based event pipeline (not simulated).
2. Demonstrate producer–consumer decoupling.
3. Implement input validation at both frontend and backend.
4. Build a real-time monitoring dashboard.
5. Write meaningful unit tests using JUnit 5.
6. Set up a GitHub Actions CI pipeline.
7. Document the complete system for academic submission.

---

## 4. Architecture

### 4.1 High-Level Architecture

```
Web Dashboard (browser)
        │
        │ HTTP POST /api/orders
        ▼
Java HTTP Server (OrderServer — port 8080)
        │ validates, creates OrderEvent
        │
        ▼
KafkaProducer.send()
        │
        ▼
Apache Kafka Broker (Docker — port 9092)
  Topic: orders
  Partitions: 3
        │
        ▼
KafkaConsumer.poll()
  Consumer Group: order-processing-group
        │
        ▼
EventProcessor
  - Validates received event
  - Enriches (trims strings, sets default eventType)
  - Sets status = PROCESSED or FAILED
        │
        ▼
EventStore (in-memory, thread-safe)
        │
        │ HTTP GET /api/events
        ▼
Web Dashboard (updates table and stats)
```

### 4.2 Key Design Decisions

| Decision | Rationale |
|---|---|
| Java's built-in HttpServer | No Spring dependency needed; keeps the project lightweight and easy to understand |
| orderId as Kafka message key | Guarantees ordering: all events for the same order go to the same partition |
| Embedded consumer thread | Simplifies deployment for a college demo; no separate process needed |
| In-memory EventStore | Avoids database setup; acceptable for demonstration scope |
| Frontend served by Java server | Eliminates CORS issues; single port (8080) for everything |

---

## 5. Components

### 5.1 OrderEvent (Model)

The core data structure that flows through the entire pipeline:

```json
{
  "orderId": "ORD-101",
  "customerName": "Riya",
  "product": "Laptop",
  "amount": 65000,
  "eventType": "ORDER_CREATED",
  "timestamp": "2024-01-01T10:00:00Z",
  "status": "PENDING"
}
```

### 5.2 OrderValidator

Enforces business rules:
- Order ID must not be empty.
- Customer name must not be empty.
- Product must not be empty.
- Amount must be > 0.

### 5.3 OrderEventProducer

Initializes a `KafkaProducer<String, String>`. Serializes the `OrderEvent` to JSON using Jackson and sends it to the `orders` topic using the `orderId` as the message key.

### 5.4 OrderConsumer

Initializes a `KafkaConsumer<String, String>` with group ID `order-processing-group`. Continuously polls Kafka with a 1-second timeout. For each received record, it deserializes the JSON back to an `OrderEvent` and calls `EventProcessor.process()`.

### 5.5 EventProcessor

Applies processing logic:
1. Validates that the received event has valid fields (orderId not null, amount > 0).
2. Enriches the event (trims strings, sets default eventType if null).
3. Sets `status = PROCESSED` on success or `status = FAILED` on exception.

### 5.6 EventStore

Thread-safe singleton using `ConcurrentLinkedDeque`. Holds up to 100 recent events. Provides computed statistics (processed count, pending count, failed count) consumed by the `/api/events` endpoint.

### 5.7 OrderServer

Built-in Java `HttpServer` (no external library). Handles:
- `POST /api/orders` → validates, publishes to Kafka, stores pending event
- `GET /api/events` → returns events from EventStore
- `GET /api/health` → returns Kafka connection status
- `/` and static files → serves the frontend directory

### 5.8 Frontend Dashboard

- **Navbar**: Title, "Kafka Event Pipeline" label, live Kafka connection indicator.
- **Stats cards**: Total Events, Processed, Pending, Failed — updated every 3 seconds.
- **Create Order form**: Client-side validation + real HTTP POST.
- **Event Pipeline diagram**: Visual PRODUCER → KAFKA → CONSUMER → PROCESSOR flow.
- **Events table**: Order ID, Event Type, Product, Amount (₹), Status (colored pill), Time.
- **System Status panel**: Real-time status from `/api/health`.
- **Activity Timeline**: Timestamped list of recent events.

---

## 6. Technologies

### Apache Kafka

Apache Kafka is a distributed event streaming platform. It organizes messages into **topics**. Topics are divided into **partitions** for parallelism. Producers write to partitions; consumers read from them in offset order.

In this project:
- **Topic**: `orders`
- **Partitions**: 3
- **Consumer Group**: `order-processing-group`
- **Offsets**: managed automatically by Kafka (`enable.auto.commit=true`)

### Java 21 with Maven

Java 21 LTS provides virtual threads and modern language features. Maven manages the build lifecycle and dependency resolution.

### JUnit 5

JUnit Jupiter (JUnit 5) provides `@Test`, `@BeforeEach`, `assertThrows`, `assertEquals`, `assertTrue`, `assertDoesNotThrow` and `@DisplayName` annotations used throughout the test suite.

### Docker Compose

Docker Compose runs Zookeeper and Kafka as containers. A `kafka-setup` service creates the `orders` topic automatically on first start.

### GitHub Actions

GitHub Actions runs the CI pipeline on every push and pull request. The pipeline fails if any JUnit test fails, preventing broken code from being merged to `main`.

---

## 7. Working — Step by Step

1. **User opens `http://localhost:8080`** → Java server serves `frontend/index.html`.
2. **User fills the Create Order form** → JavaScript validates fields client-side.
3. **User clicks SEND EVENT** → JavaScript calls `POST /api/orders` with JSON payload.
4. **Java server receives request** → `OrderValidator.validate()` checks all fields.
5. **If valid** → `OrderEventProducer.publish()` serializes to JSON and calls `KafkaProducer.send()`.
6. **Kafka stores the message** in the `orders` topic (partition selected by orderId hash).
7. **Consumer thread polls Kafka** → `KafkaConsumer.poll()` returns the new record.
8. **Consumer deserializes JSON** → `OrderEvent` object reconstructed.
9. **EventProcessor.process()** is called → status set to `PROCESSED`.
10. **EventStore.addEvent()** stores the processed event.
11. **Dashboard polls `GET /api/events`** every 3 seconds → table updates with PROCESSED status.

---

## 8. Kafka Communication Details

### Producer Configuration

```
bootstrap.servers = localhost:9092
key.serializer    = StringSerializer
value.serializer  = StringSerializer
acks              = 1
retries           = 3
client.id         = order-event-producer
```

### Consumer Configuration

```
bootstrap.servers        = localhost:9092
group.id                 = order-processing-group
key.deserializer         = StringDeserializer
value.deserializer       = StringDeserializer
auto.offset.reset        = earliest
enable.auto.commit       = true
auto.commit.interval.ms  = 1000
```

### Topic Configuration

```
Topic Name:    orders
Partitions:    3
Replication:   1 (single broker, sufficient for development)
```

---

## 9. Testing

### Test Coverage

| Test Class | Tests | What is Verified |
|---|---|---|
| `OrderValidatorTest` | 7 | Valid order, empty orderId, null orderId, blank customerName, empty product, zero amount, negative amount, null event |
| `OrderEventSerializationTest` | 4 | JSON serialization, deserialization, round-trip, default status |
| `EventProcessorTest` | 6 | Successful processing → PROCESSED, invalid amount → FAILED, string trimming, null eventType handling, null orderId → FAILED, multiple independent events |

**Total: 17 test methods across 3 test classes.**

All tests are pure unit tests. They do NOT require Kafka or Docker. They can be run with:

```bash
cd backend
mvn test
```

---

## 10. CI/CD Pipeline

The GitHub Actions workflow (`.github/workflows/ci.yml`) does:

1. Checkout code
2. Set up JDK 21 (Adoptium Temurin)
3. Cache Maven repository (uses `pom.xml` as cache key)
4. Run `mvn test` — fails build if any test fails
5. Run `mvn package -DskipTests` — builds fat JARs
6. Upload test reports and JARs as workflow artifacts

The CI pipeline ensures that:
- Every push to `main` or `develop` is tested automatically.
- Every Pull Request is tested before merging.
- Failed tests immediately notify the developer via GitHub.

---

## 11. Distributed Systems Concepts Demonstrated

| Concept | How Demonstrated |
|---|---|
| **Event-driven architecture** | Orders create events that are published and consumed asynchronously |
| **Producer–consumer decoupling** | Producer and consumer have no direct reference to each other |
| **Message broker** | Apache Kafka decouples producers from consumers |
| **Partitioning** | `orders` topic has 3 partitions; orderId used as key for deterministic routing |
| **Consumer groups** | `order-processing-group` — Kafka tracks offset per group |
| **Offset management** | Consumer resumes from last offset after restart (`earliest` on first run) |
| **Fault tolerance** | Producer retries; consumer auto-commits offsets |
| **Asynchronous processing** | Dashboard polls for results; order creation doesn't block on processing |
| **Horizontal scalability** | Multiple consumer instances in the group would share partition load |

---

## 12. Results

The completed system achieves:

- ✅ Real Kafka message publishing confirmed by partition/offset logs.
- ✅ Consumer receives and processes events with clear log output.
- ✅ Dashboard displays live event status transitions (PENDING → PROCESSED).
- ✅ All 17 JUnit tests pass with `mvn test`.
- ✅ GitHub Actions CI pipeline runs successfully.
- ✅ Frontend served through HTTP (no CORS issues).
- ✅ Input validation works on both client and server sides.

---

## 13. Limitations

1. **No persistent storage**: Events are in-memory; lost on server restart. A real system would use a database.
2. **Single Kafka broker**: Production systems use 3+ brokers for replication.
3. **Consumer in same JVM**: For simplicity, the consumer runs as a thread inside the server JAR. A production system would deploy it as a separate service.
4. **No WebSocket push**: The dashboard polls every 3 seconds instead of receiving server-push updates.
5. **No authentication or authorization**: Intentionally out of scope for this demonstration.

---

## 14. Future Scope

- Add PostgreSQL/MongoDB for persistent event storage.
- Separate the consumer into a standalone microservice.
- Add Kafka Streams for aggregation and real-time analytics.
- Implement WebSocket (SSE) for real-time dashboard push.
- Add a dead-letter queue (DLQ) for failed events.
- Deploy with Kubernetes + Helm charts.
- Add distributed tracing with OpenTelemetry.
- Add Kafka Schema Registry for event schema management.

---

## 15. Conclusion

This project successfully demonstrates event-driven distributed communication using Apache Kafka. The system shows how a producer and consumer can be decoupled through a message broker, enabling asynchronous, fault-tolerant, and scalable event processing.

The implementation covers the complete distributed-systems pipeline from web dashboard to event processor, with real Kafka integration, proper unit testing, CI/CD automation, and a clean, interactive frontend. It provides a solid foundation for understanding and explaining distributed-systems concepts in an academic or professional context.

---

*Submitted as part of BTech Distributed Systems coursework.*
