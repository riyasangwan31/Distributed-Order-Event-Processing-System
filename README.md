# Distributed Order Event Processing System

> A real working distributed-systems project demonstrating event-driven communication using **Apache Kafka**, **Java 21**, and a **Vanilla JS dashboard**.

---

## 1. Project Overview

This project implements a complete, end-to-end distributed order event processing pipeline. When a user submits an order on the web dashboard, the event travels through:

```
Web Dashboard → Java Producer → Apache Kafka (orders topic) → Java Consumer → Event Processor → Processed Event
```

The dashboard polls the backend in real time and displays the event status as it transitions from **PENDING → PROCESSED**.

---

## 2. Problem Statement

Traditional monolithic order processing systems are tightly coupled, creating single points of failure. This project demonstrates how an event-driven architecture using Apache Kafka decouples the order creator (producer) from the order processor (consumer), enabling scalable and fault-tolerant distributed communication.

---

## 3. Objectives

- Demonstrate real Kafka-based event publishing and consumption.
- Show producer/consumer decoupling in a distributed system.
- Implement a clean REST API with form validation.
- Provide an interactive real-time dashboard.
- Include automated unit tests with JUnit 5.
- Set up GitHub Actions CI pipeline.

---

## 4. Features

- ✅ Real Kafka message publishing (no mocks or simulations)
- ✅ Java consumer subscribing to `orders` topic (consumer group: `order-processing-group`)
- ✅ REST API: `POST /api/orders`, `GET /api/events`, `GET /api/health`
- ✅ Input validation (both client-side and server-side)
- ✅ Real-time dashboard with event table, stats, and pipeline visualization
- ✅ JUnit 5 unit tests (validation, serialization, processing)
- ✅ GitHub Actions CI workflow
- ✅ Frontend served by Java HTTP server (no CORS issues, no `file://`)

---

## 5. Architecture Diagram

```
┌─────────────────────────────────────────────────────────────┐
│                        Web Dashboard                        │
│                   (HTML5 + CSS3 + Vanilla JS)               │
│                   http://localhost:8080                     │
└────────────────────────┬────────────────────────────────────┘
                         │ POST /api/orders
                         ▼
┌─────────────────────────────────────────────────────────────┐
│              Java HTTP Server (OrderServer)                  │
│              - Validates order                               │
│              - Publishes to Kafka                            │
│              - Serves GET /api/events, /api/health           │
│              - Serves frontend static files                  │
└─────────────────┬──────────────────────┬────────────────────┘
                  │ Publish               │ Embedded consumer thread
                  ▼                       ▼
┌──────────────────────────┐   ┌──────────────────────────────┐
│    Apache Kafka           │   │     OrderConsumer            │
│    Topic: orders          │◄──│     Group: order-processing  │
│    Partitions: 3          │   │     -group                   │
└──────────────────────────┘   └──────────────┬───────────────┘
                                               │
                                               ▼
                                ┌──────────────────────────────┐
                                │       EventProcessor          │
                                │  Validates → Enriches →       │
                                │  Sets status PROCESSED         │
                                └──────────────┬───────────────┘
                                               │
                                               ▼
                                ┌──────────────────────────────┐
                                │         EventStore            │
                                │  In-memory (thread-safe)      │
                                │  Served via GET /api/events   │
                                └──────────────────────────────┘
```

---

## 6. Technology Stack

| Layer      | Technology           | Version |
|------------|----------------------|---------|
| Frontend   | HTML5, CSS3, Vanilla JS | –    |
| Backend    | Java                 | 21      |
| Build      | Maven                | 3.9+    |
| Messaging  | Apache Kafka         | 3.6     |
| Broker     | Confluent Kafka Image | 7.5.0  |
| Logging    | SLF4J + Logback      | –       |
| Testing    | JUnit 5              | 5.10.1  |
| CI/CD      | GitHub Actions       | –       |
| Containers | Docker Compose       | –       |

---

## 7. Project Structure

```
distributed-order-system/
│
├── frontend/
│   ├── index.html                  # Single-page dashboard
│   ├── css/
│   │   └── style.css               # Warm palette, no blue
│   └── js/
│       └── app.js                  # API calls, form validation, polling
│
├── backend/
│   ├── pom.xml                     # Maven dependencies
│   └── src/
│       ├── main/java/com/distributed/orders/
│       │   ├── model/
│       │   │   └── OrderEvent.java         # Core event data model
│       │   ├── validation/
│       │   │   └── OrderValidator.java     # Input validation rules
│       │   ├── producer/
│       │   │   └── OrderEventProducer.java # Kafka producer
│       │   ├── consumer/
│       │   │   └── OrderConsumer.java      # Kafka consumer
│       │   ├── processor/
│       │   │   └── EventProcessor.java     # Business logic processor
│       │   ├── store/
│       │   │   └── EventStore.java         # In-memory event store
│       │   └── server/
│       │       └── OrderServer.java        # HTTP server + static files
│       ├── main/resources/
│       │   └── logback.xml                 # Logging configuration
│       └── test/java/com/distributed/orders/
│           ├── validation/
│           │   └── OrderValidatorTest.java
│           ├── model/
│           │   └── OrderEventSerializationTest.java
│           └── processor/
│               └── EventProcessorTest.java
│
├── docker-compose.yml              # Zookeeper + Kafka + topic creation
│
├── docs/
│   └── PROJECT_REPORT.md           # BTech submission report
│
├── .github/
│   └── workflows/
│       └── ci.yml                  # GitHub Actions CI pipeline
│
├── .gitignore
└── README.md                       # This file
```

---

## 8. How Kafka Works in This Project

1. **Topic**: `orders` with 3 partitions (allows parallel processing by multiple consumers).
2. **Producer**: When an order is submitted via the dashboard, the Java backend serializes it to JSON and calls `KafkaProducer.send()`. The `orderId` is used as the **message key**, ensuring all events for the same order always land in the same partition (ordering guarantee).
3. **Consumer**: The `OrderConsumer` subscribes to `orders` with consumer group `order-processing-group`. It polls for new messages, deserializes JSON back to an `OrderEvent`, and passes it to the `EventProcessor`.
4. **Offsets**: Kafka tracks which messages each consumer group has processed. If the consumer restarts, it continues from where it left off (`auto.offset.reset=earliest` on first run).

---

## 9. How the Producer Works

- Class: `OrderEventProducer`
- Uses `org.apache.kafka.clients.producer.KafkaProducer`
- Serializes `OrderEvent` → JSON using Jackson
- Sends with `acks=1` (leader acknowledgement) for reliability
- Retries up to 3 times on failure
- Logs partition and offset on successful send

---

## 10. How the Consumer Works

- Class: `OrderConsumer`
- Uses `org.apache.kafka.clients.consumer.KafkaConsumer`
- Consumer group: `order-processing-group`
- Polls with 1-second timeout
- Deserializes JSON → `OrderEvent` using Jackson
- Passes event to `EventProcessor`
- Stores result in `EventStore` (shared with HTTP server)
- Auto-commits offsets every 1 second

---

## 11. API Endpoints

### POST /api/orders
Creates and publishes an order event to Kafka.

**Request:**
```json
{
  "orderId": "ORD-101",
  "customerName": "Riya",
  "product": "Laptop",
  "amount": 65000
}
```

**Response (success):**
```json
{
  "success": true,
  "message": "Order event published to Kafka",
  "orderId": "ORD-101"
}
```

**Response (validation error):**
```json
{
  "success": false,
  "message": "Amount must be greater than 0"
}
```

### GET /api/events
Returns all events currently in the in-memory store.

**Response:**
```json
{
  "events": [...],
  "total": 5,
  "processed": 4,
  "pending": 1,
  "failed": 0
}
```

### GET /api/health
Returns server and Kafka connection status.

**Response:**
```json
{
  "status": "UP",
  "kafka": "CONNECTED",
  "producer": "RUNNING",
  "consumer": "RUNNING",
  "processor": "RUNNING",
  "timestamp": "2024-01-01T10:00:00Z"
}
```

---

## 12. Prerequisites

Install the following before running:

| Tool          | Version    | Download |
|---------------|------------|----------|
| Java JDK      | 21+        | https://adoptium.net |
| Maven         | 3.9+       | https://maven.apache.org |
| Docker Desktop | Latest    | https://www.docker.com/products/docker-desktop |
| Git           | Any        | https://git-scm.com |

Verify installations:
```bash
java -version
mvn -version
docker --version
docker compose version
```

---

## 13. Installation

```bash
# Clone the repository
git clone https://github.com/YOUR_USERNAME/distributed-order-system.git

# Navigate to the project root
cd distributed-order-system
```

---

## 14. How to Start Kafka (Docker)

From the **project root** directory:

```bash
# Start Zookeeper, Kafka, and create the orders topic
docker compose up -d

# Verify all containers are running
docker compose ps

# Check Kafka logs (optional)
docker logs kafka
```

Wait about 20–30 seconds for Kafka to be fully ready.

**To stop Kafka:**
```bash
docker compose down
```

---

## 15. How to Start the Backend (Java HTTP Server)

From the **project root** directory:

```bash
# Build the project
cd backend
mvn package -DskipTests

# Run the server (serves API + frontend on port 8080)
java -jar target/order-producer.jar
```

Or with a custom Kafka address:
```bash
KAFKA_BOOTSTRAP_SERVERS=localhost:9092 java -jar target/order-producer.jar
```

**Windows PowerShell:**
```powershell
$env:KAFKA_BOOTSTRAP_SERVERS="localhost:9092"
java -jar target/order-producer.jar
```

You should see:
```
HTTP Server started on port 8080
Dashboard: http://localhost:8080
```

---

## 16. How to Open the Dashboard

After the backend is running, open your browser at:

```
http://localhost:8080
```

> ⚠️ **Do NOT open `index.html` by double-clicking it.** The frontend must be served through the Java HTTP server to avoid CORS issues. Always use `http://localhost:8080`.

---

## 17. How to Run Tests

```bash
cd backend
mvn test
```

Tests are **pure unit tests** — they do **not** require Kafka or Docker to be running.

Expected output:
```
Tests run: 16, Failures: 0, Errors: 0, Skipped: 0
BUILD SUCCESS
```

### What is tested:

| Test Class | Tests |
|---|---|
| `OrderValidatorTest` | Valid order, empty orderId, empty customerName, zero amount, negative amount, empty product, null event |
| `OrderEventSerializationTest` | JSON serialization, deserialization, round-trip, default status |
| `EventProcessorTest` | Successful processing, failed for invalid amount, string trimming, null eventType, null orderId, multiple events |

---

## 18. GitHub Actions CI

The CI pipeline runs on every `push` to `main` or `develop` and on every Pull Request targeting `main`.

**Pipeline steps:**
1. Checkout repository
2. Set up JDK 21 (Temurin)
3. Cache Maven dependencies
4. Run `mvn test` — **fails the pipeline if any test fails**
5. Run `mvn package -DskipTests` — builds JARs
6. Upload test reports and JARs as artifacts

**View CI status:** Go to your GitHub repository → Actions tab.

---

## 19. GitHub Branch Protection (Recommended)

To enforce that tests must pass before merging to `main`:

1. Go to your GitHub repository → **Settings → Branches**
2. Click **Add branch protection rule**
3. Set **Branch name pattern**: `main`
4. Enable:
   - ✅ **Require a pull request before merging**
   - ✅ **Require status checks to pass before merging**
   - Search for and add: `build-and-test`
   - ✅ **Require branches to be up to date before merging**
5. Click **Save changes**

> ⚠️ **Important**: GitHub Actions alone does NOT prevent direct `git push` to `main`. Branch protection rules must be configured to enforce this. Once set up, failed tests will automatically block the Pull Request from being merged.

---

## 20. Screenshots

> _Add screenshots here after running the project._

| Screenshot | Description |
|---|---|
| `docs/screenshots/dashboard.png` | Main dashboard with stats |
| `docs/screenshots/create-order.png` | Order creation form |
| `docs/screenshots/events-table.png` | Events table with statuses |
| `docs/screenshots/consumer-logs.png` | Consumer terminal output |
| `docs/screenshots/github-actions.png` | CI pipeline passing |

---

## 21. Distributed Systems Concepts Demonstrated

| Concept | Implementation |
|---|---|
| **Event-driven architecture** | Orders trigger events that flow asynchronously through Kafka |
| **Producer-consumer decoupling** | Producer and consumer run independently; neither knows the other directly |
| **Message broker** | Kafka acts as the central message broker (topic: `orders`) |
| **Consumer groups** | `order-processing-group` allows horizontal scaling |
| **Partitioning** | 3 partitions; orderId used as key for ordering guarantee |
| **Offset management** | Kafka tracks consumer position; consumer resumes after restart |
| **Fault tolerance** | Producer retries on failure; consumer auto-commits offsets |
| **Asynchronous processing** | Dashboard submits order and polls for result; not a blocking call |

---

## 22. Limitations

- **No persistent storage**: Events are stored in-memory; lost on server restart.
- **Single Kafka broker**: Uses one broker for simplicity; production requires replication.
- **No authentication**: The API has no security layer (intentionally out of scope).
- **Consumer in same JVM**: For simplicity, the consumer runs as a thread inside the server JAR. In production, it would be a separate service.

---

## 23. Future Improvements

- Add a PostgreSQL or MongoDB database for event persistence.
- Add a separate consumer microservice.
- Add WebSocket support for true real-time push updates.
- Add Kafka Streams for event aggregation and analytics.
- Add Kubernetes deployment manifests.
- Add authentication with JWT.
- Add dead-letter queue (DLQ) for failed events.

---

## License

MIT License — for educational use.
