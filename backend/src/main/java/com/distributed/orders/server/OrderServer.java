package com.distributed.orders.server;

import com.distributed.orders.consumer.OrderConsumer;
import com.distributed.orders.model.OrderEvent;
import com.distributed.orders.producer.OrderEventProducer;
import com.distributed.orders.store.EventStore;
import com.distributed.orders.validation.OrderValidator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;

/**
 * Lightweight HTTP server that:
 *  - Serves the frontend (index.html, CSS, JS) from the ../frontend/ directory
 *  - Provides REST API endpoints: POST /api/orders, GET /api/events, GET /api/health
 *  - Runs an embedded Kafka consumer in a background thread
 *
 * Uses Java's built-in com.sun.net.httpserver.HttpServer (no Spring required).
 */
public class OrderServer {

    private static final Logger logger = LoggerFactory.getLogger(OrderServer.class);
    private static final int PORT = 8080;

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final OrderValidator validator = new OrderValidator();
    private OrderEventProducer kafkaProducer;
    private String bootstrapServers;

    public OrderServer(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
    }

    public void start() throws IOException {
        // Initialize Kafka producer
        kafkaProducer = new OrderEventProducer(bootstrapServers);

        // Start embedded Kafka consumer in background thread
        Thread consumerThread = new Thread(() -> {
            OrderConsumer consumer = new OrderConsumer(bootstrapServers);
            Runtime.getRuntime().addShutdownHook(new Thread(consumer::stop));
            consumer.start();
        }, "kafka-consumer-thread");
        consumerThread.setDaemon(true);
        consumerThread.start();

        // Create and configure HTTP server
        HttpServer server = HttpServer.create(new InetSocketAddress(PORT), 0);
        server.setExecutor(Executors.newFixedThreadPool(10));

        // API routes
        server.createContext("/api/orders", new OrderHandler());
        server.createContext("/api/events", new EventsHandler());
        server.createContext("/api/health", new HealthHandler());

        // Serve frontend static files
        server.createContext("/", new StaticFileHandler());

        server.start();

        logger.info("========================================");
        logger.info("  Distributed Order Event Processing");
        logger.info("  HTTP Server started on port {}", PORT);
        logger.info("  Dashboard: http://localhost:{}", PORT);
        logger.info("  Kafka:     {}", bootstrapServers);
        logger.info("========================================");
    }

    // -------------------------------------------------------------------------
    // Handler: POST /api/orders
    // -------------------------------------------------------------------------
    private class OrderHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            addCorsHeaders(exchange);

            if ("OPTIONS".equalsIgnoreCase(exchange.getRequestMethod())) {
                exchange.sendResponseHeaders(204, -1);
                return;
            }

            if (!"POST".equalsIgnoreCase(exchange.getRequestMethod())) {
                sendJson(exchange, 405, Map.of("success", false, "message", "Method not allowed"));
                return;
            }

            try {
                // Parse request body
                InputStream is = exchange.getRequestBody();
                byte[] body = is.readAllBytes();
                OrderEvent event = objectMapper.readValue(body, OrderEvent.class);

                // Set metadata
                event.setEventType("ORDER_CREATED");
                event.setTimestamp(Instant.now().toString());
                event.setStatus("PENDING");

                // Validate
                validator.validate(event);

                // Publish to Kafka
                kafkaProducer.publish(event);

                // Add as PENDING to store immediately (consumer will update to PROCESSED)
                EventStore.getInstance().addEvent(event);

                logger.info("[SERVER] Order accepted: orderId={}", event.getOrderId());
                sendJson(exchange, 200, Map.of(
                        "success", true,
                        "message", "Order event published to Kafka",
                        "orderId", event.getOrderId()
                ));

            } catch (IllegalArgumentException e) {
                logger.warn("[SERVER] Validation error: {}", e.getMessage());
                sendJson(exchange, 400, Map.of("success", false, "message", e.getMessage()));
            } catch (Exception e) {
                logger.error("[SERVER] Error processing order: {}", e.getMessage(), e);
                sendJson(exchange, 500, Map.of("success", false,
                        "message", "Failed to publish event to Kafka: " + e.getMessage()));
            }
        }
    }

    // -------------------------------------------------------------------------
    // Handler: GET /api/events
    // -------------------------------------------------------------------------
    private class EventsHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            addCorsHeaders(exchange);

            if ("OPTIONS".equalsIgnoreCase(exchange.getRequestMethod())) {
                exchange.sendResponseHeaders(204, -1);
                return;
            }

            if (!"GET".equalsIgnoreCase(exchange.getRequestMethod())) {
                sendJson(exchange, 405, Map.of("success", false, "message", "Method not allowed"));
                return;
            }

            try {
                EventStore store = EventStore.getInstance();
                List<OrderEvent> events = store.getAllEvents();

                Map<String, Object> response = new HashMap<>();
                response.put("events", events);
                response.put("total", store.size());
                response.put("processed", store.getProcessedCount());
                response.put("pending", store.getPendingCount());
                response.put("failed", store.getFailedCount());

                sendJson(exchange, 200, response);
            } catch (Exception e) {
                logger.error("[SERVER] Error fetching events: {}", e.getMessage());
                sendJson(exchange, 500, Map.of("success", false, "message", "Error fetching events"));
            }
        }
    }

    // -------------------------------------------------------------------------
    // Handler: GET /api/health
    // -------------------------------------------------------------------------
    private class HealthHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            addCorsHeaders(exchange);

            if ("OPTIONS".equalsIgnoreCase(exchange.getRequestMethod())) {
                exchange.sendResponseHeaders(204, -1);
                return;
            }

            // Check if Kafka producer is initialized (basic health)
            boolean kafkaOk = kafkaProducer != null;

            Map<String, Object> response = new HashMap<>();
            response.put("status", "UP");
            response.put("kafka", kafkaOk ? "CONNECTED" : "DISCONNECTED");
            response.put("producer", kafkaOk ? "RUNNING" : "STOPPED");
            response.put("consumer", "RUNNING");
            response.put("processor", "RUNNING");
            response.put("timestamp", Instant.now().toString());

            sendJson(exchange, 200, response);
        }
    }

    // -------------------------------------------------------------------------
    // Static file handler: serves frontend/ directory
    // -------------------------------------------------------------------------
    private class StaticFileHandler implements HttpHandler {

        // Resolve the frontend directory relative to where the JAR/class is run
        private final Path frontendRoot;

        StaticFileHandler() {
            // Try several candidate paths so the server works both from the
            // backend/ working directory and from the project root.
            Path candidate1 = Paths.get("../frontend").toAbsolutePath().normalize();
            Path candidate2 = Paths.get("frontend").toAbsolutePath().normalize();
            if (Files.isDirectory(candidate1)) {
                frontendRoot = candidate1;
            } else {
                frontendRoot = candidate2;
            }
            logger.info("[SERVER] Serving frontend from: {}", frontendRoot);
        }

        @Override
        public void handle(HttpExchange exchange) throws IOException {
            addCorsHeaders(exchange);
            String path = exchange.getRequestURI().getPath();

            // Default to index.html
            if ("/".equals(path) || path.isEmpty()) {
                path = "/index.html";
            }

            Path filePath = frontendRoot.resolve(path.substring(1)).normalize();

            // Security: prevent path traversal
            if (!filePath.startsWith(frontendRoot)) {
                sendPlain(exchange, 403, "Forbidden");
                return;
            }

            if (!Files.exists(filePath) || Files.isDirectory(filePath)) {
                sendPlain(exchange, 404, "Not found: " + path);
                return;
            }

            String contentType = detectContentType(path);
            byte[] bytes = Files.readAllBytes(filePath);

            exchange.getResponseHeaders().set("Content-Type", contentType);
            exchange.sendResponseHeaders(200, bytes.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(bytes);
            }
        }

        private String detectContentType(String path) {
            if (path.endsWith(".html")) return "text/html; charset=UTF-8";
            if (path.endsWith(".css"))  return "text/css; charset=UTF-8";
            if (path.endsWith(".js"))   return "application/javascript; charset=UTF-8";
            if (path.endsWith(".json")) return "application/json; charset=UTF-8";
            if (path.endsWith(".png"))  return "image/png";
            if (path.endsWith(".ico"))  return "image/x-icon";
            return "application/octet-stream";
        }
    }

    // -------------------------------------------------------------------------
    // Utility helpers
    // -------------------------------------------------------------------------

    private void addCorsHeaders(HttpExchange exchange) {
        exchange.getResponseHeaders().set("Access-Control-Allow-Origin", "*");
        exchange.getResponseHeaders().set("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
        exchange.getResponseHeaders().set("Access-Control-Allow-Headers", "Content-Type");
    }

    private void sendJson(HttpExchange exchange, int statusCode, Object data) throws IOException {
        String json = objectMapper.writeValueAsString(data);
        byte[] bytes = json.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "application/json; charset=UTF-8");
        exchange.sendResponseHeaders(statusCode, bytes.length);
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(bytes);
        }
    }

    private void sendPlain(HttpExchange exchange, int statusCode, String text) throws IOException {
        byte[] bytes = text.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "text/plain; charset=UTF-8");
        exchange.sendResponseHeaders(statusCode, bytes.length);
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(bytes);
        }
    }

    // -------------------------------------------------------------------------
    // Main entry point
    // -------------------------------------------------------------------------
    public static void main(String[] args) throws IOException {
        String bootstrapServers = System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092");

        OrderServer server = new OrderServer(bootstrapServers);
        server.start();

        // Keep the main thread alive
        Runtime.getRuntime().addShutdownHook(new Thread(() ->
                LoggerFactory.getLogger(OrderServer.class).info("[SERVER] Shutting down.")
        ));
    }
}
