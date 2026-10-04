package com.distributed.orders.consumer;

import com.distributed.orders.model.OrderEvent;
import com.distributed.orders.processor.EventProcessor;
import com.distributed.orders.store.EventStore;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

/**
 * Kafka Consumer that subscribes to the "orders" topic and processes events.
 *
 * Consumer Group: order-processing-group
 * This ensures multiple consumer instances share the partition load.
 *
 * Run this as a separate process alongside the HTTP server (OrderServer).
 */
public class OrderConsumer {

    private static final Logger logger = LoggerFactory.getLogger(OrderConsumer.class);
    private static final String TOPIC = "orders";
    private static final String GROUP_ID = "order-processing-group";

    private final KafkaConsumer<String, String> consumer;
    private final EventProcessor processor;
    private final ObjectMapper objectMapper;
    private volatile boolean running = true;

    public OrderConsumer(String bootstrapServers) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        // Start from earliest unread message on first run
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        // Commit offsets automatically
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "order-consumer-1");

        this.consumer = new KafkaConsumer<>(props);
        this.processor = new EventProcessor();
        this.objectMapper = new ObjectMapper();
    }

    /**
     * Main consumer loop.
     * Polls Kafka for new messages and processes each one.
     */
    public void start() {
        consumer.subscribe(Collections.singletonList(TOPIC));
        logger.info("[CONSUMER] Subscribed to topic '{}' with group '{}'", TOPIC, GROUP_ID);
        logger.info("[CONSUMER] Waiting for order events...");

        while (running) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

            if (!records.isEmpty()) {
                logger.info("[CONSUMER] Received {} record(s) from Kafka", records.count());
            }

            for (ConsumerRecord<String, String> record : records) {
                processRecord(record);
            }
        }

        consumer.close();
        logger.info("[CONSUMER] Consumer stopped.");
    }

    /**
     * Deserializes and processes a single Kafka record.
     */
    private void processRecord(ConsumerRecord<String, String> record) {
        logger.info("[CONSUMER] Received event → partition={}, offset={}, key={}",
                record.partition(), record.offset(), record.key());

        try {
            // Deserialize JSON → OrderEvent
            OrderEvent event = objectMapper.readValue(record.value(), OrderEvent.class);
            logger.info("[CONSUMER] Deserialized: {}", event);

            // Pass to Event Processor
            OrderEvent processed = processor.process(event);

            // Store in shared EventStore so the HTTP server can serve it via GET /api/events
            EventStore.getInstance().addEvent(processed);

            logger.info("[CONSUMER] Final status for orderId={}: {}", processed.getOrderId(), processed.getStatus());

        } catch (Exception e) {
            logger.error("[CONSUMER] Failed to process record: {}", e.getMessage(), e);
        }
    }

    /**
     * Gracefully stops the consumer loop.
     */
    public void stop() {
        running = false;
    }

    // -------------------------------------------------------------------------
    // Entry point for running the consumer as a standalone process
    // -------------------------------------------------------------------------
    public static void main(String[] args) {
        String bootstrapServers = System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092");
        logger.info("========================================");
        logger.info("  Distributed Order Event Consumer");
        logger.info("  Connecting to Kafka: {}", bootstrapServers);
        logger.info("========================================");

        OrderConsumer consumer = new OrderConsumer(bootstrapServers);

        // Shutdown hook for graceful stop
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("[CONSUMER] Shutdown signal received. Stopping...");
            consumer.stop();
        }));

        consumer.start();
    }
}
