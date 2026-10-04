package com.distributed.orders.producer;

import com.distributed.orders.model.OrderEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.Future;

/**
 * Kafka Producer that publishes OrderEvent messages to the "orders" topic.
 *
 * This class is the heart of the distributed system's producer component.
 * It converts the OrderEvent object to JSON and sends it to Kafka.
 */
public class OrderEventProducer {

    private static final Logger logger = LoggerFactory.getLogger(OrderEventProducer.class);
    private static final String TOPIC = "orders";

    private final KafkaProducer<String, String> producer;
    private final ObjectMapper objectMapper;

    public OrderEventProducer(String bootstrapServers) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // Ensure all replicas acknowledge the write (reliability)
        props.put(ProducerConfig.ACKS_CONFIG, "1");
        // Retry on transient failures
        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        // Unique client ID for monitoring
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "order-event-producer");

        this.producer = new KafkaProducer<>(props);
        this.objectMapper = new ObjectMapper();
    }

    /**
     * Publishes an OrderEvent to the Kafka "orders" topic.
     * The orderId is used as the Kafka message key, ensuring that all events
     * for the same order go to the same partition (ordering guarantee).
     *
     * @param event the order event to publish
     * @throws Exception if serialization or Kafka publishing fails
     */
    public void publish(OrderEvent event) throws Exception {
        String json = objectMapper.writeValueAsString(event);
        ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, event.getOrderId(), json);

        logger.info("[PRODUCER] Publishing event to Kafka topic '{}': orderId={}, product={}",
                TOPIC, event.getOrderId(), event.getProduct());

        Future<RecordMetadata> future = producer.send(record, (metadata, exception) -> {
            if (exception != null) {
                logger.error("[PRODUCER] Failed to publish event: {}", exception.getMessage());
            } else {
                logger.info("[PRODUCER] Event published successfully → partition={}, offset={}",
                        metadata.partition(), metadata.offset());
            }
        });

        // Block briefly to detect immediate send failures
        future.get();
    }

    /**
     * Closes the Kafka producer, flushing any buffered messages.
     */
    public void close() {
        producer.flush();
        producer.close();
        logger.info("[PRODUCER] Kafka producer closed.");
    }
}
