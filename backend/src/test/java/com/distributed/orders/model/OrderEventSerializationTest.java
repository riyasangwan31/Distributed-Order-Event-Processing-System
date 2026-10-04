package com.distributed.orders.model;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for OrderEvent serialization and deserialization.
 * Verifies the JSON structure that flows through Kafka.
 */
@DisplayName("OrderEvent Serialization Tests")
class OrderEventSerializationTest {

    private ObjectMapper objectMapper;

    @BeforeEach
    void setUp() {
        objectMapper = new ObjectMapper();
    }

    // -------------------------------------------------------------------------
    // Test 5: Event serialization / deserialization
    // -------------------------------------------------------------------------
    @Test
    @DisplayName("OrderEvent should serialize to valid JSON correctly")
    void testEventSerialization() throws Exception {
        OrderEvent event = new OrderEvent(
                "ORD-202",
                "Arjun",
                "Headphones",
                3500.0,
                "ORDER_CREATED",
                "2024-01-01T12:00:00Z"
        );

        String json = objectMapper.writeValueAsString(event);

        // Verify JSON contains all fields
        assertNotNull(json);
        assertTrue(json.contains("ORD-202"), "JSON should contain orderId");
        assertTrue(json.contains("Arjun"), "JSON should contain customerName");
        assertTrue(json.contains("Headphones"), "JSON should contain product");
        assertTrue(json.contains("3500.0"), "JSON should contain amount");
        assertTrue(json.contains("ORDER_CREATED"), "JSON should contain eventType");
        assertTrue(json.contains("PENDING"), "Default status should be PENDING");
    }

    @Test
    @DisplayName("OrderEvent should deserialize from JSON correctly")
    void testEventDeserialization() throws Exception {
        String json = "{" +
                "\"orderId\":\"ORD-303\"," +
                "\"customerName\":\"Priya\"," +
                "\"product\":\"Monitor\"," +
                "\"amount\":18000.0," +
                "\"eventType\":\"ORDER_CREATED\"," +
                "\"timestamp\":\"2024-01-01T15:00:00Z\"," +
                "\"status\":\"PROCESSED\"" +
                "}";

        OrderEvent event = objectMapper.readValue(json, OrderEvent.class);

        assertNotNull(event);
        assertEquals("ORD-303", event.getOrderId());
        assertEquals("Priya", event.getCustomerName());
        assertEquals("Monitor", event.getProduct());
        assertEquals(18000.0, event.getAmount(), 0.001);
        assertEquals("ORDER_CREATED", event.getEventType());
        assertEquals("PROCESSED", event.getStatus());
    }

    @Test
    @DisplayName("Serialize then deserialize should produce identical event")
    void testRoundTripSerialization() throws Exception {
        OrderEvent original = new OrderEvent(
                "ORD-404",
                "Vikram",
                "Keyboard",
                2000.0,
                "ORDER_CREATED",
                "2024-01-01T18:00:00Z"
        );

        // Serialize
        String json = objectMapper.writeValueAsString(original);
        // Deserialize
        OrderEvent restored = objectMapper.readValue(json, OrderEvent.class);

        assertEquals(original.getOrderId(), restored.getOrderId());
        assertEquals(original.getCustomerName(), restored.getCustomerName());
        assertEquals(original.getProduct(), restored.getProduct());
        assertEquals(original.getAmount(), restored.getAmount(), 0.001);
        assertEquals(original.getEventType(), restored.getEventType());
        assertEquals(original.getStatus(), restored.getStatus());
    }

    @Test
    @DisplayName("OrderEvent default status should be PENDING after construction")
    void testDefaultStatusIsPending() {
        OrderEvent event = new OrderEvent(
                "ORD-505", "Test", "Widget", 100.0, "ORDER_CREATED", "2024-01-01T00:00:00Z"
        );
        assertEquals("PENDING", event.getStatus());
    }
}
