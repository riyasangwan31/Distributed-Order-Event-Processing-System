package com.distributed.orders.processor;

import com.distributed.orders.model.OrderEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for EventProcessor.
 * Tests event processing logic including status transitions and enrichment.
 */
@DisplayName("EventProcessor Tests")
class EventProcessorTest {

    private EventProcessor processor;

    @BeforeEach
    void setUp() {
        processor = new EventProcessor();
    }

    // -------------------------------------------------------------------------
    // Test 6: Event processing
    // -------------------------------------------------------------------------
    @Test
    @DisplayName("Valid event should be processed and marked as PROCESSED")
    void testSuccessfulEventProcessing() {
        OrderEvent event = new OrderEvent(
                "ORD-601",
                "Meera",
                "Tablet",
                25000.0,
                "ORDER_CREATED",
                "2024-01-01T10:00:00Z"
        );
        event.setStatus("PENDING");

        OrderEvent result = processor.process(event);

        assertNotNull(result);
        assertEquals("PROCESSED", result.getStatus());
        assertEquals("ORD-601", result.getOrderId());
    }

    @Test
    @DisplayName("Event with invalid amount should be marked as FAILED")
    void testProcessingFailsForInvalidAmount() {
        OrderEvent event = new OrderEvent();
        event.setOrderId("ORD-602");
        event.setCustomerName("Test");
        event.setProduct("Widget");
        event.setAmount(-100.0);   // invalid
        event.setEventType("ORDER_CREATED");
        event.setTimestamp("2024-01-01T10:00:00Z");
        event.setStatus("PENDING");

        OrderEvent result = processor.process(event);

        assertEquals("FAILED", result.getStatus());
    }

    @Test
    @DisplayName("Processor should trim whitespace in customerName and product")
    void testProcessorTrimsStrings() {
        OrderEvent event = new OrderEvent(
                "ORD-603",
                "  Ananya  ",
                "  Webcam  ",
                1500.0,
                "ORDER_CREATED",
                "2024-01-01T10:00:00Z"
        );

        OrderEvent result = processor.process(event);

        assertEquals("Ananya", result.getCustomerName());
        assertEquals("Webcam", result.getProduct());
        assertEquals("PROCESSED", result.getStatus());
    }

    @Test
    @DisplayName("Processor should set eventType if null")
    void testProcessorSetsDefaultEventType() {
        OrderEvent event = new OrderEvent();
        event.setOrderId("ORD-604");
        event.setCustomerName("Kiran");
        event.setProduct("Mouse");
        event.setAmount(800.0);
        event.setEventType(null);  // null event type
        event.setTimestamp("2024-01-01T10:00:00Z");

        OrderEvent result = processor.process(event);

        assertEquals("ORDER_CREATED", result.getEventType());
        assertEquals("PROCESSED", result.getStatus());
    }

    @Test
    @DisplayName("Processor should handle event with null orderId and mark as FAILED")
    void testProcessingFailsForNullOrderId() {
        OrderEvent event = new OrderEvent();
        event.setOrderId(null);   // invalid
        event.setCustomerName("Test");
        event.setProduct("Widget");
        event.setAmount(500.0);
        event.setTimestamp("2024-01-01T10:00:00Z");

        OrderEvent result = processor.process(event);

        assertEquals("FAILED", result.getStatus());
    }

    @Test
    @DisplayName("Multiple events should be processed independently")
    void testMultipleEventsProcessedIndependently() {
        OrderEvent e1 = new OrderEvent("ORD-701", "User1", "ProductA", 100.0, "ORDER_CREATED", "2024-01-01T10:00:00Z");
        OrderEvent e2 = new OrderEvent("ORD-702", "User2", "ProductB", 200.0, "ORDER_CREATED", "2024-01-01T11:00:00Z");

        OrderEvent r1 = processor.process(e1);
        OrderEvent r2 = processor.process(e2);

        assertEquals("PROCESSED", r1.getStatus());
        assertEquals("PROCESSED", r2.getStatus());
        assertEquals("ORD-701", r1.getOrderId());
        assertEquals("ORD-702", r2.getOrderId());
    }
}
