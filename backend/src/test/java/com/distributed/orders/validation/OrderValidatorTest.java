package com.distributed.orders.validation;

import com.distributed.orders.model.OrderEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for OrderValidator.
 * Tests all validation rules defined in the project specification.
 */
@DisplayName("OrderValidator Tests")
class OrderValidatorTest {

    private OrderValidator validator;

    @BeforeEach
    void setUp() {
        validator = new OrderValidator();
    }

    // -------------------------------------------------------------------------
    // Test 1: Valid order creation
    // -------------------------------------------------------------------------
    @Test
    @DisplayName("Valid order should pass validation without exceptions")
    void testValidOrderCreation() {
        OrderEvent event = new OrderEvent(
                "ORD-101",
                "Riya",
                "Laptop",
                65000.0,
                "ORDER_CREATED",
                "2024-01-01T10:00:00Z"
        );

        // Should not throw any exception
        assertDoesNotThrow(() -> validator.validate(event));
        assertTrue(validator.isValid(event));
    }

    // -------------------------------------------------------------------------
    // Test 2: Empty order ID
    // -------------------------------------------------------------------------
    @Test
    @DisplayName("Empty order ID should throw IllegalArgumentException")
    void testEmptyOrderId() {
        OrderEvent event = new OrderEvent(
                "",           // empty orderId
                "Riya",
                "Laptop",
                65000.0,
                "ORDER_CREATED",
                "2024-01-01T10:00:00Z"
        );

        IllegalArgumentException ex = assertThrows(
                IllegalArgumentException.class,
                () -> validator.validate(event)
        );
        assertTrue(ex.getMessage().contains("Order ID"));
        assertFalse(validator.isValid(event));
    }

    @Test
    @DisplayName("Null order ID should throw IllegalArgumentException")
    void testNullOrderId() {
        OrderEvent event = new OrderEvent();
        event.setOrderId(null);
        event.setCustomerName("Riya");
        event.setProduct("Laptop");
        event.setAmount(65000.0);

        assertThrows(IllegalArgumentException.class, () -> validator.validate(event));
    }

    // -------------------------------------------------------------------------
    // Test 3: Empty customer name
    // -------------------------------------------------------------------------
    @Test
    @DisplayName("Empty customer name should throw IllegalArgumentException")
    void testEmptyCustomerName() {
        OrderEvent event = new OrderEvent(
                "ORD-101",
                "   ",        // blank customer name
                "Laptop",
                65000.0,
                "ORDER_CREATED",
                "2024-01-01T10:00:00Z"
        );

        IllegalArgumentException ex = assertThrows(
                IllegalArgumentException.class,
                () -> validator.validate(event)
        );
        assertTrue(ex.getMessage().contains("Customer name"));
        assertFalse(validator.isValid(event));
    }

    // -------------------------------------------------------------------------
    // Test 4: Invalid amount
    // -------------------------------------------------------------------------
    @Test
    @DisplayName("Zero amount should throw IllegalArgumentException")
    void testZeroAmount() {
        OrderEvent event = new OrderEvent(
                "ORD-101",
                "Riya",
                "Laptop",
                0.0,          // invalid amount
                "ORDER_CREATED",
                "2024-01-01T10:00:00Z"
        );

        IllegalArgumentException ex = assertThrows(
                IllegalArgumentException.class,
                () -> validator.validate(event)
        );
        assertTrue(ex.getMessage().contains("Amount"));
        assertFalse(validator.isValid(event));
    }

    @Test
    @DisplayName("Negative amount should throw IllegalArgumentException")
    void testNegativeAmount() {
        OrderEvent event = new OrderEvent(
                "ORD-101",
                "Riya",
                "Laptop",
                -500.0,       // negative amount
                "ORDER_CREATED",
                "2024-01-01T10:00:00Z"
        );

        assertThrows(IllegalArgumentException.class, () -> validator.validate(event));
    }

    @Test
    @DisplayName("Empty product should throw IllegalArgumentException")
    void testEmptyProduct() {
        OrderEvent event = new OrderEvent(
                "ORD-101",
                "Riya",
                "",           // empty product
                65000.0,
                "ORDER_CREATED",
                "2024-01-01T10:00:00Z"
        );

        IllegalArgumentException ex = assertThrows(
                IllegalArgumentException.class,
                () -> validator.validate(event)
        );
        assertTrue(ex.getMessage().contains("Product"));
    }

    @Test
    @DisplayName("Null event should throw IllegalArgumentException")
    void testNullEvent() {
        assertThrows(IllegalArgumentException.class, () -> validator.validate(null));
    }
}
