package com.distributed.orders.validation;

import com.distributed.orders.model.OrderEvent;

/**
 * Validates incoming order data before it is published to Kafka.
 * All validation rules are in one place for easy understanding.
 */
public class OrderValidator {

    /**
     * Validates an OrderEvent and throws an IllegalArgumentException
     * with a descriptive message if validation fails.
     *
     * @param event the OrderEvent to validate
     * @throws IllegalArgumentException if any field is invalid
     */
    public void validate(OrderEvent event) {
        if (event == null) {
            throw new IllegalArgumentException("Order event cannot be null");
        }

        if (event.getOrderId() == null || event.getOrderId().trim().isEmpty()) {
            throw new IllegalArgumentException("Order ID cannot be empty");
        }

        if (event.getCustomerName() == null || event.getCustomerName().trim().isEmpty()) {
            throw new IllegalArgumentException("Customer name cannot be empty");
        }

        if (event.getProduct() == null || event.getProduct().trim().isEmpty()) {
            throw new IllegalArgumentException("Product cannot be empty");
        }

        if (event.getAmount() <= 0) {
            throw new IllegalArgumentException("Amount must be greater than 0");
        }
    }

    /**
     * Returns true if the event is valid, false otherwise.
     * A convenience method for callers who want a boolean instead of an exception.
     */
    public boolean isValid(OrderEvent event) {
        try {
            validate(event);
            return true;
        } catch (IllegalArgumentException e) {
            return false;
        }
    }
}
