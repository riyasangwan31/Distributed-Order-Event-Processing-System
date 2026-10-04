package com.distributed.orders.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * Represents an order event that flows through the Kafka pipeline.
 * This is the core data structure shared between producer and consumer.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class OrderEvent {

    private String orderId;
    private String customerName;
    private String product;
    private double amount;
    private String eventType;
    private String timestamp;
    private String status;

    // Default constructor required by Jackson
    public OrderEvent() {}

    public OrderEvent(String orderId, String customerName, String product,
                      double amount, String eventType, String timestamp) {
        this.orderId = orderId;
        this.customerName = customerName;
        this.product = product;
        this.amount = amount;
        this.eventType = eventType;
        this.timestamp = timestamp;
        this.status = "PENDING";
    }

    // --- Getters and Setters ---

    public String getOrderId() { return orderId; }
    public void setOrderId(String orderId) { this.orderId = orderId; }

    public String getCustomerName() { return customerName; }
    public void setCustomerName(String customerName) { this.customerName = customerName; }

    public String getProduct() { return product; }
    public void setProduct(String product) { this.product = product; }

    public double getAmount() { return amount; }
    public void setAmount(double amount) { this.amount = amount; }

    public String getEventType() { return eventType; }
    public void setEventType(String eventType) { this.eventType = eventType; }

    public String getTimestamp() { return timestamp; }
    public void setTimestamp(String timestamp) { this.timestamp = timestamp; }

    public String getStatus() { return status; }
    public void setStatus(String status) { this.status = status; }

    @Override
    public String toString() {
        return "OrderEvent{" +
               "orderId='" + orderId + '\'' +
               ", customerName='" + customerName + '\'' +
               ", product='" + product + '\'' +
               ", amount=" + amount +
               ", eventType='" + eventType + '\'' +
               ", timestamp='" + timestamp + '\'' +
               ", status='" + status + '\'' +
               '}';
    }
}
