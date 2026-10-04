package com.distributed.orders.processor;

import com.distributed.orders.model.OrderEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Event Processor: the final stage of the Kafka pipeline.
 *
 * After the consumer receives an event from Kafka, the processor
 * applies business logic (validation, enrichment, status update)
 * and marks the event as PROCESSED or FAILED.
 */
public class EventProcessor {

    private static final Logger logger = LoggerFactory.getLogger(EventProcessor.class);

    /**
     * Processes a received OrderEvent.
     * In a real system this would persist to a database, trigger notifications,
     * update inventory, etc. Here we demonstrate the processing stage.
     *
     * @param event the OrderEvent received from Kafka
     * @return the processed OrderEvent with updated status
     */
    public OrderEvent process(OrderEvent event) {
        logger.info("[PROCESSOR] Processing event: orderId={}, product={}, amount={}",
                event.getOrderId(), event.getProduct(), event.getAmount());

        try {
            // Simulate processing steps
            validateEventForProcessing(event);
            enrichEvent(event);

            event.setStatus("PROCESSED");
            logger.info("[PROCESSOR] ✓ Event processed successfully: orderId={}", event.getOrderId());

        } catch (Exception e) {
            event.setStatus("FAILED");
            logger.error("[PROCESSOR] ✗ Event processing failed: orderId={}, reason={}",
                    event.getOrderId(), e.getMessage());
        }

        return event;
    }

    /**
     * Validates that the received event has all required fields intact.
     */
    private void validateEventForProcessing(OrderEvent event) {
        if (event.getOrderId() == null || event.getOrderId().isEmpty()) {
            throw new IllegalStateException("Received event has null orderId");
        }
        if (event.getAmount() <= 0) {
            throw new IllegalStateException("Received event has invalid amount: " + event.getAmount());
        }
    }

    /**
     * Enriches the event (e.g., applying business-specific transformations).
     * For demonstration, we normalize the eventType.
     */
    private void enrichEvent(OrderEvent event) {
        if (event.getEventType() == null) {
            event.setEventType("ORDER_CREATED");
        }
        // Trim all string fields
        if (event.getCustomerName() != null) {
            event.setCustomerName(event.getCustomerName().trim());
        }
        if (event.getProduct() != null) {
            event.setProduct(event.getProduct().trim());
        }
    }
}
