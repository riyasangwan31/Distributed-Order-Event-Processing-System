package com.distributed.orders.store;

import com.distributed.orders.model.OrderEvent;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedDeque;

/**
 * In-memory store for processed OrderEvents.
 *
 * Singleton shared between the HTTP server and the embedded consumer thread.
 * Uses a thread-safe ConcurrentLinkedDeque to handle concurrent reads/writes.
 *
 * Note: Data is lost when the server restarts. For a production system, a
 * persistent database would be used. For this demonstration, in-memory is
 * sufficient and keeps the project simple.
 */
public class EventStore {

    private static final int MAX_EVENTS = 100;
    private static final EventStore INSTANCE = new EventStore();

    private final Deque<OrderEvent> events = new ConcurrentLinkedDeque<>();

    private EventStore() {}

    public static EventStore getInstance() {
        return INSTANCE;
    }

    /**
     * Adds an event to the store. Older events are dropped when capacity is exceeded.
     */
    public void addEvent(OrderEvent event) {
        events.addFirst(event);
        // Keep only the most recent MAX_EVENTS events
        while (events.size() > MAX_EVENTS) {
            events.removeLast();
        }
    }

    /**
     * Returns all stored events (most recent first), up to the given limit.
     */
    public List<OrderEvent> getRecentEvents(int limit) {
        List<OrderEvent> result = new ArrayList<>();
        int count = 0;
        for (OrderEvent event : events) {
            if (count++ >= limit) break;
            result.add(event);
        }
        return Collections.unmodifiableList(result);
    }

    /**
     * Returns all stored events.
     */
    public List<OrderEvent> getAllEvents() {
        return getRecentEvents(MAX_EVENTS);
    }

    /**
     * Returns the total number of stored events.
     */
    public int size() {
        return events.size();
    }

    /**
     * Clears all events (used in tests).
     */
    public void clear() {
        events.clear();
    }

    // Computed statistics

    public long getProcessedCount() {
        return events.stream().filter(e -> "PROCESSED".equals(e.getStatus())).count();
    }

    public long getPendingCount() {
        return events.stream().filter(e -> "PENDING".equals(e.getStatus())).count();
    }

    public long getFailedCount() {
        return events.stream().filter(e -> "FAILED".equals(e.getStatus())).count();
    }
}
