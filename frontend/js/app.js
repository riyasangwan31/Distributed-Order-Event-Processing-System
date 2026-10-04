/**
 * Distributed Order Event Processing System
 * Frontend JavaScript — Vanilla JS, no frameworks
 *
 * Responsibilities:
 *  1. Form validation (client-side)
 *  2. POST /api/orders → Java backend → Kafka
 *  3. GET  /api/events → fetch processed events and update the table
 *  4. GET  /api/health → update system status indicators
 *  5. Activity timeline tracking
 */

'use strict';

/* ============================================================================
   CONFIGURATION
============================================================================ */
const API_BASE = 'http://localhost:8080';

/* ============================================================================
   UTILITY FUNCTIONS
============================================================================ */

/**
 * Formats an ISO timestamp string to HH:MM local time.
 */
function formatTime(isoString) {
    if (!isoString) return '–';
    try {
        const d = new Date(isoString);
        return d.toLocaleTimeString('en-IN', { hour: '2-digit', minute: '2-digit', hour12: false });
    } catch (_) {
        return '–';
    }
}

/**
 * Formats a rupee amount with ₹ symbol and commas.
 */
function formatAmount(amount) {
    if (amount === null || amount === undefined) return '–';
    return '₹' + Number(amount).toLocaleString('en-IN');
}

/**
 * Returns a time-ago string ("2 minutes ago", etc.) from an ISO timestamp.
 */
function timeAgo(isoString) {
    if (!isoString) return '';
    const diff = Math.floor((Date.now() - new Date(isoString).getTime()) / 1000);
    if (diff < 5)   return 'just now';
    if (diff < 60)  return diff + ' seconds ago';
    const mins = Math.floor(diff / 60);
    if (mins < 60)  return mins + ' minute' + (mins > 1 ? 's' : '') + ' ago';
    const hrs = Math.floor(mins / 60);
    return hrs + ' hour' + (hrs > 1 ? 's' : '') + ' ago';
}

/**
 * Shows a notification message below the form.
 */
function showNotification(type, message) {
    const el = document.getElementById('notification');
    el.className = 'notification ' + type;
    el.innerHTML = (type === 'success' ? '&#10003; ' : '&#10005; ') + message;
    // Auto-hide after 5 seconds
    clearTimeout(el._hideTimer);
    el._hideTimer = setTimeout(() => {
        el.className = 'notification hidden';
    }, 5000);
}

/**
 * Sets a field's error state.
 */
function setFieldError(fieldId, errorId, message) {
    const field = document.getElementById(fieldId);
    const error = document.getElementById(errorId);
    if (message) {
        field.classList.add('invalid');
        error.textContent = message;
    } else {
        field.classList.remove('invalid');
        error.textContent = '';
    }
}

/**
 * Clears all form error states.
 */
function clearFormErrors() {
    ['order-id', 'customer-name', 'product', 'amount'].forEach(id => {
        const field = document.getElementById(id);
        if (field) field.classList.remove('invalid');
    });
    ['error-order-id', 'error-customer-name', 'error-product', 'error-amount'].forEach(id => {
        const el = document.getElementById(id);
        if (el) el.textContent = '';
    });
}

/* ============================================================================
   CLIENT-SIDE FORM VALIDATION
============================================================================ */
function validateForm() {
    let valid = true;
    clearFormErrors();

    const orderId = document.getElementById('order-id').value.trim();
    const customerName = document.getElementById('customer-name').value.trim();
    const product = document.getElementById('product').value.trim();
    const amount = parseFloat(document.getElementById('amount').value);

    if (!orderId) {
        setFieldError('order-id', 'error-order-id', 'Order ID cannot be empty.');
        valid = false;
    }

    if (!customerName) {
        setFieldError('customer-name', 'error-customer-name', 'Customer name cannot be empty.');
        valid = false;
    }

    if (!product) {
        setFieldError('product', 'error-product', 'Please select a product.');
        valid = false;
    }

    if (isNaN(amount) || amount <= 0) {
        setFieldError('amount', 'error-amount', 'Amount must be greater than 0.');
        valid = false;
    }

    return valid;
}

/* ============================================================================
   SUBMIT FORM → POST /api/orders
============================================================================ */
document.getElementById('order-form').addEventListener('submit', async (e) => {
    e.preventDefault();

    if (!validateForm()) return;

    const submitBtn = document.getElementById('submit-btn');
    submitBtn.disabled = true;
    submitBtn.innerHTML = '<i class="fa-solid fa-spinner fa-spin"></i> <span>Sending…</span>';

    const payload = {
        orderId:      document.getElementById('order-id').value.trim(),
        customerName: document.getElementById('customer-name').value.trim(),
        product:      document.getElementById('product').value.trim(),
        amount:       parseFloat(document.getElementById('amount').value)
    };

    try {
        const response = await fetch(`${API_BASE}/api/orders`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(payload)
        });

        const data = await response.json();

        if (response.ok && data.success) {
            showNotification('success', 'Event sent to Kafka successfully — ' + payload.orderId);
            // Animate the pipeline nodes
            animatePipeline();
            // Add to activity timeline
            addActivityItem(payload.orderId + ' order event created');
            // Refresh the events table shortly after (give consumer time to process)
            setTimeout(fetchEvents, 1500);
            setTimeout(fetchEvents, 4000);
        } else {
            showNotification('error', data.message || 'Failed to send event.');
        }

    } catch (err) {
        console.error('Network error:', err);
        showNotification('error', 'Backend unavailable. Is the Java server running on port 8080?');
    } finally {
        submitBtn.disabled = false;
        submitBtn.innerHTML = '<i class="fa-solid fa-paper-plane"></i> <span>SEND EVENT</span>';
    }
});

/* ============================================================================
   FETCH EVENTS → GET /api/events
============================================================================ */
async function fetchEvents() {
    try {
        const response = await fetch(`${API_BASE}/api/events`);
        if (!response.ok) throw new Error('HTTP ' + response.status);
        const data = await response.json();

        updateStats(data);
        updateEventsTable(data.events || []);

    } catch (err) {
        console.warn('Could not fetch events:', err.message);
    }
}

/**
 * Updates the four stat cards.
 */
function updateStats(data) {
    document.getElementById('stat-total').textContent     = data.total     ?? 0;
    document.getElementById('stat-processed').textContent = data.processed ?? 0;
    document.getElementById('stat-pending').textContent   = data.pending   ?? 0;
    document.getElementById('stat-failed').textContent    = data.failed    ?? 0;
}

/**
 * Rebuilds the events table body from an array of OrderEvent objects.
 */
function updateEventsTable(events) {
    const tbody = document.getElementById('events-tbody');

    if (!events || events.length === 0) {
        tbody.innerHTML = '<tr class="empty-row"><td colspan="6">No events yet. Create an order to get started.</td></tr>';
        return;
    }

    tbody.innerHTML = '';

    events.forEach((ev, index) => {
        const status = (ev.status || 'PENDING').toUpperCase();
        const statusClass = status === 'PROCESSED' ? 'processed' : status === 'FAILED' ? 'failed' : 'pending';
        const statusDot = status === 'PROCESSED' ? '●' : status === 'FAILED' ? '●' : '●';

        const tr = document.createElement('tr');
        if (index === 0) tr.classList.add('row-new');

        tr.innerHTML = `
            <td class="order-id-cell">${escapeHtml(ev.orderId || '–')}</td>
            <td>${escapeHtml(ev.eventType || 'ORDER_CREATED')}</td>
            <td>${escapeHtml(ev.product || '–')}</td>
            <td class="amount-cell">${formatAmount(ev.amount)}</td>
            <td>
                <span class="status-pill ${statusClass}">
                    ${statusDot} ${status}
                </span>
            </td>
            <td>${formatTime(ev.timestamp)}</td>
        `;

        tbody.appendChild(tr);
    });
}

/**
 * Escapes HTML to prevent XSS.
 */
function escapeHtml(str) {
    const div = document.createElement('div');
    div.appendChild(document.createTextNode(str));
    return div.innerHTML;
}

/* ============================================================================
   HEALTH CHECK → GET /api/health
============================================================================ */
async function fetchHealth() {
    // Update navbar indicator
    const dot  = document.getElementById('kafka-dot');
    const text = document.getElementById('kafka-status-text');

    try {
        const response = await fetch(`${API_BASE}/api/health`);
        if (!response.ok) throw new Error('HTTP ' + response.status);
        const data = await response.json();

        const kafkaOk = data.kafka === 'CONNECTED';

        // Navbar badge
        dot.className  = 'status-dot ' + (kafkaOk ? 'connected' : 'disconnected');
        text.textContent = kafkaOk ? 'Kafka Connected' : 'Kafka Disconnected';

        // System status panel
        setSystemStatus('dot-kafka', 'text-kafka', kafkaOk, kafkaOk ? 'Connected' : 'Disconnected');

    } catch (err) {
        dot.className = 'status-dot error';
        text.textContent = 'Backend Offline';
        setSystemStatus('dot-kafka', 'text-kafka', false, 'Offline');
    }
}

function setSystemStatus(dotId, textId, ok, label) {
    const dotEl  = document.getElementById(dotId);
    const textEl = document.getElementById(textId);
    if (dotEl)  dotEl.className  = 'status-dot ' + (ok ? 'success' : 'error');
    if (textEl) textEl.textContent = label;
}

/* ============================================================================
   PIPELINE ANIMATION
============================================================================ */
function animatePipeline() {
    const nodes = ['node-producer', 'node-kafka', 'node-consumer', 'node-processor'];
    nodes.forEach((id, i) => {
        setTimeout(() => {
            const el = document.getElementById(id);
            if (el) {
                el.classList.add('active');
                setTimeout(() => el.classList.remove('active'), 800);
            }
        }, i * 600);
    });
}

/* ============================================================================
   ACTIVITY TIMELINE
============================================================================ */
const activityItems = [];

function addActivityItem(message) {
    activityItems.unshift({ message, timestamp: new Date().toISOString() });
    if (activityItems.length > 20) activityItems.pop();
    renderTimeline();
}

function renderTimeline() {
    const container = document.getElementById('activity-timeline');
    if (activityItems.length === 0) {
        container.innerHTML = '<div class="timeline-empty">No activity yet.</div>';
        return;
    }
    container.innerHTML = activityItems.map(item => `
        <div class="timeline-item">
            <div class="timeline-dot"></div>
            <div class="timeline-content">
                <span class="timeline-msg">${escapeHtml(item.message)}</span>
                <span class="timeline-time">${timeAgo(item.timestamp)}</span>
            </div>
        </div>
    `).join('');
}

/* ============================================================================
   REFRESH BUTTON
============================================================================ */
document.getElementById('refresh-btn').addEventListener('click', async () => {
    const btn = document.getElementById('refresh-btn');
    btn.classList.add('spinning');
    await fetchEvents();
    setTimeout(() => btn.classList.remove('spinning'), 700);
});

/* ============================================================================
   POLLING
   - Fetch events every 3 seconds (to show consumer processing results)
   - Fetch health every 10 seconds
============================================================================ */
let eventsPollInterval = null;
let healthPollInterval = null;

function startPolling() {
    // Immediately fetch
    fetchEvents();
    fetchHealth();

    // Then poll
    eventsPollInterval = setInterval(fetchEvents, 3000);
    healthPollInterval = setInterval(fetchHealth, 10000);

    // Update timeline time-ago strings every 30 seconds
    setInterval(renderTimeline, 30000);
}

/* ============================================================================
   INIT
============================================================================ */
document.addEventListener('DOMContentLoaded', () => {
    startPolling();
});
