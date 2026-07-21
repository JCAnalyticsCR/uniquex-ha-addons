"""
Métricas Prometheus del mirror.

Counters e histogramas alineados con los CTQs de las investigaciones:
- reconexiones upstream
- lag de eventos (time_fired → received)
- duración de service calls
- mensajes dropeados por QueueFull
"""

from __future__ import annotations

from prometheus_client import Counter, Gauge, Histogram

# ---- Upstream HA ----

UPSTREAM_RECONNECTS = Counter(
    "ha_mirror_upstream_reconnects_total",
    "Total de reconexiones al WebSocket de HA",
)

UPSTREAM_CONNECTED = Gauge(
    "ha_mirror_upstream_connected",
    "1 si el upstream está en estado READY, 0 si no",
)

UPSTREAM_AUTH_FAILURES = Counter(
    "ha_mirror_upstream_auth_failures_total",
    "Total de fallos de autenticación contra HA (HaAuthError)",
)

EVENT_LAG_SECONDS = Histogram(
    "ha_mirror_event_lag_seconds",
    "Latencia desde time_fired en HA hasta recepción en el mirror",
    buckets=[0.05, 0.1, 0.25, 0.5, 1.0, 2.0, 5.0],
)

EVENTS_RECEIVED = Counter(
    "ha_mirror_events_received_total",
    "Total de eventos state_changed recibidos del upstream",
)

# ---- Service calls ----

SERVICE_CALLS_TOTAL = Counter(
    "ha_mirror_service_calls_total",
    "Total de service calls recibidos por el mirror",
    labelnames=["domain", "service"],
)

SERVICE_CALL_CONFIRMED = Counter(
    "ha_mirror_service_calls_confirmed_total",
    "Service calls confirmados con state_changed antes del timeout",
    labelnames=["domain"],
)

SERVICE_CALL_TIMEOUT = Counter(
    "ha_mirror_service_calls_timeout_total",
    "Service calls sin confirmación dentro del timeout",
    labelnames=["domain"],
)

SERVICE_CALL_DURATION = Histogram(
    "ha_mirror_service_call_duration_seconds",
    "Tiempo desde aceptación del call hasta confirmación (state_changed)",
    labelnames=["domain"],
    buckets=[0.1, 0.25, 0.5, 1.0, 2.0, 5.0, 10.0],
)

# ---- WebSocket clientes ----

WS_CLIENTS_CONNECTED = Gauge(
    "ha_mirror_ws_clients_connected",
    "Clientes WebSocket actualmente conectados a /ws/state",
)

WS_MESSAGES_DROPPED = Counter(
    "ha_mirror_ws_messages_dropped_total",
    "Mensajes dropeados por QueueFull en fan-out a clientes WS",
)

WS_MESSAGES_SENT = Counter(
    "ha_mirror_ws_messages_sent_total",
    "Mensajes enviados exitosamente a clientes WS",
    labelnames=["type"],
)

# ---- Hydration ----

HYDRATION_DURATION = Histogram(
    "ha_mirror_hydration_duration_seconds",
    "Duración de la hidratación completa post-auth_ok",
    buckets=[0.5, 1.0, 2.0, 5.0, 10.0, 30.0],
)

HYDRATION_ENTITY_COUNT = Gauge(
    "ha_mirror_hydration_entity_count",
    "Número de entidades hidratadas en el último ciclo",
)
