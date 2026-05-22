"""Fake microservices metrics exporter for local dev testing.

Simulates a checkout-service, payment-api, and inventory-db with realistic
Prometheus metrics: request rates, error rates, latencies, resource usage.
"""
import random
import time
import threading
from prometheus_client import (
    Counter,
    Gauge,
    Histogram,
    start_http_server,
    REGISTRY,
)

# ── Checkout Service ─────────────────────────────────────────────────────────

checkout_requests = Counter(
    "http_requests_total",
    "Total HTTP requests",
    ["service", "method", "status", "path"],
)
checkout_latency = Histogram(
    "http_request_duration_seconds",
    "HTTP request latency",
    ["service", "method", "path"],
    buckets=[0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0],
)
checkout_in_flight = Gauge(
    "http_requests_in_flight",
    "In-flight requests",
    ["service"],
)

# ── Resource metrics ─────────────────────────────────────────────────────────

cpu_usage = Gauge("process_cpu_usage_ratio", "CPU usage ratio", ["service"])
memory_bytes = Gauge("process_memory_bytes", "Memory usage bytes", ["service"])
goroutines = Gauge("go_goroutines", "Number of goroutines", ["service"])

# ── Pod / K8s-like metrics ───────────────────────────────────────────────────

pod_restarts = Counter("kube_pod_container_restarts_total", "Pod restarts", ["pod", "namespace"])
container_cpu = Gauge("container_cpu_usage_seconds_total", "Container CPU", ["pod", "namespace", "container"])
container_memory = Gauge("container_memory_working_set_bytes", "Container memory", ["pod", "namespace", "container"])

# ── Database metrics ─────────────────────────────────────────────────────────

db_connections_active = Gauge("db_connections_active", "Active DB connections", ["service", "database"])
db_query_duration = Histogram(
    "db_query_duration_seconds",
    "DB query latency",
    ["service", "database", "operation"],
    buckets=[0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0],
)

SERVICES = ["checkout-service", "payment-api", "inventory-db"]
METHODS = ["GET", "POST"]
PATHS = ["/api/checkout", "/api/payment", "/api/inventory", "/healthz"]
NAMESPACES = ["default", "production"]


def simulate_traffic():
    """Generate realistic traffic patterns with occasional error spikes."""
    tick = 0
    while True:
        tick += 1

        for svc in SERVICES:
            # Base request rate varies by service
            base_rate = {"checkout-service": 15, "payment-api": 10, "inventory-db": 5}[svc]
            n_requests = random.randint(base_rate - 3, base_rate + 5)

            for _ in range(n_requests):
                method = random.choice(METHODS)
                path = random.choice(PATHS)

                # Error rate: normally 1%, spikes to 15% every ~60 ticks for checkout
                error_chance = 0.01
                if svc == "checkout-service" and (tick % 120) > 100:
                    error_chance = 0.15  # error spike

                status = "500" if random.random() < error_chance else random.choice(["200", "200", "200", "201", "304"])

                checkout_requests.labels(service=svc, method=method, status=status, path=path).inc()

                # Latency: normally 20-100ms, spikes during errors
                base_lat = random.uniform(0.02, 0.1)
                if status == "500":
                    base_lat = random.uniform(0.5, 3.0)  # slow when erroring
                elif svc == "payment-api":
                    base_lat = random.uniform(0.05, 0.3)  # payment is slower

                checkout_latency.labels(service=svc, method=method, path=path).observe(base_lat)

            # In-flight gauge
            checkout_in_flight.labels(service=svc).set(random.randint(2, 25))

            # CPU / memory
            cpu_base = {"checkout-service": 0.3, "payment-api": 0.2, "inventory-db": 0.4}[svc]
            cpu_usage.labels(service=svc).set(cpu_base + random.uniform(-0.05, 0.15))
            mem_base = {"checkout-service": 256e6, "payment-api": 180e6, "inventory-db": 512e6}[svc]
            memory_bytes.labels(service=svc).set(mem_base + random.uniform(-20e6, 50e6))
            goroutines.labels(service=svc).set(random.randint(50, 200))

            # Pod metrics
            for i in range(2):  # 2 replicas per service
                pod = f"{svc}-{i}"
                ns = "production"
                container_cpu.labels(pod=pod, namespace=ns, container=svc).set(
                    cpu_base + random.uniform(-0.02, 0.1)
                )
                container_memory.labels(pod=pod, namespace=ns, container=svc).set(
                    mem_base + random.uniform(-10e6, 30e6)
                )

            # Occasional pod restart
            if random.random() < 0.005:
                pod_restarts.labels(pod=f"{svc}-{random.randint(0,1)}", namespace="production").inc()

        # DB metrics
        db_connections_active.labels(service="checkout-service", database="orders_db").set(random.randint(5, 30))
        db_connections_active.labels(service="inventory-db", database="inventory_db").set(random.randint(3, 20))

        for op in ["SELECT", "INSERT", "UPDATE"]:
            db_query_duration.labels(
                service="checkout-service", database="orders_db", operation=op
            ).observe(random.uniform(0.001, 0.05))
            db_query_duration.labels(
                service="inventory-db", database="inventory_db", operation=op
            ).observe(random.uniform(0.002, 0.1))

        time.sleep(1)  # tick every second


if __name__ == "__main__":
    print("Starting fake metrics exporter on :9091 ...")
    start_http_server(9091)
    simulate_traffic()
