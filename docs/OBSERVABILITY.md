# Datadog Observability

Full observability support with optional `datadog` feature flag. Zero overhead when disabled.

## Enable Datadog Features

```bash
# Build with Datadog support
cargo build --release --features datadog

# Or use the Docker image
docker build -f docker/Dockerfile.datadog -t redis-rust:datadog .
```

## Metrics (DogStatsD)

| Metric | Type | Description |
|--------|------|-------------|
| `redis_rust.command.duration` | histogram | Command latency (ms) |
| `redis_rust.command.count` | counter | Command throughput |
| `redis_rust.connections.active` | gauge | Active connections |
| `redis_rust.ttl.evictions` | counter | Keys evicted by TTL |

## Tracing (APM)

- Distributed tracing with OpenTelemetry + Datadog exporter
- Automatic span creation for connections and commands
- Trace correlation in JSON logs

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `DD_SERVICE` | `redis-rust` | Service name |
| `DD_ENV` | `development` | Environment tag |
| `DD_VERSION` | pkg version | Service version |
| `DD_DOGSTATSD_URL` | `127.0.0.1:8125` | DogStatsD endpoint |
| `DD_TRACE_AGENT_URL` | `http://127.0.0.1:8126` | APM agent endpoint |
| `DD_TRACE_SAMPLE_RATE` | `1.0` | Trace sampling (0.0-1.0) |

## Docker Compose with Datadog Agent

```bash
# Start full stack with Datadog agent
docker-compose -f docker/docker-compose.datadog.yml up
```

## DST-Compatible Metrics

The observability module provides a `MetricsRecorder` trait for simulation testing:

```rust
use redis_sim::observability::{MetricsRecorder, SimulatedMetrics};

// In tests: use simulated metrics
let metrics = SimulatedMetrics::new();
metrics.record_command("GET", 1.0, true);
assert_eq!(metrics.command_count(), 1);

// In production: use real Datadog metrics
let metrics = Metrics::new(&DatadogConfig::from_env());
```
