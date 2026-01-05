# Redis Cache - Production Server + Deterministic Simulator

A production-ready, actor-based Redis cache server in Rust with distributed replication capabilities. Features both a deterministic simulator (FoundationDB/TigerBeetle-style testing) and a production server using Tiger Style principles with actor-based sharded architecture.

## Features

- **Production Redis Server**: Compatible with `redis-cli` and all Redis clients
- **Tiger Style Engineering**: Explicit over implicit, assertion-heavy, deterministic behavior
- **50+ Redis Commands**: Full caching feature set (strings, lists, sets, hashes, sorted sets)
- **Dynamic Shard Architecture**: Runtime-configurable shards with lock-free message passing
- **Anna KVS-Style Replication**: Configurable consistency (eventual, causal), coordination-free
- **Hot Key Detection**: Adaptive replication for high-traffic keys
- **Deterministic Simulation**: FoundationDB/TigerBeetle-style testing harness
- **Maelstrom/Jepsen Integration**: Formal linearizability testing (single-node verified)
- **Datadog Observability**: Optional metrics, tracing, and logging via feature flag

## Quick Start

```bash
# Run the optimized production server
cargo run --bin redis-server-optimized --release

# Connect with redis-cli
redis-cli -p 3000

# Run all tests (331+ tests)
cargo test --all

# Run benchmarks
cargo run --bin benchmark --release
```

## Performance vs Official Redis 7.4

Docker-based benchmark (2 CPUs, 1GB RAM per container, 50 clients, 100K requests):

### Non-Pipelined (Pipeline=1)

| Operation | Official Redis 7.4 | Rust Implementation | Relative |
|-----------|-------------------|---------------------|----------|
| SET | 77,882 req/sec | 74,963 req/sec | **96%** |
| GET | 79,618 req/sec | 74,239 req/sec | **93%** |

### Pipelined (Pipeline=16)

| Operation | Official Redis 7.4 | Rust Implementation | Relative |
|-----------|-------------------|---------------------|----------|
| SET | 763,359 req/sec | **1,020,408 req/sec** | **+34%** |
| GET | 854,701 req/sec | **980,392 req/sec** | **+15%** |

Our implementation achieves **~93-96%** of Redis on single operations and **15-34% FASTER** on pipelined workloads!

See [BENCHMARK_RESULTS.md](BENCHMARK_RESULTS.md) for full details and methodology.

## Architecture

### Production Server (Tiger Style)

```
Client Connections
        |
   [Tokio Runtime]
        |
   [Dynamic Shard Actors] <-- Runtime configurable count
        |
   [ShardMessage enum: Command | EvictExpired]
        |
   [CommandExecutor per shard]
```

- **Actor-per-Shard**: Each shard runs as independent actor with message passing
- **Lock-Free**: No RwLock, uses tokio channels for all shard communication
- **Dynamic Sharding**: Runtime-configurable shard count (default: num_cpus)
- **TTL Manager Actor**: Background actor sends eviction messages every 100ms
- **Tiger Style**: `debug_assert!` invariants, explicit error handling, no silent failures

### Performance Optimizations

- **jemalloc Allocator**: ~10% reduced memory fragmentation
- **Actor-per-Shard**: ~30% improvement from lock-free design
- **Buffer Pooling**: ~20% improvement from `crossbeam::ArrayQueue` reuse
- **Zero-copy RESP Parser**: ~15% improvement with `bytes::Bytes` + `memchr`
- **Connection Pooling**: ~10% improvement from semaphore-limited connections

### Anna KVS-Style Replication

```
Node 1                    Node 2                    Node 3
  |                         |                         |
[LWW Register]  <--Gossip-->  [LWW Register]  <--Gossip-->  [LWW Register]
  |                         |                         |
[Lamport Clock]           [Lamport Clock]           [Lamport Clock]
  |                         |                         |
[Vector Clock]            [Vector Clock]            [Vector Clock]
```

- **Configurable Consistency**: `Eventual` (LWW) or `Causal` (vector clocks)
- **CRDT-Based**: Last-Writer-Wins registers with Lamport clocks for total ordering
- **Vector Clocks**: Causal consistency mode tracks happens-before relationships
- **Gossip Protocol**: Periodic state synchronization between nodes
- **Hot Key Detection**: Automatic increased replication for high-traffic keys
- **Anti-Entropy**: Merkle tree-based consistency verification and repair

### Consistency Guarantees

| Mode | Single-Node | Multi-Node |
|------|-------------|------------|
| Linearizable | Yes (verified via Maelstrom) | No |
| Eventual | Yes | Yes (CRDT convergence verified) |
| Causal | Yes | Yes (vector clock verified) |

**Important**: Multi-node mode provides **eventual consistency**, not linearizability. This is by design, following Anna KVS architecture for coordination-free scalability.

## Testing

### Test Suite (331+ tests total)

```bash
# Unit tests
cargo test --lib

# All tests including integration
cargo test --all
```

**Test Categories:**
- **Unit Tests** (150+): Core Redis commands, RESP parsing, data structures, VOPR invariants
- **Eventual Consistency** (9): CRDT convergence, partition healing
- **Causal Consistency** (10): Vector clocks, read-your-writes, happens-before
- **CRDT DST** (15): Multi-seed CRDT convergence testing (100+ seeds)
- **DST/Simulation** (5): Multi-seed chaos testing
- **Streaming DST** (11): Object store fault injection, 100+ seeds
- **Streaming Persistence** (9): Write buffer, recovery, compaction
- **Anti-Entropy** (8): Merkle tree sync, split-brain recovery
- **Hot Key Detection** (5): Adaptive replication, Zipfian workloads

### Maelstrom/Jepsen Tests

```bash
# Single-node linearizability (PASSES)
./maelstrom/maelstrom/maelstrom test -w lin-kv --bin ./target/release/maelstrom-kv-replicated \
    --time-limit 30 --concurrency 10

# Multi-node eventual consistency
./maelstrom/maelstrom/maelstrom test -w lin-kv --bin ./target/release/maelstrom-kv-replicated \
    --node-count 3 --time-limit 30 --concurrency 10
```

**Note**: Multi-node linearizability test will FAIL because we use eventual consistency. This is expected behavior.

## Supported Commands

### Strings
`GET`, `SET`, `SETEX`, `SETNX`, `MGET`, `MSET`, `APPEND`, `GETSET`, `STRLEN`

### Counters
`INCR`, `DECR`, `INCRBY`, `DECRBY`

### Expiration
`EXPIRE`, `EXPIREAT`, `PEXPIREAT`, `TTL`, `PTTL`, `PERSIST`

### Keys
`DEL`, `EXISTS`, `TYPE`, `KEYS`, `FLUSHDB`, `FLUSHALL`

### Lists
`LPUSH`, `RPUSH`, `LPOP`, `RPOP`, `LLEN`, `LRANGE`, `LINDEX`

### Sets
`SADD`, `SREM`, `SMEMBERS`, `SISMEMBER`, `SCARD`

### Hashes
`HSET`, `HGET`, `HDEL`, `HGETALL`, `HKEYS`, `HVALS`, `HLEN`, `HEXISTS`, `HINCRBY`

### Sorted Sets
`ZADD`, `ZREM`, `ZSCORE`, `ZRANK`, `ZRANGE`, `ZREVRANGE`, `ZCARD`

### Server
`PING`, `INFO`

## Comparison with Official Redis

| Feature | Official Redis 7.4 | This Implementation |
|---------|-------------------|---------------------|
| Performance (SET/GET) | Baseline | ~93-96% (single) / +15-34% (pipelined) |
| Pipelining | Full support | **15-34% FASTER** |
| Persistence (RDB/AOF) | Yes | Yes (Streaming to Object Store) |
| Clustering | Redis Cluster | Anna-style CRDT |
| Consistency | Single-leader strong | Eventual/Causal |
| Pub/Sub | Yes | No |
| Lua Scripting | Yes | No |
| Streams | Yes | No |
| ACL/Auth | Yes | No |
| Hot Key Handling | Manual | Automatic detection |
| Deterministic Testing | No | Yes (DST framework) |
| Datadog Integration | Via plugin | Native (feature flag) |

**Trade-offs**: We sacrifice some Redis features (pub/sub, Lua) in favor of coordination-free replication, streaming persistence to object stores (S3/local), and deterministic simulation testing.

## Project Structure

```
src/
├── bin/
│   ├── redis_server_optimized.rs    # Optimized server binary
│   ├── benchmark.rs                 # Performance benchmarks
│   └── maelstrom_kv_replicated.rs   # Maelstrom/Jepsen binary
├── redis/
│   ├── commands.rs                  # CommandExecutor (shared core)
│   ├── resp.rs                      # RESP protocol parser
│   ├── resp_optimized.rs            # Zero-copy RESP parser
│   └── sds.rs                       # Simple Dynamic Strings
├── production/
│   ├── server_optimized.rs          # Tiger Style server
│   ├── sharded_actor.rs             # Dynamic shard actors
│   ├── connection_optimized.rs      # Zero-copy connection handler
│   ├── ttl_manager.rs               # TtlManagerActor
│   ├── connection_pool.rs           # Buffer pooling
│   ├── hotkey.rs                    # Hot key detection
│   ├── adaptive_replication.rs      # Per-key RF management
│   ├── load_balancer.rs             # Shard scaling
│   └── gossip_manager.rs            # Gossip protocol
├── replication/
│   ├── hash_ring.rs                 # Consistent hashing
│   ├── lattice.rs                   # CRDT primitives (LWW, GCounter, PNCounter)
│   ├── state.rs                     # Replica state management
│   └── gossip.rs                    # Gossip message types
├── simulator/
│   ├── harness.rs                   # SimulationHarness, ScenarioBuilder
│   ├── executor.rs                  # Event-driven execution
│   ├── time.rs                      # VirtualTime
│   ├── network.rs                   # Network fault simulation
│   └── rng.rs                       # DeterministicRng, buggify()
└── observability/                   # Datadog integration (optional)
    ├── config.rs                    # Environment configuration
    ├── metrics.rs                   # DogStatsD client
    ├── recorder.rs                  # DST-compatible MetricsRecorder trait
    ├── tracing_setup.rs             # OpenTelemetry + Datadog APM
    └── spans.rs                     # Span helpers
```

## Docker Benchmarking

Run fair comparison against official Redis:

```bash
cd docker-benchmark

# In-memory comparison (Redis vs Rust optimized)
./run-benchmarks.sh

# Persistent comparison (Redis AOF vs Rust S3/MinIO)
./run-persistent-benchmarks.sh
```

## Datadog Observability

Full observability support with optional `datadog` feature flag. Zero overhead when disabled.

### Enable Datadog Features

```bash
# Build with Datadog support
cargo build --release --features datadog

# Or use the Docker image
docker build -f docker/Dockerfile.datadog -t redis-rust:datadog .
```

### Metrics (DogStatsD)

| Metric | Type | Description |
|--------|------|-------------|
| `redis_rust.command.duration` | histogram | Command latency (ms) |
| `redis_rust.command.count` | counter | Command throughput |
| `redis_rust.connections.active` | gauge | Active connections |
| `redis_rust.ttl.evictions` | counter | Keys evicted by TTL |

### Tracing (APM)

- Distributed tracing with OpenTelemetry + Datadog exporter
- Automatic span creation for connections and commands
- Trace correlation in JSON logs

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `DD_SERVICE` | `redis-rust` | Service name |
| `DD_ENV` | `development` | Environment tag |
| `DD_VERSION` | pkg version | Service version |
| `DD_DOGSTATSD_URL` | `127.0.0.1:8125` | DogStatsD endpoint |
| `DD_TRACE_AGENT_URL` | `http://127.0.0.1:8126` | APM agent endpoint |
| `DD_TRACE_SAMPLE_RATE` | `1.0` | Trace sampling (0.0-1.0) |

### Docker Compose with Datadog Agent

```bash
# Start full stack with Datadog agent
docker-compose -f docker/docker-compose.datadog.yml up
```

### DST-Compatible Metrics

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

## Dependencies

### Core
- `tokio` - Async runtime
- `tikv-jemallocator` - Custom memory allocator
- `crossbeam` - Lock-free buffer pooling
- `bytes` / `memchr` - Zero-copy parsing
- `serde` / `serde_json` - Gossip protocol serialization
- `rand_chacha` - Deterministic PRNG for simulation
- `tracing` - Structured logging

### Datadog (optional, via `--features datadog`)
- `dogstatsd` - DogStatsD metrics client
- `opentelemetry` / `opentelemetry-datadog` - APM tracing
- `tracing-opentelemetry` - Tracing integration

## License

MIT
