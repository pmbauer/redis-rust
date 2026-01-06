# Redis Cache - Deterministic Simulator + Experimental Server

An experimental, actor-based Redis-compatible cache server in Rust with distributed replication capabilities. Features both a deterministic simulator (FoundationDB/TigerBeetle-style testing) and an experimental server using Tiger Style principles with actor-based sharded architecture.

> **Status**: Research project with production-oriented architecture and correctness tooling. Not yet a production Redis replacement. See [Redis Compatibility](#redis-compatibility) for semantic differences.

> **Security Warning**: This server has **no authentication or access control**. Do NOT expose to untrusted networks or the public internet. Bind to localhost or use network-level access control (firewall, VPC).

## Features

- **Redis-Compatible Server**: Compatible with `redis-cli` and all Redis clients (RESP2 protocol)
- **Tiger Style Engineering**: Explicit over implicit, assertion-heavy, deterministic behavior
- **50+ Redis Commands**: Full caching feature set (strings, lists, sets, hashes, sorted sets)
- **Dynamic Shard Architecture**: Runtime-configurable shards with lock-free message passing
- **Anna KVS-Style Replication**: Configurable consistency (eventual, causal), coordination-free
- **Hot Key Detection**: Adaptive replication for high-traffic keys
- **Deterministic Simulation**: FoundationDB/TigerBeetle-style testing harness
- **Redis Equivalence Testing**: Differential testing against real Redis (27 commands verified)
- **Zipfian Workload Simulation**: Realistic hot/cold key access patterns
- **Maelstrom/Jepsen Integration**: Formal linearizability testing (single-node verified)
- **Datadog Observability**: Optional metrics, tracing, and logging via feature flag

## Quick Start

```bash
# Run the optimized production server
cargo run --bin redis-server-optimized --release

# Connect with redis-cli
redis-cli -p 3000

# Run all tests (480+ tests)
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

### Test Suite (480+ tests total)

```bash
# Unit tests
cargo test --lib

# All tests including integration
cargo test --all

# Redis equivalence testing (requires real Redis running on 6379)
cargo test redis_equivalence --release -- --ignored
```

**Test Categories:**
- **Unit Tests** (372+): Core Redis commands, RESP parsing, data structures, VOPR invariants
- **Redis Equivalence** (27 commands): Differential testing against real Redis
- **Eventual Consistency** (9): CRDT convergence, partition healing
- **Causal Consistency** (10): Vector clocks, read-your-writes, happens-before
- **CRDT DST** (15): Multi-seed CRDT convergence testing (100+ seeds)
- **DST/Simulation** (5): Multi-seed chaos testing with Zipfian key distribution
- **Streaming DST** (11): Object store fault injection, 100+ seeds
- **Streaming Persistence** (9): Write buffer, recovery, compaction
- **Anti-Entropy** (8): Merkle tree sync, split-brain recovery
- **Hot Key Detection** (5): Adaptive replication, Zipfian workloads

### Redis Equivalence Testing

Differential testing compares our implementation against real Redis to ensure identical behavior:

```bash
# Start real Redis on default port
docker run -d -p 6379:6379 redis:7-alpine

# Start rust implementation
REDIS_PORT=3000 cargo run --bin redis-server-optimized --release &

# Run equivalence tests
cargo test redis_equivalence --release -- --ignored
```

**27 core commands verified identical** including:
- String operations (GET, SET, APPEND, STRLEN, GETRANGE)
- Numeric operations (INCR, DECR, INCRBY, DECRBY)
- Hash operations (HSET, HGET, HDEL, HGETALL, HINCRBY)
- List operations (LPUSH, RPUSH, LPOP, RPOP, LRANGE, LLEN)
- Set operations (SADD, SREM, SMEMBERS, SISMEMBER, SCARD)
- Sorted set operations (ZADD, ZSCORE, ZRANK, ZRANGE, ZCARD)
- Key operations (DEL, EXISTS, TYPE, EXPIRE, TTL)

### Zipfian Workload Simulation

DST tests use Zipfian distribution for realistic hot/cold key access patterns:

```rust
// Top 10 keys receive ~40% of accesses (skew=1.0)
KeyDistribution::Zipfian { num_keys: 100, skew: 1.0 }
```

This simulates real-world workloads where a small subset of keys receive disproportionate traffic.

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

## Redis Compatibility

### Wire Protocol
| Protocol | Status |
|----------|--------|
| **RESP2** | Full support (compatible with all Redis clients) |
| **RESP3** | Not supported |

### Semantic Differences from Redis

This implementation intentionally differs from Redis in several ways:

| Behavior | Redis | This Implementation | Rationale |
|----------|-------|---------------------|-----------|
| **Multi-node Consistency** | Single-leader strong | Eventual/Causal (CRDT) | Coordination-free scalability |
| **Transactions** | MULTI/EXEC atomic | Not supported | Conflicts with CRDT model |
| **Keyspace Notifications** | Supported | Not supported | Not implemented |
| **Eviction Policies** | LRU/LFU/Random/TTL | TTL-only | Simpler model |
| **Memory Limits** | maxmemory + eviction | No memory limits | Not implemented |
| **Persistence Model** | RDB snapshots / AOF log | Streaming to object store (S3) | Cloud-native design |
| **Cluster Protocol** | Redis Cluster (hash slots) | Anna-style CRDT gossip | Different architecture |
| **Blocking Operations** | BLPOP, BRPOP, etc. | Not supported | Not implemented |

### Not Implemented (No Plans)
These features conflict with the CRDT/eventual consistency architecture:
- **Transactions**: MULTI, EXEC, WATCH, DISCARD
- **Blocking operations**: BLPOP, BRPOP, BLMOVE, etc.
- **Cluster commands**: CLUSTER *, READONLY, READWRITE

### Not Implemented (Roadmap)
These could be added without architectural changes:
- **Pub/Sub**: PUBLISH, SUBSCRIBE, PSUBSCRIBE
- **Lua scripting**: EVAL, EVALSHA, SCRIPT
- **Streams**: XADD, XREAD, XRANGE, XGROUP
- **Authentication**: AUTH, ACL commands
- **TLS**: Encrypted connections

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
| Differential Testing | N/A | Yes (27 commands vs Redis) |
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
├── observability/                   # Datadog integration (optional)
│   ├── config.rs                    # Environment configuration
│   ├── metrics.rs                   # DogStatsD client
│   ├── recorder.rs                  # DST-compatible MetricsRecorder trait
│   ├── tracing_setup.rs             # OpenTelemetry + Datadog APM
│   └── spans.rs                     # Span helpers
└── simulator/
    ├── dst_integration.rs           # DST with Zipfian distribution
    └── ...

tests/
└── redis_equivalence_test.rs        # Differential testing vs real Redis
```

## Docker Benchmarking

Run fair comparison against official Redis:

```bash
cd docker-benchmark

# In-memory comparison (Redis vs Rust optimized)
./run-benchmarks.sh

# Redis 8.0 comparison (three-way: Redis 7.4 vs Redis 8.0 vs Rust)
./run-redis8-comparison.sh

# Persistent comparison (Redis AOF vs Rust S3/MinIO)
./run-persistent-benchmarks.sh
```

## RedisEvolve - Automatic Performance Tuning

RedisEvolve is an evolutionary optimization harness that automatically discovers optimal configuration parameters by running benchmarks and evolving towards Redis 8.0 performance.

### How It Works

```
┌─────────────────────────────────────────────────────────────────┐
│                    Python Evolution Engine                       │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐        │
│  │ Generate │→ │ Evaluate │→ │  Select  │→ │  Evolve  │ → loop │
│  │ Population│  │ Fitness  │  │   Best   │  │ (mutate) │        │
│  └──────────┘  └──────────┘  └──────────┘  └──────────┘        │
└─────────────────────────────────────────────────────────────────┘
```

1. **Generate** population of config candidates (shards, buffer sizes, batch thresholds)
2. **Evaluate** each candidate: write TOML → rebuild Docker → run redis-benchmark → compute fitness
3. **Select** best performers, **evolve** via crossover + mutation
4. **Repeat** for N generations until convergence

### Running Evolution

```bash
# Quick test with mock evaluator (no Docker)
python -m evolve.harness --mock --generations 3 --population 6

# Full evolution with real benchmarks (~8-10 hours)
python -m evolve.harness --generations 10 --population 8
```

### Results

After 10 generations (62 candidates evaluated), RedisEvolve discovered:

| Parameter | Default | Optimized | Impact |
|-----------|---------|-----------|--------|
| num_shards | 16 | **4** | Less contention |
| response_pool.capacity | 256 | **512** | More pooled responses |
| response_pool.prewarm | 64 | **96** | Better warm start |
| buffers.read_size | 8192 | 8192 | Already optimal |
| batching.batch_threshold | 2 | **6** | More aggressive batching |

**Result: 99.1% of Redis 8.0 performance** (up from ~88% with defaults)

Key insight: Fewer shards (4 vs 16) reduces contention and improves throughput.

### Configuration

The optimized config is saved to `evolve/results/best_config.toml`. To use it:

```bash
# Copy to production config
cp evolve/results/best_config.toml perf_config.toml

# Server will automatically load it on startup
cargo run --bin redis-server-optimized --release
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

## Security

### Current Limitations
- **No Authentication**: No AUTH command, no password protection
- **No ACL**: No user-based access control
- **No TLS**: No encrypted connections
- **No Command Restrictions**: All commands available to all clients

### Deployment Requirements
1. **Never expose to public internet** - This is critical
2. Bind to `127.0.0.1` or private network interfaces only
3. Use network-level access control (iptables, security groups, VPC)
4. Run in isolated container/namespace
5. Monitor for unauthorized access attempts

### Security Roadmap
- [ ] AUTH command support
- [ ] TLS encryption
- [ ] Basic ACL (user-based permissions)
- [ ] Command allowlist/denylist

## License

MIT
