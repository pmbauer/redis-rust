# Redis Cache - Production Server + Deterministic Simulator

A production-ready, actor-based Redis cache server in Rust with distributed replication capabilities. Features both a deterministic simulator (FoundationDB/TigerBeetle-style testing) and a production server using Tiger Style principles with actor-based sharded architecture.

## Features

- **Production Redis Server**: Compatible with `redis-cli` and all Redis clients
- **Tiger Style Engineering**: Explicit over implicit, assertion-heavy, deterministic behavior
- **35+ Redis Commands**: Full caching feature set (strings, lists, sets, hashes, sorted sets)
- **16-Shard Actor Architecture**: Lock-free message passing for parallel execution
- **Anna KVS Replication**: Coordination-free, eventually-consistent multi-node deployments
- **Deterministic Simulation**: FoundationDB/TigerBeetle-style testing harness
- **Maelstrom/Jepsen Integration**: Formal linearizability testing

## Quick Start

```bash
# Run the optimized production server
cargo run --bin redis-server-optimized --release

# Connect with redis-cli
redis-cli -p 3000

# Run all tests (22 tests)
cargo test --lib

# Run benchmarks
cargo run --bin benchmark --release
```

## Architecture

### Production Server (Tiger Style)

```
Client Connections
        |
   [Tokio Runtime]
        |
   [16 Shard Actors] ← Lock-free message passing
        |
   [ShardMessage enum: Command | EvictExpired]
        |
   [CommandExecutor per shard]
```

- **Actor-per-Shard**: Each shard runs as independent actor with message passing
- **Lock-Free**: No RwLock, uses tokio channels for all shard communication
- **Explicit Messages**: `ShardMessage::Command` and `ShardMessage::EvictExpired`
- **TTL Manager Actor**: Background actor sends eviction messages every 100ms
- **Tiger Style**: `debug_assert!` invariants, explicit error handling, no silent failures

### Performance Optimizations

- **jemalloc Allocator**: ~10% reduced memory fragmentation
- **Actor-per-Shard**: ~30% improvement from lock-free design
- **Buffer Pooling**: ~20% improvement from `crossbeam::ArrayQueue` reuse
- **Zero-copy RESP Parser**: ~15% improvement with `bytes::Bytes` + `memchr`
- **Connection Pooling**: ~10% improvement from semaphore-limited connections

### Deterministic Simulation (FDB/TigerBeetle Style)

```rust
// Same CommandExecutor runs in production and simulation
let harness = ScenarioBuilder::new(seed)
    .with_buggify(0.1)  // 10% chaos injection
    .at_time(0).client(1, Command::SetEx("key".into(), 1, SDS::from_str("value")))
    .at_time(1500).client(1, Command::Get("key".into()))
    .run_with_eviction(100);
```

- **SimulationHarness**: Wraps CommandExecutor with VirtualTime
- **ScenarioBuilder**: Ergonomic test authoring with time-based scheduling
- **BUGGIFY**: Probabilistic chaos injection for edge case discovery
- **History Recording**: Full invoke/complete log for linearizability checks
- **Deterministic Replay**: Same seed produces identical results

### Anna KVS-Style Replication

```
Node 1                    Node 2                    Node 3
  |                         |                         |
[LWW Register]  <--Gossip-->  [LWW Register]  <--Gossip-->  [LWW Register]
  |                         |                         |
[Lamport Clock]           [Lamport Clock]           [Lamport Clock]
```

- **CRDT-Based**: Last-Writer-Wins registers with Lamport clocks
- **Vector Clocks**: Optional causal consistency tracking
- **Gossip Protocol**: Periodic state synchronization between nodes

## Performance

| Operation | Throughput | Latency |
|-----------|------------|---------|
| PING | ~40,000 req/sec | 0.02 ms |
| SET | ~38,000 req/sec | 0.03 ms |
| GET | ~35,000 req/sec | 0.03 ms |
| INCR | ~38,000 req/sec | 0.03 ms |

Expected with optimizations enabled. See [BENCHMARK_RESULTS.md](BENCHMARK_RESULTS.md) for details.

## Testing

### Unit Tests (22 tests)
```bash
cargo test --lib
```

### Simulation Tests (8 tests)
- `test_basic_set_get` - Baseline operations
- `test_ttl_expiration_with_fast_forward` - Virtual time control
- `test_ttl_boundary_race` - Edge case at exact expiration time
- `test_concurrent_increments` - Multiple clients at same timestamp
- `test_deterministic_replay` - Identical results with same seed
- `test_buggify_chaos` - Probabilistic delay injection
- `test_persist_cancels_expiration` - PERSIST command behavior
- `test_multi_seed_invariants` - 100 seeds verify invariants hold

### Maelstrom/Jepsen Tests
```bash
./scripts/maelstrom_test.sh
```

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
`HSET`, `HGET`, `HDEL`, `HGETALL`, `HKEYS`, `HVALS`, `HLEN`, `HEXISTS`

### Sorted Sets
`ZADD`, `ZREM`, `ZSCORE`, `ZRANK`, `ZRANGE`, `ZCARD`

### Server
`PING`, `INFO`

## Project Structure

```
src/
├── bin/
│   ├── server.rs                    # Production server entry
│   ├── redis_server_optimized.rs    # Optimized server binary
│   ├── benchmark.rs                 # Performance benchmarks
│   └── maelstrom_kv.rs              # Maelstrom binary
├── redis/
│   ├── commands.rs                  # CommandExecutor (shared core)
│   ├── resp.rs                      # RESP protocol parser
│   ├── resp_optimized.rs            # Zero-copy RESP parser
│   └── sds.rs                       # Simple Dynamic Strings
├── production/
│   ├── server_optimized.rs          # Tiger Style server
│   ├── sharded_actor.rs             # Actor-per-shard with ShardMessage
│   ├── connection_optimized.rs      # Zero-copy connection handler
│   ├── ttl_manager.rs               # TtlManagerActor
│   ├── connection_pool.rs           # Buffer pooling
│   ├── replicated_state.rs          # Replicated sharded state
│   └── gossip_manager.rs            # Gossip protocol
├── replication/
│   ├── lattice.rs                   # CRDT primitives (LWW, VectorClock)
│   ├── state.rs                     # Replica state management
│   └── gossip.rs                    # Gossip message types
└── simulator/
    ├── harness.rs                   # SimulationHarness, ScenarioBuilder
    ├── executor.rs                  # Event-driven execution
    ├── time.rs                      # VirtualTime
    ├── network.rs                   # Network fault simulation
    └── rng.rs                       # DeterministicRng, buggify()
```

## Dependencies

- `tokio` - Async runtime
- `tikv-jemallocator` - Custom memory allocator
- `crossbeam` - Lock-free buffer pooling
- `bytes` / `memchr` - Zero-copy parsing
- `serde` / `serde_json` - Gossip protocol serialization
- `rand_chacha` - Deterministic PRNG for simulation
- `tracing` - Structured logging

## License

MIT
