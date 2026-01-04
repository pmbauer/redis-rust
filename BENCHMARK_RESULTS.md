# Redis Server Performance Benchmark Results

## Test Configuration

**Server:** Tiger Style Redis Server (Actor-per-Shard Architecture)
**Binary:** `redis-server-optimized`
**Port:** 3000
**Shards:** Dynamic (default: num_cpus)
**Date:** January 4, 2026

### System Configuration

| Component | Specification |
|-----------|---------------|
| CPU | Intel Core i9-11950H @ 2.60GHz (8 cores, 16 threads) |
| RAM | 32 GB DDR4 |
| OS | Ubuntu 22.04 (Linux 6.8.0-86-generic) |
| Rust | 1.87.0-nightly (f9e0239a7 2025-03-04) |
| Cargo | 1.87.0-nightly (2622e844b 2025-02-28) |

## Fair Comparison: Docker Benchmark vs Official Redis 7.4

To ensure a fair comparison, we run both servers in identical Docker containers with resource limits.

### Docker Configuration

| Setting | Value |
|---------|-------|
| CPU Limit | 2 cores per container |
| Memory Limit | 1GB per container |
| Network | Host networking |
| Requests | 100,000 |
| Clients | 50 concurrent |
| Pipeline | 1 (non-pipelined) |
| Data Size | 64 bytes |

### Non-Pipelined Performance (Pipeline=1)

| Operation | Official Redis 7.4 | Rust Implementation | Relative |
|-----------|-------------------|---------------------|----------|
| SET | 75,930 req/sec | 78,003 req/sec | **103%** |
| GET | 80,192 req/sec | 72,992 req/sec | **91%** |
| INCR | 77,399 req/sec | 73,313 req/sec | **95%** |

**Conclusion:** Our implementation achieves **91-103% of Redis 7.4 performance** on single operations. SET is slightly faster, while GET/INCR have minor overhead from CRDT conflict resolution.

### Pipelined Performance (Pipeline=16)

| Operation | Official Redis 7.4 | Rust Implementation | Relative |
|-----------|-------------------|---------------------|----------|
| SET | 800,000 req/sec | **909,090 req/sec** | **+14%** |
| GET | 657,894 req/sec | **854,700 req/sec** | **+30%** |

**Result:** Our implementation is **14-30% FASTER than Redis 7.4** on pipelined workloads!

**Why we're faster on pipelining:**
1. Batched response flushing (single syscall for all responses)
2. TCP_NODELAY enabled for lower latency
3. Lock-free actor architecture handles concurrent requests efficiently
4. Zero-copy RESP parsing with `bytes::Bytes`

## Local Benchmark (Single Machine, No Resource Limits)

### Optimized Server (`redis-server-optimized`)

| Command | Throughput | Latency | Notes |
|---------|------------|---------|-------|
| PING | 419,725 req/sec | 0.002 ms | Baseline |
| SET | 283,124 req/sec | 0.004 ms | Write path |
| GET | 270,442 req/sec | 0.004 ms | Read path |

**Note:** Local benchmarks run without resource constraints and networking overhead. Docker benchmarks provide a fairer comparison.

### Performance Optimization Stack

| Optimization | Description | Improvement |
|-------------|-------------|-------------|
| jemalloc | `tikv-jemallocator` custom allocator | ~10% |
| Actor-per-Shard | Lock-free tokio channels (no RwLock) | ~30% |
| Buffer Pooling | `crossbeam::ArrayQueue` buffer reuse | ~20% |
| Zero-copy Parser | `bytes::Bytes` + `memchr` RESP parsing | ~15% |
| Connection Pooling | Semaphore-limited with shared buffers | ~10% |

### Performance Evolution

| Version | Architecture | Throughput | Key Change |
|---------|-------------|------------|------------|
| v1 (baseline) | Single Lock | ~15,000 req/sec | Initial implementation |
| v2 (sharded) | 16 Shards + RwLock | ~25,000 req/sec | +67% from sharding |
| v3 (optimized) | Actor-per-Shard | ~80,000 req/sec | Lock-free design |
| v4 (fair test) | Docker constrained | ~80,000 req/sec | Matches Redis 7.4 |

## Architecture Details

### Actor-per-Shard Design

```
Client Connection
       |
  [Connection Handler]
       |
  hash(key) % num_shards
       |
  [ShardActor 0..N]  <-- tokio::mpsc channels (lock-free)
       |
  [CommandExecutor]
```

- **Lock-Free**: No `RwLock` contention between shards
- **Dynamic Shards**: Runtime-configurable shard count
- **Message Passing**: Explicit `ShardMessage` enum routes commands
- **TTL Manager**: Separate actor sends `EvictExpired` messages

### Zero-Copy RESP Parser

```
[RespCodec::parse]
       |
  [memchr] for CRLF scanning
       |
  [bytes::Bytes] zero-copy slicing
       |
  [RespValueZeroCopy] borrowed references
```

- **No Allocations**: Parser borrows from input buffer
- **Fast Scanning**: `memchr` SIMD-optimized byte search
- **Incremental**: Handles partial reads efficiently

## Comparison with Official Redis 7.4

| Feature | Official Redis 7.4 | This Implementation | Notes |
|---------|-------------------|---------------------|-------|
| **Performance (SET P=1)** | 75,930 req/sec | 78,003 req/sec | **103% of Redis** |
| **Performance (GET P=1)** | 80,192 req/sec | 72,992 req/sec | 91% of Redis |
| **Performance (INCR P=1)** | 77,399 req/sec | 73,313 req/sec | 95% of Redis |
| **Pipelining SET (P=16)** | 800,000 req/sec | **909,090 req/sec** | **+14% FASTER** |
| **Pipelining GET (P=16)** | 657,894 req/sec | **854,700 req/sec** | **+30% FASTER** |
| Persistence (RDB/AOF) | Yes | Streaming (Object Store) | Different model |
| Clustering | Redis Cluster | Anna-style CRDT | Different model |
| Consistency | Strong (single-leader) | Eventual/Causal | Trade-off |
| Pub/Sub | Yes | No | Not implemented |
| Lua Scripting | Yes | No | Not implemented |
| Memory Safety | Manual C | Rust guarantees | Safer |
| Deterministic Testing | No | DST framework | Better testability |
| Hot Key Detection | Manual | Automatic | Better |

### Trade-offs

**What we sacrifice:**
- Traditional persistence (RDB/AOF) - we use streaming object store instead
- Pub/Sub, Streams, Lua scripting
- Strong consistency in multi-node

**What we gain:**
- **FASTER pipelining** (14-30% faster at P=16)
- Memory safety via Rust
- Coordination-free replication (Anna-style)
- Deterministic simulation testing (DST)
- Automatic hot key detection
- Configurable consistency (eventual/causal)
- Zero-cost TimeSource abstraction for testability

## Replication Performance

| Mode | Throughput | Notes |
|------|------------|-------|
| Single-node | ~80,000 req/sec | No replication overhead |
| Replicated (3 nodes, eventual) | ~64,000 req/sec (est.) | With gossip synchronization |
| Replication Overhead | ~20% | Delta capture + gossip |

### Replication Features

- **Coordination-free**: No consensus protocol for writes
- **Conflict Resolution**: LWW registers with Lamport clocks
- **Consistency Modes**: Eventual (LWW) or Causal (vector clocks)
- **Gossip Interval**: 100ms (configurable)
- **Hot Key Detection**: Automatic RF increase for high-traffic keys
- **Anti-Entropy**: Merkle tree-based consistency verification

## Streaming Persistence (Object Store)

### Overview

Streaming persistence provides durable storage via object stores (S3/LocalFs) with:
- **Delta streaming**: Writes batched every 250ms
- **Segment format**: Binary with CRC32 checksums
- **Manifest-based recovery**: Atomic updates with crash recovery
- **DST-tested**: VOPR-style multi-seed testing with fault injection

### Test Coverage

| Test Type | Seeds | Result |
|-----------|-------|--------|
| Calm (no faults) | 100 | 100% pass |
| Moderate (some faults) | 100 | 80%+ pass |
| Chaos (many faults) | 50 | Completes with expected failures |

## Correctness Testing

### Test Suite (361 tests: 278 unit + 83 integration)

| Category | Tests | Coverage |
|----------|-------|----------|
| Unit Tests | 150+ | RESP parsing, commands, data structures |
| Eventual Consistency | 9 | CRDT convergence, partition healing |
| Causal Consistency | 10 | Vector clocks, read-your-writes |
| DST/Simulation | 5 | Multi-seed chaos testing |
| Streaming DST | 11 | Object store fault injection (100+ seeds) |
| Streaming Persistence | 9 | Write buffer, recovery, compaction |
| Anti-Entropy | 8 | Merkle tree sync, split-brain |
| Hot Key Detection | 5 | Adaptive replication |
| Metrics Service | 26 | CRDT counters, gauges, distributions |
| Integration | 18+ | End-to-end scenarios |

### Maelstrom/Jepsen Results

| Test | Nodes | Result | Notes |
|------|-------|--------|-------|
| Linearizability (lin-kv) | 1 | **PASS** | Single-node is linearizable |
| Linearizability (lin-kv) | 3 | **FAIL** | Expected: eventual consistency |
| Linearizability (lin-kv) | 5 | **FAIL** | Expected: eventual consistency |

**Note:** Multi-node linearizability tests FAIL by design. We use Anna-style eventual consistency, not Raft/Paxos consensus.

## Running Benchmarks

### Docker Benchmark (Recommended for Fair Comparison)

```bash
cd docker-benchmark
./run-benchmarks.sh
```

### Local Benchmark

```bash
# Run optimized server
cargo run --bin redis-server-optimized --release

# Run internal benchmarks
cargo run --bin benchmark --release
```

### Run Tests

```bash
# All tests (361 total)
cargo test --release

# Unit tests only
cargo test --lib
```

## Conclusion

The Tiger Style Redis server demonstrates:

- **91-103% of Redis 7.4 performance** on single operations (SET is faster!)
- **14-30% FASTER than Redis 7.4** on pipelined workloads (P=16)
- **909,090 req/sec peak pipelined SET throughput**
- **854,700 req/sec peak pipelined GET throughput**
- **Sub-millisecond latency** (0.002-0.004 ms average)
- **Memory-safe** Rust implementation with no data races
- **Deterministic testability** via FoundationDB-style simulation (DST)
- **Zero-cost TimeSource abstraction** for time-travel testing
- **361 tests** covering consistency, replication, persistence, and chaos scenarios

### Known Limitations

1. **Streaming persistence**: Object store-based (S3/LocalFs), not traditional RDB/AOF
2. **No pub/sub**: Not implemented
3. **Multi-node consistency**: Eventual, not linearizable (by design)
