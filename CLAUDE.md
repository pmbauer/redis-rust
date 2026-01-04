# Claude Code Guidelines for redis-rust

## Project Philosophy

This project follows **Simulation-First Development** inspired by FoundationDB and TigerBeetle. The core principle: **if you can't simulate it, you can't test it properly**.

## Architecture Principles

### 1. Deterministic Simulation Testing (DST)

All I/O operations must go through abstractions that can be:
- **Simulated**: Deterministic, controllable behavior
- **Fault-injected**: Network partitions, disk failures, message drops
- **Time-controlled**: Fast-forward time, test timeout scenarios

```rust
// GOOD: I/O through trait abstraction
trait ObjectStore: Send + Sync {
    async fn put(&self, key: &str, data: &[u8]) -> Result<()>;
    async fn get(&self, key: &str) -> Result<Vec<u8>>;
}

// BAD: Direct I/O that can't be simulated
std::fs::write(path, data)?;
```

### 2. Actor Architecture

Components communicate via message passing, not shared mutable state:

```rust
// GOOD: Actor owns state exclusively
struct PersistenceActor {
    state: PersistenceState,  // Owned, not shared
    rx: mpsc::Receiver<Message>,
}

// BAD: Shared state with mutex
struct SharedPersistence {
    state: Arc<Mutex<PersistenceState>>,  // Contention, hard to test
}
```

### 3. TigerStyle Coding

- **Assertions**: Use `debug_assert!` liberally for invariants
- **No hidden allocations**: Be explicit about where memory is allocated
- **Simple control flow**: Prefer early returns, avoid deep nesting
- **Explicit errors**: No panics in production paths, explicit `Result<T, E>`

### 4. Static Stability

Systems must remain stable under partial failures:
- Graceful degradation when dependencies fail
- Bounded queues with backpressure
- Timeouts on all external operations

## Testing Strategy

### Unit Tests
- Test pure logic without I/O
- Use `InMemoryObjectStore` for storage tests

### Simulation Tests (DST)
- Use `SimulatedObjectStore` with fault injection
- Control time via `SimulatedClock`
- Run thousands of seeds to find edge cases

### Linearizability Tests (Jepsen-style)
- Use Maelstrom for distributed correctness
- Test under network partitions
- Verify consistency guarantees

## Key Files

| File | Purpose |
|------|---------|
| `src/io/mod.rs` | I/O abstractions (Clock, Network, RNG) |
| `src/simulator/` | DST harness and fault injection |
| `src/streaming/simulated_store.rs` | Fault-injectable object store |
| `src/buggify/` | Probabilistic fault injection |

## Common Patterns

### Creating Testable Components

```rust
// 1. Define trait for the I/O operation
pub trait ObjectStore: Send + Sync + 'static {
    fn put(&self, key: &str, data: &[u8]) -> impl Future<Output = IoResult<()>>;
}

// 2. Create production implementation
pub struct LocalFsObjectStore { path: PathBuf }

// 3. Create simulated implementation with fault injection
pub struct SimulatedObjectStore {
    inner: InMemoryObjectStore,
    fault_config: FaultConfig,
    rng: SimulatedRng,
}

// 4. Use generic in component
pub struct StreamingPersistence<S: ObjectStore> {
    store: S,
    // ...
}
```

### Actor Shutdown Pattern

```rust
enum Message {
    DoWork(Work),
    Shutdown { response: oneshot::Sender<()> },
}

async fn run(mut self) {
    while let Some(msg) = self.rx.recv().await {
        match msg {
            Message::DoWork(w) => self.handle_work(w).await,
            Message::Shutdown { response } => {
                self.cleanup().await;
                let _ = response.send(());
                break;
            }
        }
    }
}
```

### Bridging Sync to Async

When you need to connect sync code (like command execution) to async actors:

```rust
// Use std::sync::mpsc for fire-and-forget from sync context
let (tx, rx) = std::sync::mpsc::channel();

// Bridge task drains sync channel into async actor
async fn bridge(rx: Receiver<Delta>, actor: ActorHandle) {
    loop {
        // Use recv_timeout to stay responsive to shutdown
        if let Some(delta) = rx.recv_timeout(Duration::from_millis(50)) {
            actor.send(delta);
        }
        if shutdown.load(Ordering::SeqCst) {
            break;
        }
    }
}
```

## Debugging Tips

1. **Seed-based reproduction**: All simulations use seeds. Save failing seeds to reproduce bugs.
2. **Trace logging**: Use `tracing` crate with structured logging.
3. **Invariant checks**: Add assertions for state invariants after each operation.

## Performance Guidelines

1. **Batch operations**: Accumulate deltas in write buffer before flushing
2. **Async I/O**: Use `tokio::spawn` for background operations
3. **Zero-copy where possible**: Use `Bytes` for large data transfers
4. **Profile before optimizing**: Use `cargo flamegraph`

## Benchmarking Requirements

**All performance comparisons MUST use Docker containers** to ensure fair, reproducible results.

### Why Docker-Only Benchmarks?

| Concern | Docker Solution |
|---------|-----------------|
| Resource fairness | Both servers get identical CPU/memory limits |
| Environment consistency | Same OS, kernel, networking stack |
| Reproducibility | Anyone can run identical benchmarks |
| Apples-to-apples | Eliminates "my machine is faster" bias |

### Docker Benchmark Configuration

Location: `docker-benchmark/`

| Setting | Value | Rationale |
|---------|-------|-----------|
| CPU Limit | 2 cores | Realistic server constraint |
| Memory Limit | 1GB | Realistic server constraint |
| Network | Host networking | Eliminates Docker NAT overhead |
| Requests | 100,000 | Statistically significant |
| Clients | 50 concurrent | Realistic load |
| Pipeline depths | 1, 16, 64 | Tests different access patterns |

### Running Docker Benchmarks

```bash
# REQUIRED for any performance claims
cd docker-benchmark
./run-benchmarks.sh

# This runs:
# 1. Official Redis 7.4 (port 6379)
# 2. Rust implementation (port 3000)
# 3. Both non-pipelined and pipelined tests
```

### What NOT to Use for Comparisons

```bash
# DON'T use these for Redis vs Rust comparisons:
cargo run --release --bin quick_benchmark  # Local only, no Redis comparison
cargo run --release --bin benchmark        # Local only, unfair conditions
```

The local benchmarks (`quick_benchmark`, `benchmark`) are useful for:
- Quick smoke tests during development
- Profiling and optimization work
- Regression detection between commits

But they MUST NOT be used for Redis vs Rust performance claims in documentation.

### Updating BENCHMARK_RESULTS.md

When updating performance numbers:

1. **Always run Docker benchmarks** - never use local numbers for comparisons
2. **Run multiple times** - take median of 3 runs to reduce variance
3. **Document the environment** - include Docker version, host OS
4. **Include all pipeline depths** - P=1, P=16, P=64 tell different stories

```bash
# Example workflow for updating benchmarks
cd docker-benchmark
for i in 1 2 3; do
    echo "=== Run $i ==="
    ./run-benchmarks.sh 2>&1 | tee run_$i.log
done
# Use median results in BENCHMARK_RESULTS.md
```

## Commit Requirements

**Every commit must be fully tested and documented:**

### Pre-Commit Checklist

1. **Full Test Suite**: Run `cargo test --release` - all tests must pass
2. **DST Tests**: Run streaming and simulator DST tests with multiple seeds
3. **Local Smoke Test**: Run `cargo run --release --bin quick_benchmark` for quick validation
4. **Docker Benchmarks**: Run `docker-benchmark/run-benchmarks.sh` if updating performance claims
5. **Documentation**: Update `BENCHMARK_RESULTS.md` with Docker benchmark results if metrics change

### Commit Workflow

```bash
# 1. Run full test suite
cargo test --release

# 2. Run DST batch tests (if modifying streaming/persistence)
cargo test streaming_dst --release

# 3. Run Maelstrom linearizability tests (if modifying replication)
./maelstrom/maelstrom test -w lin-kv --bin ./target/release/maelstrom_kv_replicated \
    --node-count 3 --time-limit 60 --rate 100

# 4. Quick local smoke test
cargo run --release --bin quick_benchmark

# 5. Docker benchmarks (REQUIRED for performance claims)
cd docker-benchmark && ./run-benchmarks.sh

# 6. Update BENCHMARK_RESULTS.md with Docker benchmark results

# 7. Commit with descriptive message
git commit -m "Description of changes

- What was added/changed
- Test results summary
- Docker benchmark comparison (if performance-related)"
```

### What Must Be Updated

| Change Type | Required Updates |
|-------------|-----------------|
| New feature | Tests, README feature list |
| Performance change | Docker benchmarks, BENCHMARK_RESULTS.md (Docker numbers only) |
| Bug fix | Regression test |
| Replication change | Maelstrom test run |
| Streaming change | DST tests with fault injection |
| Any Redis comparison | Docker benchmarks (NEVER local benchmarks) |

### Benchmark Documentation Format

When updating `BENCHMARK_RESULTS.md`, include:
```markdown
## [Date] - [Change Description]

### Test Configuration
- **Method**: Docker benchmarks (docker-benchmark/run-benchmarks.sh)
- **Docker**: [version]
- **Host OS**: [OS version]
- **CPU Limit**: 2 cores per container
- **Memory Limit**: 1GB per container

### Results (Docker - Fair Comparison)
| Operation | Redis 7.4 | Rust Implementation | Relative |
|-----------|-----------|---------------------|----------|
| SET (P=1) | X req/s   | Y req/s             | Z%       |
| GET (P=1) | X req/s   | Y req/s             | Z%       |
| SET (P=16)| X req/s   | Y req/s             | Z%       |
| GET (P=16)| X req/s   | Y req/s             | Z%       |
```

**Important**: All Redis vs Rust comparison numbers MUST come from Docker benchmarks.

## Dependencies

Core dependencies and their purposes:
- `tokio`: Async runtime
- `bincode`: Binary serialization (3-5x smaller than JSON)
- `crc32fast`: Checksums for data integrity
- `tracing`: Structured logging
