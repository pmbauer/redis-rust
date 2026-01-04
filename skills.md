# Skills for redis-rust Development

## Skill: Simulation-First Development

### When to Use
When implementing any new feature that involves:
- I/O operations (disk, network, object storage)
- Distributed coordination
- State persistence
- Background workers

### Workflow

1. **Define the I/O abstraction first**
   ```rust
   pub trait MyIo: Send + Sync + 'static {
       fn operation(&self) -> impl Future<Output = Result<T>>;
   }
   ```

2. **Create InMemory implementation for unit tests**
   ```rust
   pub struct InMemoryMyIo {
       data: Arc<RwLock<HashMap<K, V>>>,
   }
   ```

3. **Create Simulated implementation for DST**
   ```rust
   pub struct SimulatedMyIo {
       inner: InMemoryMyIo,
       rng: SimulatedRng,
       fault_config: FaultConfig,
   }
   ```

4. **Write DST tests with fault injection**
   ```rust
   #[test]
   fn test_under_faults() {
       for seed in 0..1000 {
           let store = SimulatedMyIo::with_seed(seed);
           // Run workload
           // Check invariants
       }
   }
   ```

5. **Create production implementation last**
   ```rust
   pub struct RealMyIo { /* actual implementation */ }
   ```

### Key Files
- `src/io/mod.rs` - Core I/O abstractions
- `src/streaming/simulated_store.rs` - Example simulated store
- `src/simulator/dst.rs` - DST harness

---

## Skill: Actor Pattern Implementation

### When to Use
When you need:
- Background processing with clean shutdown
- State that should not be shared
- Predictable message ordering

### Template

```rust
use tokio::sync::{mpsc, oneshot};

// 1. Define messages
pub enum MyActorMessage {
    DoSomething { data: Data, response: oneshot::Sender<Result<()>> },
    FireAndForget { data: Data },
    Shutdown { response: oneshot::Sender<()> },
}

// 2. Define actor (owns state exclusively)
struct MyActor {
    state: MyState,
    rx: mpsc::UnboundedReceiver<MyActorMessage>,
}

impl MyActor {
    async fn run(mut self) {
        while let Some(msg) = self.rx.recv().await {
            match msg {
                MyActorMessage::DoSomething { data, response } => {
                    let result = self.handle_something(data).await;
                    let _ = response.send(result);
                }
                MyActorMessage::FireAndForget { data } => {
                    self.handle_something(data).await;
                }
                MyActorMessage::Shutdown { response } => {
                    self.cleanup().await;
                    let _ = response.send(());
                    break;
                }
            }
        }
    }
}

// 3. Define handle (Clone, Send)
#[derive(Clone)]
pub struct MyActorHandle {
    tx: mpsc::UnboundedSender<MyActorMessage>,
}

impl MyActorHandle {
    pub async fn do_something(&self, data: Data) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        self.tx.send(MyActorMessage::DoSomething { data, response: tx })?;
        rx.await?
    }

    pub fn fire_and_forget(&self, data: Data) {
        let _ = self.tx.send(MyActorMessage::FireAndForget { data });
    }

    pub async fn shutdown(&self) {
        let (tx, rx) = oneshot::channel();
        if self.tx.send(MyActorMessage::Shutdown { response: tx }).is_ok() {
            let _ = rx.await;
        }
    }
}

// 4. Spawn function
pub fn spawn_my_actor(initial_state: MyState) -> (MyActorHandle, JoinHandle<()>) {
    let (tx, rx) = mpsc::unbounded_channel();
    let actor = MyActor { state: initial_state, rx };
    let handle = MyActorHandle { tx };
    let task = tokio::spawn(actor.run());
    (handle, task)
}
```

---

## Skill: TigerStyle Coding

### Principles

1. **Assertions for invariants**
   ```rust
   debug_assert!(buffer.len() <= MAX_BUFFER_SIZE, "buffer overflow");
   debug_assert!(timestamp > last_timestamp, "time went backwards");
   ```

2. **Simple control flow**
   ```rust
   // GOOD: Early return
   fn process(data: Option<Data>) -> Result<()> {
       let data = match data {
           Some(d) => d,
           None => return Ok(()),
       };
       // process data
   }

   // BAD: Deep nesting
   fn process(data: Option<Data>) -> Result<()> {
       if let Some(data) = data {
           if data.is_valid() {
               // deeply nested logic
           }
       }
   }
   ```

3. **Explicit error handling**
   ```rust
   // GOOD: Explicit Result
   fn parse(input: &[u8]) -> Result<Data, ParseError> {
       if input.len() < HEADER_SIZE {
           return Err(ParseError::TooShort { got: input.len(), expected: HEADER_SIZE });
       }
       // ...
   }

   // BAD: Panic
   fn parse(input: &[u8]) -> Data {
       assert!(input.len() >= HEADER_SIZE); // Panic in production!
       // ...
   }
   ```

4. **No hidden allocations**
   ```rust
   // GOOD: Pre-allocate
   let mut buffer = Vec::with_capacity(expected_size);

   // BAD: Grow dynamically in hot path
   let mut buffer = Vec::new(); // Will reallocate multiple times
   ```

---

## Skill: VOPR-Style Testing

### Concept
VOPR (Viewstamped Operation Replication Protocol) style testing uses:
- Randomized workloads
- Fault injection at random points
- Invariant checking after each operation
- Seed-based reproducibility

### Template

```rust
struct VoprTest {
    seed: u64,
    rng: StdRng,
    system: System,
    history: Vec<Operation>,
}

impl VoprTest {
    fn run(&mut self, ops: usize) {
        for _ in 0..ops {
            // 1. Generate random operation
            let op = self.generate_operation();

            // 2. Maybe inject fault
            if self.rng.gen_bool(0.1) {
                self.inject_fault();
            }

            // 3. Execute operation
            let result = self.system.execute(&op);
            self.history.push((op, result));

            // 4. Check invariants
            self.check_invariants();
        }
    }

    fn check_invariants(&self) {
        // Linearizability: history is linearizable
        // Durability: acknowledged writes are persisted
        // Consistency: state matches expected
    }
}

#[test]
fn vopr_stress_test() {
    for seed in 0..10000 {
        let mut test = VoprTest::new(seed);
        test.run(1000);
        // If this panics, the seed is printed for reproduction
    }
}
```

---

## Skill: Responsive Background Workers

### Problem
Long-running background tasks (like compaction) that sleep for extended periods don't respond quickly to shutdown signals.

### Solution
Break long sleeps into smaller chunks with shutdown checks:

```rust
async fn worker_loop(shutdown: Arc<AtomicBool>, check_interval: Duration) {
    loop {
        // Do work
        self.do_work().await;

        // Sleep in chunks, checking shutdown
        let chunk = Duration::from_millis(100);
        let mut remaining = check_interval;
        while remaining > Duration::ZERO {
            if shutdown.load(Ordering::SeqCst) {
                return; // Respond to shutdown within 100ms
            }
            let sleep_time = remaining.min(chunk);
            tokio::time::sleep(sleep_time).await;
            remaining = remaining.saturating_sub(chunk);
        }
    }
}
```

---

## Skill: Jepsen-Style Testing with Maelstrom

### Setup
```bash
# Install Maelstrom
wget https://github.com/jepsen-io/maelstrom/releases/download/v0.2.3/maelstrom.tar.bz2
tar -xf maelstrom.tar.bz2
```

### Running Tests
```bash
./maelstrom/maelstrom test -w lin-kv \
    --bin ./target/release/maelstrom_kv_replicated \
    --node-count 3 \
    --time-limit 60 \
    --rate 100 \
    --nemesis partition
```

### Key Nemeses
- `partition`: Network partitions
- `pause`: Process pauses (simulates GC)
- `kill`: Process crashes
- `clock-skew`: Time jumps

---

## Skill: Segment File Format Design

### Principles
1. **Header with magic bytes**: Detect corruption/wrong file type
2. **Version field**: Support format evolution
3. **Checksums**: Detect corruption
4. **Length-prefixed records**: Enable streaming reads

### Template
```
┌──────────────────────────────────┐
│ Header (fixed size)              │
│ - magic: [u8; 4] ("RSEG")       │
│ - version: u8                    │
│ - flags: u8                      │
│ - record_count: u32              │
│ - header_checksum: u32           │
├──────────────────────────────────┤
│ Records (variable)               │
│ - length: u32                    │
│ - data: [u8; length]             │
│ (repeat for each record)         │
├──────────────────────────────────┤
│ Footer (fixed size)              │
│ - data_checksum: u32             │
│ - total_size: u64                │
└──────────────────────────────────┘
```

---

## Skill: Manifest-Based Recovery

### Concept
Use a manifest file to track which segments exist and their order. This enables:
- Atomic updates via rename
- Crash recovery
- Compaction without data loss

### Pattern
```rust
struct Manifest {
    version: u64,          // Increment on each update
    segments: Vec<SegmentInfo>,
    checkpoint: Option<CheckpointInfo>,
}

impl ManifestManager {
    async fn save_atomic(&self, manifest: &Manifest) -> Result<()> {
        // 1. Write to temp file
        let temp = format!("{}.tmp", self.path);
        self.store.put(&temp, &serialize(manifest)?).await?;

        // 2. Atomic rename
        self.store.rename(&temp, &self.path).await?;

        Ok(())
    }

    async fn recover(&self) -> Result<Manifest> {
        // Load manifest, then load segments in order
        let manifest = self.load().await?;
        // Validate segment checksums
        // Return recovery state
    }
}
```

---

## Skill: Commit Verification

### When to Use
Before every commit to ensure code quality and documentation are maintained.

### Pre-Commit Checklist

```bash
#!/bin/bash
# pre-commit.sh - Run before every commit

set -e

echo "=== Running full test suite ==="
cargo test --release

echo "=== Running DST tests ==="
cargo test streaming_dst --release
cargo test dst_batch --release

echo "=== Building release binary ==="
cargo build --release

echo "=== Running quick benchmark ==="
cargo run --release --bin quick_benchmark

echo "=== All checks passed! ==="
```

### Benchmark Documentation

After running benchmarks, update `BENCHMARK_RESULTS.md`:

```markdown
## [YYYY-MM-DD] - [Brief description of change]

### Configuration
- CPU: [model]
- Memory: [size]
- Rust: [version]

### Results
| Metric | Value | vs Previous |
|--------|-------|-------------|
| SET ops/sec | 150,000 | +5% |
| GET ops/sec | 180,000 | +3% |
| P99 latency | 1.2ms | -10% |

### Notes
- [Any relevant observations]
```

### Maelstrom Linearizability Verification

For replication changes:

```bash
# Build the Maelstrom binary
cargo build --release --bin maelstrom_kv_replicated

# Run linearizability test
./maelstrom/maelstrom test -w lin-kv \
    --bin ./target/release/maelstrom_kv_replicated \
    --node-count 3 \
    --time-limit 60 \
    --rate 100 \
    --nemesis partition

# Check results
cat maelstrom/maelstrom/store/latest/jepsen.log | grep -E "(OK|FAIL)"
```

### Required Artifacts Per Change Type

| Change | Tests | Benchmarks | Maelstrom | Docs |
|--------|-------|------------|-----------|------|
| Core redis commands | ✓ | ✓ | - | README |
| Replication | ✓ | - | ✓ | - |
| Streaming persistence | ✓ DST | - | - | - |
| Performance optimization | ✓ | ✓ | - | BENCHMARK_RESULTS.md |
