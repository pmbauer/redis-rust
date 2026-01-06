//! Integration tests for Streaming Persistence
//!
//! Tests the full pipeline: Command execution → Delta generation → WriteBuffer → Segment → ObjectStore

use redis_sim::production::ReplicatedShardedState;
use redis_sim::redis::{Command, SDS};
use redis_sim::replication::ReplicationConfig;
use redis_sim::streaming::{
    delta_sink_channel, InMemoryObjectStore, LocalFsObjectStore, ObjectStore,
    DeltaSinkPersistenceWorker, SegmentReader, WriteBuffer, WriteBufferConfig,
};
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;

/// Helper to create SDS from string
fn sds(s: &str) -> SDS {
    SDS::from_str(s)
}

/// Helper to create a test replication config
fn test_config(replica_id: u64) -> ReplicationConfig {
    ReplicationConfig {
        replica_id,
        enabled: false, // Disable gossip for these tests
        ..Default::default()
    }
}

/// Test basic end-to-end flow with in-memory store
#[tokio::test]
async fn test_streaming_persistence_basic_flow() {
    // Setup persistence infrastructure
    let store = Arc::new(InMemoryObjectStore::new());
    let config = WriteBufferConfig::test();
    let write_buffer = Arc::new(WriteBuffer::new(
        store.clone(),
        "segments".to_string(),
        config,
    ));

    // Create delta sink channel
    let (sender, receiver) = delta_sink_channel();

    // Create persistence worker
    let (worker, handle) = DeltaSinkPersistenceWorker::new(receiver, write_buffer.clone());
    let worker_task = tokio::spawn(worker.run());

    // Create replicated state with delta sink
    let mut state = ReplicatedShardedState::new(test_config(1));
    state.set_delta_sink(sender);

    // Execute some commands
    state.execute(Command::set("key1".to_string(), sds("value1"))).await;
    state.execute(Command::set("key2".to_string(), sds("value2"))).await;
    state.execute(Command::set("key3".to_string(), sds("value3"))).await;

    // Give worker time to process
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Force flush by waiting for flush interval
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Shutdown worker
    handle.shutdown();
    tokio::time::timeout(Duration::from_secs(1), worker_task)
        .await
        .expect("Worker should complete")
        .expect("Worker should not panic");

    // Verify segment was written
    let stats = write_buffer.stats();
    assert!(stats.total_deltas_flushed >= 3, "Expected at least 3 deltas flushed, got {}", stats.total_deltas_flushed);
    assert!(stats.total_segments_written >= 1, "Expected at least 1 segment written");

    // Verify we can read the segment back
    let list_result = store.list("segments/", None).await.unwrap();
    assert!(!list_result.objects.is_empty(), "Expected at least one segment file");

    // Read and verify segment contents
    let segment_key = &list_result.objects[0].key;
    let segment_data = store.get(segment_key).await.unwrap();
    let reader = SegmentReader::open(&segment_data).unwrap();
    reader.validate().unwrap();

    let deltas: Vec<_> = reader.deltas().unwrap().collect();
    assert!(!deltas.is_empty(), "Segment should contain deltas");

    // Verify delta contents
    let keys: Vec<_> = deltas.iter().map(|d| d.as_ref().unwrap().key.as_str()).collect();
    assert!(keys.contains(&"key1"), "Should contain key1");
    assert!(keys.contains(&"key2"), "Should contain key2");
    assert!(keys.contains(&"key3"), "Should contain key3");
}

/// Test persistence with local filesystem store
#[tokio::test]
async fn test_streaming_persistence_localfs() {
    let temp_dir = TempDir::new().unwrap();
    let store = Arc::new(LocalFsObjectStore::new(temp_dir.path().to_path_buf()));

    let config = WriteBufferConfig::test();
    let write_buffer = Arc::new(WriteBuffer::new(
        store.clone(),
        "data".to_string(),
        config,
    ));

    let (sender, receiver) = delta_sink_channel();
    let (worker, handle) = DeltaSinkPersistenceWorker::new(receiver, write_buffer.clone());
    let worker_task = tokio::spawn(worker.run());

    let mut state = ReplicatedShardedState::new(test_config(1));
    state.set_delta_sink(sender);

    // Execute commands
    for i in 0..10 {
        state.execute(Command::set(format!("key{}", i), sds(&format!("value{}", i)))).await;
    }

    // Allow processing
    tokio::time::sleep(Duration::from_millis(150)).await;

    handle.shutdown();
    worker_task.await.unwrap();

    // Verify files exist on disk
    let list_result = store.list("data/", None).await.unwrap();
    assert!(!list_result.objects.is_empty());

    // Verify segment file exists in temp dir
    let segment_path = temp_dir.path().join(&list_result.objects[0].key);
    assert!(segment_path.exists(), "Segment file should exist at {:?}", segment_path);

    // Read and validate
    let segment_data = store.get(&list_result.objects[0].key).await.unwrap();
    let reader = SegmentReader::open(&segment_data).unwrap();
    reader.validate().unwrap();

    let delta_count = reader.deltas().unwrap().count();
    assert_eq!(delta_count, 10, "Should have 10 deltas");
}

/// Test high-throughput persistence
#[tokio::test]
async fn test_streaming_persistence_high_throughput() {
    let store = Arc::new(InMemoryObjectStore::new());
    let mut config = WriteBufferConfig::test();
    config.max_deltas = 100; // Force flush every 100 deltas
    config.flush_interval = Duration::from_secs(60); // Disable time-based flush

    let write_buffer = Arc::new(WriteBuffer::new(
        store.clone(),
        "ht".to_string(),
        config,
    ));

    let (sender, receiver) = delta_sink_channel();
    let (worker, handle) = DeltaSinkPersistenceWorker::new(receiver, write_buffer.clone());
    let worker_task = tokio::spawn(worker.run());

    let mut state = ReplicatedShardedState::new(test_config(1));
    state.set_delta_sink(sender);

    // Execute many commands
    let num_commands = 500;
    for i in 0..num_commands {
        state.execute(Command::set(format!("k{}", i), sds(&format!("v{}", i)))).await;
    }

    // Allow processing
    tokio::time::sleep(Duration::from_millis(200)).await;

    handle.shutdown();
    worker_task.await.unwrap();

    // Verify all deltas were flushed
    let stats = write_buffer.stats();
    assert_eq!(stats.total_deltas_flushed, num_commands as u64);
    // Multiple segments should be written (at least 1, typically several)
    assert!(stats.total_segments_written >= 1, "Expected at least 1 segment for {} deltas", num_commands);

    // Verify all segments are readable
    let list_result = store.list("ht/", None).await.unwrap();
    let mut total_deltas = 0;

    for obj in &list_result.objects {
        let data = store.get(&obj.key).await.unwrap();
        let reader = SegmentReader::open(&data).unwrap();
        reader.validate().unwrap();
        total_deltas += reader.deltas().unwrap().count();
    }

    assert_eq!(total_deltas, num_commands, "Total deltas across all segments should match");
}

/// Test persistence survives command executor state
#[tokio::test]
async fn test_streaming_persistence_state_recovery() {
    let store = Arc::new(InMemoryObjectStore::new());

    // Phase 1: Write some data
    {
        let config = WriteBufferConfig::test();
        let write_buffer = Arc::new(WriteBuffer::new(
            store.clone(),
            "recovery".to_string(),
            config,
        ));

        let (sender, receiver) = delta_sink_channel();
        let (worker, handle) = DeltaSinkPersistenceWorker::new(receiver, write_buffer.clone());
        let worker_task = tokio::spawn(worker.run());

        let mut state = ReplicatedShardedState::new(test_config(1));
        state.set_delta_sink(sender);

        state.execute(Command::set("persistent_key".to_string(), sds("persistent_value"))).await;
        state.execute(Command::set("another_key".to_string(), sds("another_value"))).await;

        tokio::time::sleep(Duration::from_millis(100)).await;
        handle.shutdown();
        worker_task.await.unwrap();
    }

    // Phase 2: Read back from segments (simulating recovery)
    let list_result = store.list("recovery/", None).await.unwrap();
    assert!(!list_result.objects.is_empty());

    let mut recovered_keys = Vec::new();
    for obj in &list_result.objects {
        let data = store.get(&obj.key).await.unwrap();
        let reader = SegmentReader::open(&data).unwrap();

        for delta_result in reader.deltas().unwrap() {
            let delta = delta_result.unwrap();
            recovered_keys.push(delta.key.clone());
        }
    }

    assert!(recovered_keys.contains(&"persistent_key".to_string()));
    assert!(recovered_keys.contains(&"another_key".to_string()));
}

/// Test multiple replicas writing to separate prefixes
#[tokio::test]
async fn test_streaming_persistence_multi_replica() {
    let store = Arc::new(InMemoryObjectStore::new());

    async fn run_replica(
        store: Arc<InMemoryObjectStore>,
        replica_id: u64,
        commands: Vec<(&str, &str)>,
    ) {
        let config = WriteBufferConfig::test();
        let write_buffer = Arc::new(WriteBuffer::new(
            store,
            format!("replica-{}", replica_id),
            config,
        ));

        let (sender, receiver) = delta_sink_channel();
        let (worker, handle) = DeltaSinkPersistenceWorker::new(receiver, write_buffer);
        let worker_task = tokio::spawn(worker.run());

        let mut state = ReplicatedShardedState::new(test_config(replica_id));
        state.set_delta_sink(sender);

        for (key, value) in commands {
            state.execute(Command::set(key.to_string(), SDS::from_str(value))).await;
        }

        tokio::time::sleep(Duration::from_millis(100)).await;
        handle.shutdown();
        worker_task.await.unwrap();
    }

    // Run three replicas concurrently
    let store1 = store.clone();
    let store2 = store.clone();
    let store3 = store.clone();

    tokio::join!(
        run_replica(store1, 1, vec![("r1k1", "v1"), ("r1k2", "v2")]),
        run_replica(store2, 2, vec![("r2k1", "v1"), ("r2k2", "v2")]),
        run_replica(store3, 3, vec![("r3k1", "v1"), ("r3k2", "v2")]),
    );

    // Verify each replica wrote to its own prefix
    for replica_id in 1..=3 {
        let prefix = format!("replica-{}/", replica_id);
        let list_result = store.list(&prefix, None).await.unwrap();
        assert!(!list_result.objects.is_empty(), "Replica {} should have segments", replica_id);

        // Verify segment contents
        let data = store.get(&list_result.objects[0].key).await.unwrap();
        let reader = SegmentReader::open(&data).unwrap();
        let deltas: Vec<_> = reader.deltas().unwrap().collect();

        assert_eq!(deltas.len(), 2, "Replica {} should have 2 deltas", replica_id);
        let key_prefix = format!("r{}k", replica_id);
        assert!(deltas[0].as_ref().unwrap().key.starts_with(&key_prefix));
    }
}

/// Test backpressure handling
#[tokio::test]
async fn test_streaming_persistence_backpressure() {
    let store = Arc::new(InMemoryObjectStore::new());
    let mut config = WriteBufferConfig::test();
    config.backpressure_threshold_bytes = 500; // Very small threshold
    config.flush_interval = Duration::from_secs(60); // Disable auto-flush

    let write_buffer = Arc::new(WriteBuffer::new(
        store.clone(),
        "bp".to_string(),
        config,
    ));

    let (sender, receiver) = delta_sink_channel();

    // Don't start the worker - let backpressure build
    let mut state = ReplicatedShardedState::new(test_config(1));
    state.set_delta_sink(sender.clone());

    // Write until channel buffer fills
    // Note: The channel itself is unbounded, but the WriteBuffer has backpressure
    // In this test we're checking that the delta sink send succeeds even without
    // the worker running (channel is unbounded)
    for i in 0..100 {
        state.execute(Command::set(format!("key{}", i), sds(&format!("value{}", i)))).await;
    }

    // Now start the worker and verify it can drain
    let (worker, handle) = DeltaSinkPersistenceWorker::new(receiver, write_buffer.clone());
    let worker_task = tokio::spawn(worker.run());

    tokio::time::sleep(Duration::from_millis(200)).await;

    // Force a flush
    write_buffer.flush().await.unwrap();

    handle.shutdown();
    worker_task.await.unwrap();

    let stats = write_buffer.stats();
    assert!(stats.total_deltas_flushed > 0, "Should have flushed some deltas");
}

/// Test segment ordering
#[tokio::test]
async fn test_streaming_persistence_segment_ordering() {
    let store = Arc::new(InMemoryObjectStore::new());
    let mut config = WriteBufferConfig::test();
    config.max_deltas = 5; // Small batches
    config.flush_interval = Duration::from_secs(60);

    let write_buffer = Arc::new(WriteBuffer::new(
        store.clone(),
        "ordered".to_string(),
        config,
    ));

    let (sender, receiver) = delta_sink_channel();
    let (worker, handle) = DeltaSinkPersistenceWorker::new(receiver, write_buffer.clone());
    let worker_task = tokio::spawn(worker.run());

    let mut state = ReplicatedShardedState::new(test_config(1));
    state.set_delta_sink(sender);

    // Write sequentially numbered keys
    for i in 0..25 {
        state.execute(Command::set(format!("seq{:03}", i), sds(&format!("v{}", i)))).await;
        // Small delay to ensure ordering
        if i % 5 == 4 {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    tokio::time::sleep(Duration::from_millis(100)).await;
    handle.shutdown();
    worker_task.await.unwrap();

    // Verify segments are numbered sequentially
    let list_result = store.list("ordered/", None).await.unwrap();
    let mut segment_names: Vec<_> = list_result.objects.iter().map(|o| o.key.clone()).collect();
    segment_names.sort();

    // Should have segments like segment-00000000.seg, segment-00000001.seg, etc.
    for (i, name) in segment_names.iter().enumerate() {
        let expected_suffix = format!("segment-{:08}.seg", i);
        assert!(name.ends_with(&expected_suffix), "Segment {} should end with {}", name, expected_suffix);
    }
}

/// Test delete operations are persisted
#[tokio::test]
async fn test_streaming_persistence_deletes() {
    let store = Arc::new(InMemoryObjectStore::new());
    let config = WriteBufferConfig::test();
    let write_buffer = Arc::new(WriteBuffer::new(
        store.clone(),
        "deletes".to_string(),
        config,
    ));

    let (sender, receiver) = delta_sink_channel();
    let (worker, handle) = DeltaSinkPersistenceWorker::new(receiver, write_buffer.clone());
    let worker_task = tokio::spawn(worker.run());

    let mut state = ReplicatedShardedState::new(test_config(1));
    state.set_delta_sink(sender);

    // Set and then delete
    state.execute(Command::set("to_delete".to_string(), sds("temporary"))).await;
    state.execute(Command::del("to_delete".to_string())).await;
    state.execute(Command::set("to_keep".to_string(), sds("permanent"))).await;

    tokio::time::sleep(Duration::from_millis(100)).await;
    handle.shutdown();
    worker_task.await.unwrap();

    // Verify both set and delete operations are persisted
    let list_result = store.list("deletes/", None).await.unwrap();
    let data = store.get(&list_result.objects[0].key).await.unwrap();
    let reader = SegmentReader::open(&data).unwrap();

    let deltas: Vec<_> = reader.deltas().unwrap().filter_map(|r| r.ok()).collect();

    // Should have 3 deltas: SET to_delete, DEL to_delete, SET to_keep
    assert!(deltas.len() >= 2, "Should have at least set and delete deltas");

    let keys: Vec<_> = deltas.iter().map(|d| d.key.as_str()).collect();
    assert!(keys.contains(&"to_delete"), "Should have to_delete key");
    assert!(keys.contains(&"to_keep"), "Should have to_keep key");
}

/// Test concurrent writes and flushes
#[tokio::test]
async fn test_streaming_persistence_concurrent_writes() {
    let store = Arc::new(InMemoryObjectStore::new());
    let config = WriteBufferConfig::test();
    let write_buffer = Arc::new(WriteBuffer::new(
        store.clone(),
        "concurrent".to_string(),
        config,
    ));

    let (sender, receiver) = delta_sink_channel();
    let (worker, handle) = DeltaSinkPersistenceWorker::new(receiver, write_buffer.clone());
    let worker_task = tokio::spawn(worker.run());

    // Note: We can't easily set delta_sink on Arc<ReplicatedShardedState>
    // This test demonstrates concurrent writes through the delta sink
    // In production, the state would be behind Arc<RwLock<>> or similar

    // For this test, we'll use the sender directly
    let sender1 = sender.clone();
    let sender2 = sender.clone();

    use redis_sim::redis::SDS;
    use redis_sim::replication::lattice::{LamportClock, ReplicaId};
    use redis_sim::replication::state::ReplicatedValue;
    use redis_sim::replication::ReplicationDelta;

    fn make_delta(key: &str, value: &str) -> ReplicationDelta {
        let replica_id = ReplicaId::new(1);
        let clock = LamportClock { time: 1000, replica_id };
        let replicated = ReplicatedValue::with_value(SDS::from_str(value), clock);
        ReplicationDelta::new(key.to_string(), replicated, replica_id)
    }

    // Spawn concurrent writers
    let w1 = tokio::spawn(async move {
        for i in 0..50 {
            sender1.send(make_delta(&format!("w1k{}", i), &format!("v{}", i))).unwrap();
        }
    });

    let w2 = tokio::spawn(async move {
        for i in 0..50 {
            sender2.send(make_delta(&format!("w2k{}", i), &format!("v{}", i))).unwrap();
        }
    });

    w1.await.unwrap();
    w2.await.unwrap();

    tokio::time::sleep(Duration::from_millis(200)).await;
    handle.shutdown();
    worker_task.await.unwrap();

    // Verify all 100 deltas were persisted
    let stats = write_buffer.stats();
    assert_eq!(stats.total_deltas_flushed, 100, "All deltas should be flushed");

    // Count deltas across all segments
    let list_result = store.list("concurrent/", None).await.unwrap();
    let mut total = 0;
    for obj in &list_result.objects {
        let data = store.get(&obj.key).await.unwrap();
        let reader = SegmentReader::open(&data).unwrap();
        total += reader.deltas().unwrap().count();
    }
    assert_eq!(total, 100);
}
