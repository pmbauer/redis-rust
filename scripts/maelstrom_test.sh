#!/bin/bash
set -e

MAELSTROM_DIR="/tmp/maelstrom"
BIN="/home/runner/workspace/target/release/maelstrom-kv"

echo "Building maelstrom-kv binary..."
cd /home/runner/workspace
cargo build --bin maelstrom-kv --release

if [ ! -d "$MAELSTROM_DIR" ]; then
    echo "Maelstrom not found. Please run setup first."
    exit 1
fi

echo ""
echo "============================================"
echo "  Running Maelstrom Linearizability Tests"
echo "============================================"
echo ""

echo "Test 1: Single-node linearizability (should pass)"
echo "---------------------------------------------------"
cd $MAELSTROM_DIR
./maelstrom test -w lin-kv \
    --bin $BIN \
    --node-count 1 \
    --time-limit 10 \
    --rate 10 \
    --concurrency 2

echo ""
echo "Test 2: Single-node with higher load (should pass)"
echo "---------------------------------------------------"
./maelstrom test -w lin-kv \
    --bin $BIN \
    --node-count 1 \
    --time-limit 20 \
    --rate 100 \
    --concurrency 4

echo ""
echo "============================================"
echo "  All single-node tests passed!"
echo "  (Multi-node tests require replication)"
echo "============================================"
