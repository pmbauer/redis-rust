"""
Fitness evaluation - runs benchmarks and computes fitness scores.
"""

import subprocess
import shutil
import re
import time
from pathlib import Path
from dataclasses import dataclass
from typing import Dict, Optional, Tuple
import logging

from .candidate import Candidate

logger = logging.getLogger(__name__)

@dataclass
class BenchmarkResult:
    """Results from a single benchmark run."""
    set_p1: float   # SET ops/sec at pipeline=1
    get_p1: float   # GET ops/sec at pipeline=1
    set_p16: float  # SET ops/sec at pipeline=16
    get_p16: float  # GET ops/sec at pipeline=16

    def to_dict(self) -> Dict[str, float]:
        return {
            "set_p1": self.set_p1,
            "get_p1": self.get_p1,
            "set_p16": self.set_p16,
            "get_p16": self.get_p16,
        }

class FitnessEvaluator:
    """
    Evaluates fitness of candidates by running Docker benchmarks.

    Compares Rust implementation performance against Redis 8.0 baseline.
    """

    # Weights for fitness calculation (prioritize pipelined performance)
    WEIGHTS = {
        "set_p1": 0.20,
        "get_p1": 0.20,
        "set_p16": 0.30,
        "get_p16": 0.30,
    }

    def __init__(
        self,
        docker_benchmark_dir: Path,
        project_root: Path,
        redis8_baseline: Optional[BenchmarkResult] = None,
        rebuild_enabled: bool = True,
    ):
        """
        Initialize the fitness evaluator.

        Args:
            docker_benchmark_dir: Path to docker-benchmark/ directory
            project_root: Path to project root (for copying config)
            redis8_baseline: Pre-measured Redis 8.0 baseline (or will be measured)
            rebuild_enabled: Whether to rebuild Docker images (disable for testing)
        """
        self.docker_dir = docker_benchmark_dir
        self.project_root = project_root
        self.rebuild_enabled = rebuild_enabled
        self.redis8_baseline = redis8_baseline
        self._evaluation_count = 0

    def ensure_baseline(self) -> None:
        """Measure Redis 8.0 baseline if not provided."""
        if self.redis8_baseline is None:
            logger.info("Measuring Redis 8.0 baseline...")
            self.redis8_baseline = self._run_redis8_benchmark()
            logger.info(f"Redis 8.0 baseline: {self.redis8_baseline}")

    def evaluate(self, candidate: Candidate) -> float:
        """
        Evaluate a candidate's fitness.

        Returns:
            Fitness score (0-100+, where 100 = matches Redis 8.0)
        """
        self._evaluation_count += 1
        logger.info(f"Evaluating candidate {candidate._id} ({self._evaluation_count})")
        logger.info(f"  Config: {candidate.config_summary()}")

        # Ensure we have baseline
        self.ensure_baseline()

        # Write config file
        config_path = self.project_root / "perf_config.toml"
        candidate.save_toml(config_path)

        # Rebuild and benchmark
        try:
            if self.rebuild_enabled:
                self._rebuild_rust_image()

            rust_result = self._run_rust_benchmark()
            fitness = self._compute_fitness(rust_result)

            logger.info(f"  Rust result: {rust_result}")
            logger.info(f"  Fitness: {fitness:.1f}%")

            return fitness

        except Exception as e:
            logger.error(f"  Evaluation failed: {e}")
            return 0.0  # Penalty for failed evaluations

    def _compute_fitness(self, rust: BenchmarkResult) -> float:
        """
        Compute weighted fitness score comparing Rust to Redis 8.0.

        Returns percentage (0-100+, can exceed 100 if faster than Redis).
        """
        baseline = self.redis8_baseline
        assert baseline is not None

        scores = {
            "set_p1": (rust.set_p1 / baseline.set_p1) * 100 if baseline.set_p1 > 0 else 0,
            "get_p1": (rust.get_p1 / baseline.get_p1) * 100 if baseline.get_p1 > 0 else 0,
            "set_p16": (rust.set_p16 / baseline.set_p16) * 100 if baseline.set_p16 > 0 else 0,
            "get_p16": (rust.get_p16 / baseline.get_p16) * 100 if baseline.get_p16 > 0 else 0,
        }

        weighted = sum(scores[k] * self.WEIGHTS[k] for k in scores)
        return weighted

    def _rebuild_rust_image(self) -> None:
        """Rebuild the Rust Docker image with current config."""
        logger.info("  Rebuilding Rust Docker image...")

        # Copy config to docker-benchmark directory
        src = self.project_root / "perf_config.toml"
        dst = self.docker_dir / "perf_config.toml"
        shutil.copy(src, dst)

        # Rebuild image
        result = subprocess.run(
            ["docker", "compose", "-f", "docker-compose.redis8.yml",
             "build", "--no-cache", "redis-rust"],
            cwd=self.docker_dir,
            capture_output=True,
            text=True,
            timeout=300,  # 5 minute timeout
        )

        if result.returncode != 0:
            raise RuntimeError(f"Docker build failed: {result.stderr}")

    def _run_rust_benchmark(self) -> BenchmarkResult:
        """Run benchmark against Rust implementation."""
        return self._run_benchmark_script("run-redis8-comparison.sh", "redis-rust")

    def _run_redis8_benchmark(self) -> BenchmarkResult:
        """Run benchmark against Redis 8.0."""
        return self._run_benchmark_script("run-redis8-comparison.sh", "redis8")

    def _run_benchmark_script(self, script: str, target: str) -> BenchmarkResult:
        """Run benchmark script and parse results."""
        logger.info(f"  Running benchmark for {target}...")

        result = subprocess.run(
            [f"./{script}"],
            cwd=self.docker_dir,
            capture_output=True,
            text=True,
            timeout=600,  # 10 minute timeout
        )

        if result.returncode != 0:
            raise RuntimeError(f"Benchmark failed: {result.stderr}")

        # Parse results from the latest markdown file
        return self._parse_benchmark_results(target)

    def _parse_benchmark_results(self, target: str) -> BenchmarkResult:
        """Parse benchmark results from the results directory."""
        results_dir = self.docker_dir / "results"

        # Find the most recent markdown file
        md_files = sorted(results_dir.glob("*.md"), key=lambda p: p.stat().st_mtime)
        if not md_files:
            raise RuntimeError("No benchmark results found")

        latest = md_files[-1]
        content = latest.read_text()

        # Parse the markdown table for results
        # Example format in table:
        # | SET | P=1  | 123456 req/s | 234567 req/s | 52.6% |
        # | GET | P=1  | 123456 req/s | 234567 req/s | 52.6% |

        def extract_ops(pattern: str, content: str) -> Tuple[float, float]:
            """Extract (rust, redis) ops/sec from table row."""
            match = re.search(pattern, content)
            if not match:
                return 0.0, 0.0
            rust_ops = float(match.group(1).replace(",", ""))
            redis_ops = float(match.group(2).replace(",", ""))
            return rust_ops, redis_ops

        # Patterns for each metric (Rust impl | Redis 8.0)
        set_p1_pattern = r'\|\s*SET\s*\|\s*P=1\s*\|\s*([\d,]+)\s*req/s\s*\|\s*([\d,]+)\s*req/s'
        get_p1_pattern = r'\|\s*GET\s*\|\s*P=1\s*\|\s*([\d,]+)\s*req/s\s*\|\s*([\d,]+)\s*req/s'
        set_p16_pattern = r'\|\s*SET\s*\|\s*P=16\s*\|\s*([\d,]+)\s*req/s\s*\|\s*([\d,]+)\s*req/s'
        get_p16_pattern = r'\|\s*GET\s*\|\s*P=16\s*\|\s*([\d,]+)\s*req/s\s*\|\s*([\d,]+)\s*req/s'

        set_p1 = extract_ops(set_p1_pattern, content)
        get_p1 = extract_ops(get_p1_pattern, content)
        set_p16 = extract_ops(set_p16_pattern, content)
        get_p16 = extract_ops(get_p16_pattern, content)

        if target == "redis-rust":
            return BenchmarkResult(
                set_p1=set_p1[0],
                get_p1=get_p1[0],
                set_p16=set_p16[0],
                get_p16=get_p16[0],
            )
        else:  # redis8
            return BenchmarkResult(
                set_p1=set_p1[1],
                get_p1=get_p1[1],
                set_p16=set_p16[1],
                get_p16=get_p16[1],
            )

    @property
    def evaluation_count(self) -> int:
        """Number of evaluations performed."""
        return self._evaluation_count


class MockFitnessEvaluator(FitnessEvaluator):
    """
    Mock evaluator for testing (doesn't run actual benchmarks).
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.redis8_baseline = BenchmarkResult(
            set_p1=200000, get_p1=250000,
            set_p16=400000, get_p16=500000,
        )

    def evaluate(self, candidate: Candidate) -> float:
        """Return a mock fitness based on config values."""
        self._evaluation_count += 1

        # Mock fitness function that rewards certain configurations
        config = candidate.config
        fitness = 70.0  # Base fitness

        # Reward 16 shards
        if config["num_shards"] == 16:
            fitness += 5.0

        # Reward larger response pools
        if config["response_pool.capacity"] >= 256:
            fitness += 3.0

        # Reward appropriate buffer sizes
        if config["buffers.read_size"] >= 8192:
            fitness += 2.0

        # Add some randomness to simulate real benchmarks
        import random
        fitness += random.uniform(-5, 5)

        return max(0, min(100, fitness))
