#!/usr/bin/env python3
"""
OpenEvolve-style Performance Optimization Harness

Evolutionary optimization that automatically discovers optimal configuration
parameters by running benchmarks and evolving towards Redis 8.0 performance.

Usage:
    # Run full evolution (default: 15 generations, 12 population)
    python -m evolve.harness

    # Custom parameters
    python -m evolve.harness --generations 20 --population 16

    # Dry run with mock evaluator (for testing)
    python -m evolve.harness --mock

    # Resume from previous generation
    python -m evolve.harness --resume evolve/results/gen_005.json

Examples:
    # Quick test run
    python -m evolve.harness --mock --generations 3 --population 6

    # Production run
    python -m evolve.harness --generations 15 --population 12
"""

import argparse
import logging
import sys
from pathlib import Path
from datetime import datetime

# Add project root to path for imports
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from evolve.config import DEFAULT_CONFIG, config_to_toml, PARAM_BOUNDS
from evolve.candidate import Candidate
from evolve.fitness import FitnessEvaluator, MockFitnessEvaluator
from evolve.evolution import EvolutionEngine

def setup_logging(verbose: bool = False) -> None:
    """Configure logging for the harness."""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s | %(levelname)-8s | %(message)s",
        datefmt="%H:%M:%S",
    )

def print_banner() -> None:
    """Print startup banner."""
    print()
    print("=" * 60)
    print("  OpenEvolve Performance Optimization Harness")
    print("  Redis-Rust Evolutionary Configuration Discovery")
    print("=" * 60)
    print()

def print_param_space() -> None:
    """Print the parameter search space."""
    print("Parameter Search Space:")
    print("-" * 60)
    for param, bounds in PARAM_BOUNDS.items():
        step = "2^n" if bounds.step == "power_of_2" else f"±{bounds.step}"
        print(f"  {param:30s} [{bounds.min:5d} - {bounds.max:5d}] step={step}")
    print()

def main():
    parser = argparse.ArgumentParser(
        description="OpenEvolve-style performance optimization harness",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )

    # Evolution parameters
    parser.add_argument("--generations", "-g", type=int, default=15,
                       help="Number of generations to run (default: 15)")
    parser.add_argument("--population", "-p", type=int, default=12,
                       help="Population size (default: 12)")
    parser.add_argument("--mutation-rate", "-m", type=float, default=0.3,
                       help="Mutation rate per parameter (default: 0.3)")
    parser.add_argument("--elite", "-e", type=int, default=2,
                       help="Number of elite candidates to preserve (default: 2)")

    # Execution options
    parser.add_argument("--mock", action="store_true",
                       help="Use mock evaluator (no real benchmarks)")
    parser.add_argument("--resume", type=Path,
                       help="Resume from generation JSON file")
    parser.add_argument("--output", "-o", type=Path, default=Path("evolve/results"),
                       help="Output directory for results (default: evolve/results)")

    # Paths
    parser.add_argument("--docker-dir", type=Path, default=PROJECT_ROOT / "docker-benchmark",
                       help="Docker benchmark directory")

    # Logging
    parser.add_argument("--verbose", "-v", action="store_true",
                       help="Enable verbose logging")

    args = parser.parse_args()

    setup_logging(args.verbose)
    logger = logging.getLogger(__name__)

    print_banner()
    print_param_space()

    # Create evaluator
    if args.mock:
        logger.info("Using MOCK evaluator (no real benchmarks)")
        evaluator = MockFitnessEvaluator(
            docker_benchmark_dir=args.docker_dir,
            project_root=PROJECT_ROOT,
            rebuild_enabled=False,
        )
    else:
        logger.info("Using REAL evaluator (will run Docker benchmarks)")
        evaluator = FitnessEvaluator(
            docker_benchmark_dir=args.docker_dir,
            project_root=PROJECT_ROOT,
            rebuild_enabled=True,
        )

    # Create evolution engine
    engine = EvolutionEngine(
        evaluator=evaluator,
        population_size=args.population,
        elite_count=args.elite,
        mutation_rate=args.mutation_rate,
        generations=args.generations,
        results_dir=args.output,
    )

    print(f"Configuration:")
    print(f"  Generations:    {args.generations}")
    print(f"  Population:     {args.population}")
    print(f"  Elite count:    {args.elite}")
    print(f"  Mutation rate:  {args.mutation_rate:.1%}")
    print(f"  Output dir:     {args.output}")
    print()

    # Run evolution
    try:
        logger.info("Starting evolutionary optimization...")
        start_time = datetime.now()

        best = engine.run()

        elapsed = datetime.now() - start_time
        logger.info(f"Evolution completed in {elapsed}")

        # Print final results
        print()
        print("=" * 60)
        print("  BEST CONFIGURATION FOUND")
        print("=" * 60)
        print(f"  Fitness: {best.fitness:.1f}% of Redis 8.0")
        print()
        print("  Parameters:")
        for param, value in best.config.items():
            bounds = PARAM_BOUNDS[param]
            print(f"    {param:30s} = {value:5d}  (default: {bounds.default})")
        print()
        print(f"  Config saved to: {args.output / 'best_config.toml'}")
        print()

        # Print TOML for easy copying
        print("  Generated TOML:")
        print("  " + "-" * 40)
        for line in best.to_toml().split("\n"):
            print(f"  {line}")
        print("  " + "-" * 40)
        print()

        return 0

    except KeyboardInterrupt:
        logger.info("\nEvolution interrupted by user")
        if engine.best_ever:
            logger.info(f"Best so far: {engine.best_ever.fitness:.1f}%")
            engine._save_final_results()
        return 1

    except Exception as e:
        logger.exception(f"Evolution failed: {e}")
        return 1

if __name__ == "__main__":
    sys.exit(main())
