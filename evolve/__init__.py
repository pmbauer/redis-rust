"""
OpenEvolve-style Performance Optimization Harness

Evolutionary optimization that automatically discovers optimal configuration
parameters by running benchmarks and evolving towards Redis 8.0 performance.
"""

from .config import PARAM_BOUNDS, DEFAULT_CONFIG
from .candidate import Candidate
from .fitness import FitnessEvaluator
from .evolution import EvolutionEngine

__version__ = "0.1.0"
__all__ = [
    "PARAM_BOUNDS",
    "DEFAULT_CONFIG",
    "Candidate",
    "FitnessEvaluator",
    "EvolutionEngine",
]
