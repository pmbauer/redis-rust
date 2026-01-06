"""
Candidate - represents an individual configuration in the population.
"""

from dataclasses import dataclass, field
from typing import Dict, Optional
from pathlib import Path
import json

from .config import DEFAULT_CONFIG, validate_config, config_to_toml

@dataclass
class Candidate:
    """
    An individual configuration candidate in the evolution population.

    Attributes:
        config: Dict mapping parameter names to values
        fitness: Fitness score (0-100+, higher is better)
        generation: Which generation this candidate was created in
        parent_ids: IDs of parent candidates (for tracking lineage)
    """
    config: Dict[str, int]
    fitness: Optional[float] = None
    generation: int = 0
    parent_ids: list[int] = field(default_factory=list)
    _id: int = field(default_factory=lambda: Candidate._next_id())

    _id_counter: int = 0

    @classmethod
    def _next_id(cls) -> int:
        cls._id_counter += 1
        return cls._id_counter

    @classmethod
    def reset_id_counter(cls):
        """Reset ID counter (useful for testing)."""
        cls._id_counter = 0

    @classmethod
    def from_default(cls) -> "Candidate":
        """Create a candidate with default configuration."""
        return cls(config=DEFAULT_CONFIG.copy())

    @classmethod
    def from_dict(cls, data: Dict) -> "Candidate":
        """Create a candidate from a dictionary (e.g., from JSON)."""
        return cls(
            config=data["config"],
            fitness=data.get("fitness"),
            generation=data.get("generation", 0),
            parent_ids=data.get("parent_ids", []),
        )

    def to_dict(self) -> Dict:
        """Convert to dictionary for serialization."""
        return {
            "id": self._id,
            "config": self.config,
            "fitness": self.fitness,
            "generation": self.generation,
            "parent_ids": self.parent_ids,
        }

    def is_valid(self) -> bool:
        """Check if this candidate's config is valid."""
        return validate_config(self.config)

    def to_toml(self) -> str:
        """Convert to TOML configuration file content."""
        return config_to_toml(self.config)

    def save_toml(self, path: Path) -> None:
        """Save configuration to a TOML file."""
        path.write_text(self.to_toml())

    def save_json(self, path: Path) -> None:
        """Save candidate (including fitness) to JSON."""
        path.write_text(json.dumps(self.to_dict(), indent=2))

    @classmethod
    def load_json(cls, path: Path) -> "Candidate":
        """Load candidate from JSON file."""
        data = json.loads(path.read_text())
        return cls.from_dict(data)

    def __repr__(self) -> str:
        fitness_str = f"{self.fitness:.1f}" if self.fitness is not None else "?"
        return f"Candidate(id={self._id}, fitness={fitness_str}, gen={self.generation})"

    def config_summary(self) -> str:
        """Short summary of configuration for logging."""
        return (
            f"shards={self.config['num_shards']}, "
            f"pool={self.config['response_pool.capacity']}/{self.config['response_pool.prewarm']}, "
            f"buf={self.config['buffers.read_size']}, "
            f"pipe={self.config['batching.min_pipeline_buffer']}/{self.config['batching.batch_threshold']}"
        )
