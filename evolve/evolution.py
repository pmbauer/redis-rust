"""
Evolution engine - selection, mutation, crossover operators.
"""

import random
import logging
from typing import List, Dict, Tuple
from pathlib import Path
import json
from datetime import datetime

from .config import PARAM_BOUNDS, DEFAULT_CONFIG, clamp, next_power_of_two
from .candidate import Candidate
from .fitness import FitnessEvaluator

logger = logging.getLogger(__name__)

class EvolutionEngine:
    """
    Evolutionary optimization engine using genetic algorithms.

    Uses tournament selection, crossover, and mutation to evolve
    configurations towards optimal performance.
    """

    def __init__(
        self,
        evaluator: FitnessEvaluator,
        population_size: int = 12,
        elite_count: int = 2,
        mutation_rate: float = 0.3,
        generations: int = 15,
        tournament_k: int = 3,
        results_dir: Path = Path("evolve/results"),
    ):
        """
        Initialize the evolution engine.

        Args:
            evaluator: Fitness evaluator for candidates
            population_size: Number of candidates per generation
            elite_count: Number of top candidates to preserve unchanged
            mutation_rate: Probability of mutating each parameter
            generations: Number of generations to run
            tournament_k: Tournament selection size
            results_dir: Directory to save results
        """
        self.evaluator = evaluator
        self.population_size = population_size
        self.elite_count = elite_count
        self.mutation_rate = mutation_rate
        self.generations = generations
        self.tournament_k = tournament_k
        self.results_dir = Path(results_dir)
        self.results_dir.mkdir(parents=True, exist_ok=True)

        self._best_ever: Candidate | None = None
        self._history: List[Dict] = []

    def run(self) -> Candidate:
        """
        Run the evolutionary optimization.

        Returns:
            The best candidate found across all generations.
        """
        logger.info(f"Starting evolution: {self.generations} generations, "
                   f"{self.population_size} population")

        # Initialize population
        population = self._initialize_population()

        for gen in range(self.generations):
            logger.info(f"\n{'='*50}")
            logger.info(f"Generation {gen + 1}/{self.generations}")
            logger.info(f"{'='*50}")

            # Evaluate all candidates
            for candidate in population:
                if candidate.fitness is None:
                    candidate.fitness = self.evaluator.evaluate(candidate)
                    candidate.generation = gen

            # Sort by fitness (descending)
            population.sort(key=lambda c: c.fitness or 0, reverse=True)

            # Log generation results
            best = population[0]
            avg_fitness = sum(c.fitness or 0 for c in population) / len(population)
            logger.info(f"Gen {gen + 1} Results:")
            logger.info(f"  Best: {best.fitness:.1f}% ({best.config_summary()})")
            logger.info(f"  Avg:  {avg_fitness:.1f}%")
            logger.info(f"  Population: {[f'{c.fitness:.1f}' for c in population[:5]]}...")

            # Track best ever
            if self._best_ever is None or (best.fitness or 0) > (self._best_ever.fitness or 0):
                self._best_ever = best
                logger.info(f"  NEW BEST EVER: {best.fitness:.1f}%")

            # Record history
            self._history.append({
                "generation": gen + 1,
                "best_fitness": best.fitness,
                "avg_fitness": avg_fitness,
                "best_config": best.config.copy(),
                "population_fitness": [c.fitness for c in population],
            })

            # Save generation results
            self._save_generation(gen, population)

            # Early termination if we've reached target
            if (best.fitness or 0) >= 100:
                logger.info("Reached 100% fitness - stopping early!")
                break

            # Create next generation (unless last)
            if gen < self.generations - 1:
                population = self._evolve(population)

        # Final results
        logger.info(f"\n{'='*50}")
        logger.info("EVOLUTION COMPLETE")
        logger.info(f"{'='*50}")
        logger.info(f"Best fitness: {self._best_ever.fitness:.1f}%")
        logger.info(f"Best config: {self._best_ever.config_summary()}")
        logger.info(f"Full config: {self._best_ever.config}")

        # Save final results
        self._save_final_results()

        return self._best_ever

    def _initialize_population(self) -> List[Candidate]:
        """Create initial population with default and random variations."""
        population = []

        # Always include default config
        population.append(Candidate.from_default())

        # Generate random variations
        while len(population) < self.population_size:
            config = self._random_config()
            candidate = Candidate(config=config)
            if candidate.is_valid():
                population.append(candidate)

        return population

    def _random_config(self) -> Dict[str, int]:
        """Generate a random valid configuration."""
        config = {}
        for param, bounds in PARAM_BOUNDS.items():
            if bounds.step == "power_of_2":
                # Generate random power of 2 within bounds
                powers = []
                val = bounds.min
                while val <= bounds.max:
                    powers.append(val)
                    val *= 2
                config[param] = random.choice(powers)
            else:
                # Generate random value within bounds at step intervals
                steps = (bounds.max - bounds.min) // bounds.step
                step_idx = random.randint(0, steps)
                config[param] = bounds.min + step_idx * bounds.step
        return config

    def _evolve(self, population: List[Candidate]) -> List[Candidate]:
        """Create next generation through selection, crossover, and mutation."""
        new_population = []

        # Elitism: keep top candidates unchanged
        elite = population[:self.elite_count]
        for candidate in elite:
            new_candidate = Candidate(
                config=candidate.config.copy(),
                fitness=candidate.fitness,  # Preserve fitness to avoid re-evaluation
                parent_ids=[candidate._id],
            )
            new_population.append(new_candidate)
            logger.debug(f"  Elite preserved: {candidate}")

        # Fill rest with offspring
        while len(new_population) < self.population_size:
            # Tournament selection
            parent1 = self._tournament_select(population)
            parent2 = self._tournament_select(population)

            # Crossover
            child_config = self._crossover(parent1.config, parent2.config)

            # Mutation
            child_config = self._mutate(child_config)

            child = Candidate(
                config=child_config,
                parent_ids=[parent1._id, parent2._id],
            )

            if child.is_valid():
                new_population.append(child)

        return new_population

    def _tournament_select(self, population: List[Candidate]) -> Candidate:
        """Select a candidate using tournament selection."""
        tournament = random.sample(population, min(self.tournament_k, len(population)))
        return max(tournament, key=lambda c: c.fitness or 0)

    def _crossover(self, config1: Dict[str, int], config2: Dict[str, int]) -> Dict[str, int]:
        """Uniform crossover between two configurations."""
        child = {}
        for param in PARAM_BOUNDS:
            # 50/50 chance of inheriting from each parent
            if random.random() < 0.5:
                child[param] = config1[param]
            else:
                child[param] = config2[param]
        return child

    def _mutate(self, config: Dict[str, int]) -> Dict[str, int]:
        """Mutate a configuration with given probability per parameter."""
        mutated = config.copy()

        for param, bounds in PARAM_BOUNDS.items():
            if random.random() < self.mutation_rate:
                current = mutated[param]

                if bounds.step == "power_of_2":
                    # Shift up or down by one power
                    direction = random.choice([-1, 1])
                    new_val = next_power_of_two(current, direction)
                    mutated[param] = clamp(new_val, bounds.min, bounds.max)
                else:
                    # Random step within bounds
                    delta = random.choice([-1, 0, 1]) * bounds.step
                    new_val = current + delta
                    mutated[param] = clamp(new_val, bounds.min, bounds.max)

        return mutated

    def _save_generation(self, gen: int, population: List[Candidate]) -> None:
        """Save generation results to file."""
        filename = self.results_dir / f"gen_{gen + 1:03d}.json"
        data = {
            "generation": gen + 1,
            "timestamp": datetime.now().isoformat(),
            "population": [c.to_dict() for c in population],
        }
        filename.write_text(json.dumps(data, indent=2))

    def _save_final_results(self) -> None:
        """Save final results and best configuration."""
        # Save best config as TOML
        best_toml = self.results_dir / "best_config.toml"
        self._best_ever.save_toml(best_toml)
        logger.info(f"Best config saved to: {best_toml}")

        # Save full history
        history_file = self.results_dir / "evolution_history.json"
        data = {
            "completed_at": datetime.now().isoformat(),
            "generations": self.generations,
            "population_size": self.population_size,
            "mutation_rate": self.mutation_rate,
            "best_candidate": self._best_ever.to_dict(),
            "history": self._history,
        }
        history_file.write_text(json.dumps(data, indent=2))
        logger.info(f"Evolution history saved to: {history_file}")

    @property
    def best_ever(self) -> Candidate | None:
        """The best candidate found so far."""
        return self._best_ever

    @property
    def history(self) -> List[Dict]:
        """Evolution history by generation."""
        return self._history
