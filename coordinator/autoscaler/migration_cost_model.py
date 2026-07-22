import logging
import os
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from styx.common.stateflow_graph import StateflowGraph

# Migration time estimation.
# Cold-start horizon used only before the first migration has been measured;
# afterwards the estimate is driven entirely by the learned per-moved-key rate.
MIGRATION_COLD_START_HORIZON_SEC: float = float(os.getenv("MIGRATION_COLD_START_HORIZON_SEC", "30"))
MIN_CHRONOS_PREDICTION_HORIZON: int = int(os.getenv("MIN_CHRONOS_PREDICTION_HORIZON", "10"))


class MigrationCostModel:
    def __init__(self) -> None:
        self.sec_per_moved_key_ewma: float | None = None
        self.sec_per_moved_key_ewma_alpha: float = 0.5
        self.migration_keys_to_move: float = 0.0

    @staticmethod
    def f_migrate(n_partitions_old: int, n_partitions_new: int) -> float:
        """Fraction of keys that change partition under hash repartitioning."""
        if n_partitions_new > n_partitions_old:
            return 1.0 - n_partitions_old / n_partitions_new
        if n_partitions_new < n_partitions_old:
            return 1.0 - n_partitions_new / n_partitions_old
        return 0.0

    @staticmethod
    def graph_operator_partitions(graph: StateflowGraph) -> int:
        return max((op.n_partitions for op in graph.nodes.values()), default=1)

    def _planned_partition_counts(
        self, n_workers: int, n_standby: int, current_operator_partitions: int | None
    ) -> tuple[int, int]:
        """Pessimistic partition counts for the next possible scale-up."""
        max_to_add = min(n_workers, n_standby) if n_standby > 0 else 0
        n_partitions_old = current_operator_partitions if current_operator_partitions is not None else max(n_workers, 1)
        n_partitions_new = n_workers + max_to_add
        logging.debug(
            f"PLANNED PARTITION COUNTS | n_partitions_old={n_partitions_old} | n_partitions_new={n_partitions_new}"
        )
        return n_partitions_old, n_partitions_new

    def _expected_keys_to_move(
        self, total_keys: int, n_workers: int, n_standby: int, current_partitions: int | None
    ) -> float:
        """Keys expected to change partition for the next planned scale-up."""
        n_partitions_old, n_partitions_new = self._planned_partition_counts(n_workers, n_standby, current_partitions)
        return total_keys * self.f_migrate(n_partitions_old, n_partitions_new)

    def note_migration_duration(self, duration_sec: float) -> None:
        """Fold a completed migration into the learned per-moved-key rate.

        Normalizing by the number of keys actually moved lets a single learned
        rate generalize across migration sizes and directions, so we no longer
        depend on hand-tuned hashing/transfer constants.
        """
        keys_moved = self.migration_keys_to_move
        if keys_moved <= 0:
            # Nothing moved (e.g. same partition count) -> no rate signal.
            return
        sample = duration_sec / keys_moved
        if self.sec_per_moved_key_ewma is None:
            self.sec_per_moved_key_ewma = sample
        else:
            self.sec_per_moved_key_ewma += self.sec_per_moved_key_ewma_alpha * (sample - self.sec_per_moved_key_ewma)
        logging.warning(
            f"MIGRATION RATE | duration={duration_sec:.2f}s | keys_moved={keys_moved:.0f} | "
            f"sample={sample * 1e6:.2f}us/key | ewma={self.sec_per_moved_key_ewma * 1e6:.2f}us/key",
        )

    def estimate_migration_time(
        self, total_keys: int, n_workers: int, n_standby: int, current_partitions: int | None
    ) -> float:
        """Estimate the migration duration for the Chronos forecast horizon.

        Uses a single empirically learned cost per key actually moved. Before
        the first migration has been measured we fall back to a conservative
        cold-start constant; after that the estimate is fully data-driven.
        """
        keys_to_move = self._expected_keys_to_move(total_keys, n_workers, n_standby, current_partitions)

        if self.sec_per_moved_key_ewma is None or keys_to_move <= 0:
            horizon = MIGRATION_COLD_START_HORIZON_SEC
            logging.warning(
                f"MIGRATION ESTIMATE | horizon={horizon:.2f}s (cold-start) | "
                f"keys_to_move={keys_to_move:.0f} | learned_rate=None",
            )
        else:
            horizon = self.sec_per_moved_key_ewma * keys_to_move
            logging.warning(
                f"MIGRATION ESTIMATE | horizon={horizon:.2f}s | keys_to_move={keys_to_move:.0f} | "
                f"rate={self.sec_per_moved_key_ewma * 1e6:.2f}us/key",
            )
        return max(horizon, float(MIN_CHRONOS_PREDICTION_HORIZON))
