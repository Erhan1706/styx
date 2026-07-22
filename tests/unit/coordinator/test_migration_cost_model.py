"""Unit tests for coordinator/autoscaler/migration_cost_model.py"""

from types import SimpleNamespace

from autoscaler.migration_cost_model import (
    MIGRATION_COLD_START_HORIZON_SEC,
    MIN_CHRONOS_PREDICTION_HORIZON,
    MigrationCostModel,
)
import pytest


def _fake_graph(*partition_counts: int) -> SimpleNamespace:
    """A stand-in StateflowGraph: only .nodes[*].n_partitions is read."""
    nodes = {f"op{i}": SimpleNamespace(n_partitions=p) for i, p in enumerate(partition_counts)}
    return SimpleNamespace(nodes=nodes)


# ---------------------------------------------------------------------------
# f_migrate (fraction of keys that change partition)
# ---------------------------------------------------------------------------


class TestFMigrate:
    def test_scale_up(self):
        # doubling partitions rehashes half the keys
        assert MigrationCostModel.f_migrate(2, 4) == pytest.approx(0.5)

    def test_scale_down(self):
        assert MigrationCostModel.f_migrate(4, 2) == pytest.approx(0.5)

    def test_no_change_moves_nothing(self):
        assert MigrationCostModel.f_migrate(4, 4) == 0.0

    def test_scale_up_from_one(self):
        assert MigrationCostModel.f_migrate(1, 4) == pytest.approx(0.75)


# ---------------------------------------------------------------------------
# graph_operator_partitions
# ---------------------------------------------------------------------------


class TestGraphOperatorPartitions:
    def test_takes_max_across_operators(self):
        graph = _fake_graph(2, 8, 4)
        assert MigrationCostModel.graph_operator_partitions(graph) == 8

    def test_single_operator(self):
        assert MigrationCostModel.graph_operator_partitions(_fake_graph(3)) == 3

    def test_empty_graph_defaults_to_one(self):
        assert MigrationCostModel.graph_operator_partitions(_fake_graph()) == 1


# ---------------------------------------------------------------------------
# _planned_partition_counts
# ---------------------------------------------------------------------------


class TestPlannedPartitionCounts:
    def test_uses_current_partitions_when_known(self):
        model = MigrationCostModel()
        old, new = model._planned_partition_counts(n_workers=4, n_standby=2, current_operator_partitions=4)
        assert old == 4
        # can add min(n_workers, n_standby) = 2 -> 4 + 2
        assert new == 6

    def test_falls_back_to_worker_count_without_graph(self):
        model = MigrationCostModel()
        old, new = model._planned_partition_counts(n_workers=3, n_standby=1, current_operator_partitions=None)
        assert old == 3
        assert new == 4

    def test_no_standby_means_no_growth(self):
        model = MigrationCostModel()
        old, new = model._planned_partition_counts(n_workers=4, n_standby=0, current_operator_partitions=4)
        assert old == 4
        assert new == 4


# ---------------------------------------------------------------------------
# _expected_keys_to_move
# ---------------------------------------------------------------------------


class TestExpectedKeysToMove:
    def test_scales_total_keys_by_fraction(self):
        model = MigrationCostModel()
        # old=2, new=4 -> f_migrate=0.5 -> 1000 * 0.5
        keys = model._expected_keys_to_move(total_keys=1000, n_workers=2, n_standby=2, current_partitions=2)
        assert keys == pytest.approx(500.0)

    def test_zero_when_no_partition_change(self):
        model = MigrationCostModel()
        keys = model._expected_keys_to_move(total_keys=1000, n_workers=2, n_standby=0, current_partitions=2)
        assert keys == 0.0


# ---------------------------------------------------------------------------
# note_migration_duration (learned per-moved-key rate EWMA)
# ---------------------------------------------------------------------------


class TestNoteMigrationDuration:
    def test_first_sample_seeds_ewma(self):
        model = MigrationCostModel()
        model.migration_keys_to_move = 100
        model.note_migration_duration(10.0)  # 10s / 100 keys = 0.1 s/key
        assert model.sec_per_moved_key_ewma == pytest.approx(0.1)

    def test_second_sample_folds_with_alpha(self):
        model = MigrationCostModel()
        model.sec_per_moved_key_ewma_alpha = 0.5
        model.migration_keys_to_move = 100
        model.note_migration_duration(10.0)  # seed 0.1
        model.note_migration_duration(20.0)  # sample 0.2, ewma += 0.5*(0.2-0.1)
        assert model.sec_per_moved_key_ewma == pytest.approx(0.15)

    def test_no_keys_moved_is_noop(self):
        model = MigrationCostModel()
        model.migration_keys_to_move = 0.0
        model.note_migration_duration(10.0)
        assert model.sec_per_moved_key_ewma is None


# ---------------------------------------------------------------------------
# estimate_migration_time
# ---------------------------------------------------------------------------


class TestEstimateMigrationTime:
    def test_cold_start_before_any_measurement(self):
        model = MigrationCostModel()
        horizon = model.estimate_migration_time(total_keys=1000, n_workers=2, n_standby=2, current_partitions=2)
        assert horizon == pytest.approx(MIGRATION_COLD_START_HORIZON_SEC)

    def test_cold_start_when_no_keys_move_even_with_learned_rate(self):
        model = MigrationCostModel()
        model.sec_per_moved_key_ewma = 0.05
        # no standby -> no partition change -> keys_to_move == 0 -> cold start
        horizon = model.estimate_migration_time(total_keys=1000, n_workers=2, n_standby=0, current_partitions=2)
        assert horizon == pytest.approx(MIGRATION_COLD_START_HORIZON_SEC)

    def test_data_driven_estimate(self):
        model = MigrationCostModel()
        model.sec_per_moved_key_ewma = 0.05
        # old=2, new=4 -> 500 keys move -> 0.05 * 500 = 25s
        horizon = model.estimate_migration_time(total_keys=1000, n_workers=2, n_standby=2, current_partitions=2)
        assert horizon == pytest.approx(25.0)

    def test_floored_at_minimum_horizon(self):
        model = MigrationCostModel()
        model.sec_per_moved_key_ewma = 0.0001  # tiny rate -> tiny horizon
        horizon = model.estimate_migration_time(total_keys=1000, n_workers=2, n_standby=2, current_partitions=2)
        assert horizon == float(MIN_CHRONOS_PREDICTION_HORIZON)
