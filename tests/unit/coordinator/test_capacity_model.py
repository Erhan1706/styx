"""Unit tests for coordinator/autoscaler/capacity_model.py"""

from autoscaler.capacity_model import SystemCapacityEstimator, WorkerCapacityModel
import pytest

# ---------------------------------------------------------------------------
# WorkerCapacityModel
# ---------------------------------------------------------------------------


class TestWorkerCapacityModel:
    def test_estimate_none_without_data(self):
        model = WorkerCapacityModel(epoch_max_size=1000, min_batch_threshold=50)
        assert model.estimate_max_tps() is None

    def test_small_batches_ignored(self):
        model = WorkerCapacityModel(epoch_max_size=1000, min_batch_threshold=50)
        model.record(batch_size=10, epoch_latency_ms=100)  # below threshold
        assert model.estimate_max_tps() is None

    def test_first_valid_record_seeds_estimate(self):
        model = WorkerCapacityModel(epoch_max_size=1000, min_batch_threshold=50)
        model.record(batch_size=100, epoch_latency_ms=100)  # 1ms/txn -> 1000 tps
        assert model.estimate_max_tps() == pytest.approx(1000.0)

    def test_low_weight_sample_does_not_move_seeded_estimate(self):
        model = WorkerCapacityModel(epoch_max_size=1000, min_batch_threshold=50)
        model.record(batch_size=100, epoch_latency_ms=100)  # seed 1ms/txn
        # weight 0.1 < min_weight_threshold 0.5 -> ignored after seeding
        model.record(batch_size=100, epoch_latency_ms=200)
        assert model.estimate_max_tps() == pytest.approx(1000.0)

    def test_high_weight_sample_updates_estimate(self):
        model = WorkerCapacityModel(epoch_max_size=1000, min_batch_threshold=50, base_alpha=0.15)
        model.record(batch_size=600, epoch_latency_ms=600)  # seed 1ms/txn
        model.record(batch_size=800, epoch_latency_ms=1600)  # 2ms/txn, weight 0.8
        # ewma = 1 + (0.15*0.8)*(2-1) = 1.12 ms/txn -> ~892.86 tps
        assert model.estimate_max_tps() == pytest.approx(1000.0 / 1.12)

    def test_confidence_tracks_max_batch(self):
        model = WorkerCapacityModel(epoch_max_size=1000, min_batch_threshold=50)
        assert model.confidence == 0.0
        model.record(batch_size=250, epoch_latency_ms=250)
        assert model.confidence == pytest.approx(0.25)

    def test_reset_clears_state(self):
        model = WorkerCapacityModel(epoch_max_size=1000, min_batch_threshold=50)
        model.record(batch_size=250, epoch_latency_ms=250)
        model.reset()
        assert model.estimate_max_tps() is None
        assert model.confidence == 0.0


# ---------------------------------------------------------------------------
# SystemCapacityEstimator — aggregation
# ---------------------------------------------------------------------------


class TestSystemCapacityAggregation:
    def test_empty_returns_none_and_zero_confidence(self):
        est = SystemCapacityEstimator(sequence_max_size=1000)
        assert est.estimate_per_worker_capacity() is None
        assert est.estimate_system_capacity() is None
        assert est.confidence == 0.0

    def test_per_worker_capacity_is_median(self):
        est = SystemCapacityEstimator(sequence_max_size=1000)
        est.record(1, total_txns=200, epoch_latency_ms=200)  # 1000 tps
        est.record(2, total_txns=200, epoch_latency_ms=400)  # 500 tps
        assert est.estimate_per_worker_capacity() == pytest.approx(750.0)

    def test_system_capacity_scales_by_worker_count(self):
        est = SystemCapacityEstimator(sequence_max_size=1000)
        est.record(1, total_txns=200, epoch_latency_ms=200)
        est.record(2, total_txns=200, epoch_latency_ms=400)
        assert est.estimate_system_capacity() == pytest.approx(1500.0)

    def test_confidence_is_min_across_workers(self):
        est = SystemCapacityEstimator(sequence_max_size=1000)
        est.record(1, total_txns=250, epoch_latency_ms=250)  # conf 0.25
        est.record(2, total_txns=500, epoch_latency_ms=500)  # conf 0.50
        assert est.confidence == pytest.approx(0.25)

    def test_remove_worker_drops_model(self):
        est = SystemCapacityEstimator(sequence_max_size=1000)
        est.record(1, total_txns=200, epoch_latency_ms=200)
        est.record(2, total_txns=200, epoch_latency_ms=200)
        est.remove_worker(1)
        # only worker 2 remains -> system capacity == its per-worker cap * 1
        assert est.estimate_system_capacity() == pytest.approx(1000.0)


# ---------------------------------------------------------------------------
# SystemCapacityEstimator — capacity_for_workers / saturated observations
# ---------------------------------------------------------------------------


class TestCapacityForWorkers:
    def test_invalid_worker_count_returns_none(self):
        est = SystemCapacityEstimator(sequence_max_size=1000)
        assert est.capacity_for_workers(0) is None

    def test_exact_learned_value_preferred(self):
        est = SystemCapacityEstimator(sequence_max_size=1000)
        est.observe_saturated_capacity(4, 1000.0)
        assert est.capacity_for_workers(4) == pytest.approx(1000.0)

    def test_saturated_observation_is_smoothed(self):
        est = SystemCapacityEstimator(sequence_max_size=1000, capacity_by_n_alpha=0.3)
        est.observe_saturated_capacity(4, 1000.0)
        est.observe_saturated_capacity(4, 2000.0)  # 1000 + 0.3*(2000-1000)
        assert est.capacity_for_workers(4) == pytest.approx(1300.0)

    def test_extrapolates_sublinearly_from_nearest(self):
        est = SystemCapacityEstimator(sequence_max_size=1000, scaling_exponent=0.85)
        est.observe_saturated_capacity(4, 1000.0)
        # capacity(2) = 1000 * (2/4) ** 0.85
        assert est.capacity_for_workers(2) == pytest.approx(1000.0 * (0.5**0.85))

    def test_falls_back_to_per_worker_scaling(self):
        est = SystemCapacityEstimator(sequence_max_size=1000)
        est.record(1, total_txns=200, epoch_latency_ms=200)  # 1000 tps/worker
        assert est.capacity_for_workers(3) == pytest.approx(3000.0)
