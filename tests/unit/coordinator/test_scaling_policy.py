"""Unit tests for coordinator/autoscaler/scaling_policy.py"""

import time
from unittest.mock import MagicMock

from autoscaler.scaling_policy import ScalingPolicy
from autoscaler.sliding_window_metric import SlidingWindowMetric

COOLDOWN = 30.0
BACKLOG_THRESHOLD = 10.0


def _make_policy(
    *,
    confidence: float = 0.5,
    system_capacity: float | None = 1000.0,
    capacity_for_workers: float | None = 1000.0,
) -> ScalingPolicy:
    """Build a policy with a stubbed capacity estimator and empty tps window."""
    estimator = MagicMock()
    estimator.confidence = confidence
    estimator.estimate_system_capacity.return_value = system_capacity
    estimator.capacity_for_workers.return_value = capacity_for_workers
    policy = ScalingPolicy(
        scale_cooldown_period=COOLDOWN,
        backlog_threshold=BACKLOG_THRESHOLD,
        system_capacity_estimator=estimator,
        tps_sliding_window=SlidingWindowMetric(10),
    )
    # Bypass the cooldown gate by default (last action far in the past).
    policy.last_scale_action_time = 0.0
    return policy


# ---------------------------------------------------------------------------
# note_scale_action (downscale anti-flapping hysteresis)
# ---------------------------------------------------------------------------


class TestNoteScaleAction:
    def test_downscale_alone_adds_no_strike(self):
        policy = _make_policy()
        policy.note_scale_action(is_downscale=True)
        assert policy._downscale_strikes == 0
        assert policy._last_action_was_downscale is True

    def test_upscale_after_downscale_is_a_strike(self):
        policy = _make_policy()
        policy.note_scale_action(is_downscale=True)
        policy.note_scale_action(is_downscale=False)
        assert policy._downscale_strikes == 1
        assert policy._downscale_suppressed_until > 0.0

    def test_repeated_corrections_back_off_exponentially(self):
        policy = _make_policy()
        policy.note_scale_action(is_downscale=True)
        policy.note_scale_action(is_downscale=False)
        first_suppression = policy._downscale_suppressed_until
        policy.note_scale_action(is_downscale=True)
        policy.note_scale_action(is_downscale=False)
        assert policy._downscale_strikes == 2
        # second penalty window extends further than the first
        assert policy._downscale_suppressed_until > first_suppression

    def test_penalty_is_capped(self):
        policy = _make_policy()
        policy._downscale_strikes = 50  # would explode without the cap
        policy._last_action_was_downscale = True
        before = time.time()
        policy.note_scale_action(is_downscale=False)
        # suppression = now + penalty; the penalty itself must not exceed the cap
        penalty = policy._downscale_suppressed_until - before
        assert penalty <= policy._downscale_penalty_max + 1.0

    def test_clean_stretch_forgives_strikes(self):
        policy = _make_policy()
        policy._downscale_strikes = 3
        policy._last_action_was_downscale = True
        policy._downscale_suppressed_until = 0.0  # suppression already elapsed
        policy.note_scale_action(is_downscale=True)
        assert policy._downscale_strikes == 0


# ---------------------------------------------------------------------------
# resolve_scale_up_workers (clamp to available standby capacity)
# ---------------------------------------------------------------------------


class TestResolveScaleUpWorkers:
    def test_no_standby_returns_zero(self):
        policy = _make_policy()
        assert policy.resolve_scale_up_workers(to_add=3, n_standby=0) == 0

    def test_clamped_to_standby_count(self):
        policy = _make_policy()
        assert policy.resolve_scale_up_workers(to_add=5, n_standby=3) == 3

    def test_returned_unchanged_when_enough_standby(self):
        policy = _make_policy()
        assert policy.resolve_scale_up_workers(to_add=2, n_standby=5) == 2


# ---------------------------------------------------------------------------
# compute_predictive_upscaling
# ---------------------------------------------------------------------------


class TestComputePredictiveUpscaling:
    def test_low_confidence_skips(self):
        policy = _make_policy(confidence=0.1)  # below 0.25 threshold
        should_scale, to_add = policy.compute_predictive_upscaling({"0.75": [5000]}, n_workers=2, n_standby=2)
        assert should_scale is False
        assert to_add == 0

    def test_no_capacity_estimate_skips(self):
        policy = _make_policy(system_capacity=None)
        should_scale, _to_add = policy.compute_predictive_upscaling({"0.75": [5000]}, n_workers=2, n_standby=2)
        assert should_scale is False

    def test_cooldown_blocks(self):
        policy = _make_policy()
        policy.last_scale_action_time = time.time()  # just acted
        should_scale, _ = policy.compute_predictive_upscaling({"0.75": [5000]}, n_workers=2, n_standby=2)
        assert should_scale is False

    def test_demand_below_capacity_skips(self):
        policy = _make_policy(system_capacity=1000.0)
        should_scale, to_add = policy.compute_predictive_upscaling({"0.75": [500]}, n_workers=2, n_standby=2)
        assert should_scale is False
        assert to_add == 0

    def test_scales_up_and_clamps_to_standby(self):
        policy = _make_policy(system_capacity=1000.0)
        # peak 2000 vs capacity 1000, per-worker 500 -> need 5 workers -> add 3, clamp to 2
        should_scale, to_add = policy.compute_predictive_upscaling({"0.75": [2000]}, n_workers=2, n_standby=2)
        assert should_scale is True
        assert to_add == 2

    def test_missing_quantile_defaults_to_zero_demand(self):
        policy = _make_policy(system_capacity=1000.0)
        should_scale, _to_add = policy.compute_predictive_upscaling({}, n_workers=2, n_standby=2)
        assert should_scale is False


# ---------------------------------------------------------------------------
# compute_predictive_downscaling
# ---------------------------------------------------------------------------


class TestComputePredictiveDownscaling:
    def _ready_policy(self) -> ScalingPolicy:
        policy = _make_policy(capacity_for_workers=1000.0)
        policy._capacity_ewma = 1000.0  # downscale requires a known capacity
        policy._last_epoch_total_backlog = 0.0
        return policy

    def test_cooldown_blocks(self):
        policy = self._ready_policy()
        policy.last_scale_action_time = time.time()
        should, to_remove = policy.compute_predictive_downscaling(
            {"0.75": [100]}, n_workers=4, migration_in_progress=False
        )
        assert should is False
        assert to_remove == 0

    def test_migration_blocks(self):
        policy = self._ready_policy()
        should, _ = policy.compute_predictive_downscaling({"0.75": [100]}, n_workers=4, migration_in_progress=True)
        assert should is False

    def test_active_suppression_blocks(self):
        policy = self._ready_policy()
        policy._downscale_suppressed_until = time.time() + 1000
        should, _ = policy.compute_predictive_downscaling({"0.75": [100]}, n_workers=4, migration_in_progress=False)
        assert should is False

    def test_single_worker_never_downscales(self):
        policy = self._ready_policy()
        should, _ = policy.compute_predictive_downscaling({"0.75": [100]}, n_workers=1, migration_in_progress=False)
        assert should is False

    def test_backlog_present_blocks(self):
        policy = self._ready_policy()
        policy._last_epoch_total_backlog = BACKLOG_THRESHOLD + 1
        should, _ = policy.compute_predictive_downscaling({"0.75": [100]}, n_workers=4, migration_in_progress=False)
        assert should is False

    def test_no_capacity_estimate_blocks(self):
        policy = self._ready_policy()
        policy._capacity_ewma = None
        should, _ = policy.compute_predictive_downscaling({"0.75": [100]}, n_workers=4, migration_in_progress=False)
        assert should is False

    def test_low_demand_downscales(self):
        policy = self._ready_policy()
        # capacity_for_workers=1000 * safety 0.9 = 900 >> peak 100 for every N
        should, to_remove = policy.compute_predictive_downscaling(
            {"0.75": [100]}, n_workers=4, migration_in_progress=False
        )
        assert should is True
        assert to_remove == 2  # can shrink down to the 2-worker floor

    def test_high_demand_keeps_workers(self):
        policy = self._ready_policy()
        policy.system_capacity_estimator.capacity_for_workers.return_value = 100.0
        should, to_remove = policy.compute_predictive_downscaling(
            {"0.75": [5000]}, n_workers=4, migration_in_progress=False
        )
        assert should is False
        assert to_remove == 0

    def test_falls_back_to_tps_window_without_predictions(self):
        policy = self._ready_policy()
        policy.tps_sliding_window.add(100.0)
        should, to_remove = policy.compute_predictive_downscaling(None, n_workers=4, migration_in_progress=False)
        assert should is True
        assert to_remove == 2
