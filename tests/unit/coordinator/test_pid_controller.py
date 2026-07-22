"""Unit tests for coordinator/autoscaler/pid_controller.py"""

from autoscaler.pid_controller import BacklogPIDController
import pytest

# ---------------------------------------------------------------------------
# compute — gating conditions
# ---------------------------------------------------------------------------


class TestComputeGating:
    def test_zero_tps_returns_zero(self):
        pid = BacklogPIDController(backlog_threshold=10.0)
        assert pid.compute(total_backlog=100.0, smoothed_tps=0.0) == 0.0

    def test_backlog_below_threshold_returns_zero(self):
        pid = BacklogPIDController(backlog_threshold=10.0)
        assert pid.compute(total_backlog=5.0, smoothed_tps=100.0) == 0.0

    def test_backlog_at_threshold_returns_zero(self):
        pid = BacklogPIDController(backlog_threshold=10.0)
        assert pid.compute(total_backlog=10.0, smoothed_tps=100.0) == 0.0


# ---------------------------------------------------------------------------
# compute — control output
# ---------------------------------------------------------------------------


class TestComputeOutput:
    def test_first_call_is_proportional_only(self):
        pid = BacklogPIDController(kp=0.1, ki=0.001, kd=0.1, backlog_threshold=10.0)
        # error = 100/10 = 10; first call seeds prev_error so I and D terms are 0
        out = pid.compute(total_backlog=100.0, smoothed_tps=10.0)
        assert out == pytest.approx(0.1 * 10.0)

    def test_rising_backlog_grows_output(self):
        pid = BacklogPIDController(backlog_threshold=10.0)
        first = pid.compute(total_backlog=100.0, smoothed_tps=10.0)
        second = pid.compute(total_backlog=200.0, smoothed_tps=10.0)
        # error rose 10 -> 20, so P/I/D all push the signal higher
        assert second > first

    def test_integral_is_clamped(self):
        pid = BacklogPIDController(integral_max=100.0, backlog_threshold=10.0)
        for _ in range(1000):
            pid.compute(total_backlog=10_000.0, smoothed_tps=1.0)
        assert pid.integral <= pid.integral_max

    def test_prev_error_tracked_across_calls(self):
        pid = BacklogPIDController(backlog_threshold=10.0)
        pid.compute(total_backlog=100.0, smoothed_tps=10.0)
        assert pid.prev_error == pytest.approx(10.0)
