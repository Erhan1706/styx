import logging
import time
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from autoscaler.capacity_model import SystemCapacityEstimator
    from autoscaler.sliding_window_metric import SlidingWindowMetric


class ScalingPolicy:
    def __init__(
        self,
        scale_cooldown_period: float,
        backlog_threshold: float,
        system_capacity_estimator: SystemCapacityEstimator,
        tps_sliding_window: SlidingWindowMetric,
    ) -> None:
        self.capacity_confidence_threshold: float = 0.25
        self.downscale_safety_factor: float = 0.90
        self.scale_cooldown_period: float = scale_cooldown_period
        self.last_scale_action_time: float = time.time()
        self.backlog_threshold: float = backlog_threshold

        self._downscale_suppressed_until: float = 0.0
        self._downscale_strikes: int = 0
        self._downscale_penalty_base: float = scale_cooldown_period * 2
        self._downscale_penalty_max: float = scale_cooldown_period * 30
        self._last_action_was_downscale: bool = False

        self.system_capacity_estimator = system_capacity_estimator
        self._capacity_ewma: float | None = None
        self._capacity_ewma_alpha: float = 0.3  # smoothing factor (0→slow, 1→no smoothing)
        self._last_epoch_total_backlog: float = 0.0
        self.tps_sliding_window: SlidingWindowMetric = tps_sliding_window

    def note_scale_action(self, is_downscale: bool) -> None:
        now = time.time()
        if not is_downscale and self._last_action_was_downscale:
            # scale-up is the first action after a scale-down => the down was wrong
            self._downscale_strikes += 1
            penalty = min(
                self._downscale_penalty_base * (2 ** (self._downscale_strikes - 1)),
                self._downscale_penalty_max,
            )
            self._downscale_suppressed_until = now + penalty
            logging.warning(
                f"DOWNSCALE | correction detected (strike #{self._downscale_strikes}); "
                f"suppressing downscale for {penalty:.0f}s"
            )
        elif is_downscale and now > self._downscale_suppressed_until and self._last_action_was_downscale:
            # long stretch with no correction -> forgive past strikes
            self._downscale_strikes = 0
        self._last_action_was_downscale = is_downscale

    def resolve_scale_up_workers(self, to_add: int, n_standby: int) -> int:
        """Scale up only activates workers from _standby_queue; clamp and skip if none left.
        Returns the number of workers to add.
        """
        if n_standby == 0:
            self.last_scale_action_time = time.time()
            logging.warning("SCALE UP | no standby workers available")
            return 0
        if to_add > n_standby:
            logging.warning(f"SCALE UP | not enough standby workers clamping to {n_standby} workers")
            return n_standby
        return to_add

    def compute_predictive_upscaling(
        self, predictions: dict[str, list[float]], n_workers: int, n_standby: int
    ) -> tuple[bool, int]:
        """Compare the Chronos forecast against the capacity model.
        Returns (should_scale, workers_to_add).
        """
        # Check confidence before using capacity estimate
        confidence = self.system_capacity_estimator.confidence
        if confidence < self.capacity_confidence_threshold:
            logging.warning(f"PREDICTIVE | low confidence={confidence:.2f}")
            return False, 0

        raw_capacity = self.system_capacity_estimator.estimate_system_capacity()
        if raw_capacity is None or (time.time() - self.last_scale_action_time < self.scale_cooldown_period):
            return False, 0
        if self._capacity_ewma is None:
            self._capacity_ewma = raw_capacity
        else:
            self._capacity_ewma += self._capacity_ewma_alpha * (raw_capacity - self._capacity_ewma)
        system_capacity = self._capacity_ewma
        peak_p75 = max(predictions.get("0.75", [0.0]))
        headroom_factor = 1
        effective_capacity = system_capacity * headroom_factor
        logging.warning(
            f"PREDICTIVE | confidence={confidence:.2f} | peak_p75={peak_p75:.0f} | "
            f"raw_capacity={raw_capacity:.0f} | effective={effective_capacity:.0f}"
        )
        if peak_p75 <= effective_capacity:
            return False, 0
        per_worker_capacity = system_capacity / max(n_workers, 1)
        n_needed = max(
            n_workers + 1,
            int(peak_p75 / (per_worker_capacity * headroom_factor)) + 1,
        )

        to_add = n_needed - n_workers
        # scale_up only activates workers from _standby_queue; clamp and skip if none left
        to_add = self.resolve_scale_up_workers(to_add, n_standby)
        if to_add <= 0:
            return False, 0

        logging.warning(f"PREDICTIVE | SCALE UP: need {n_needed} workers (currently {n_workers}, adding {to_add})")
        return True, to_add

    def compute_predictive_downscaling(
        self, predictions: dict[str, list[float]] | None, n_workers: int, migration_in_progress: bool
    ) -> tuple[bool, int]:
        """Check if the system can serve predicted demand with fewer workers.
        Returns (should_downscale, workers_to_remove).
        """
        if (
            time.time() - self.last_scale_action_time < self.scale_cooldown_period
            or migration_in_progress
            or time.time() < self._downscale_suppressed_until
        ):
            return False, 0
        # Backlog must be essentially zero before considering downscale,
        # use value a little above zero to account for timing jitter on the worker side
        if n_workers <= 1 or self._last_epoch_total_backlog >= self.backlog_threshold or self._capacity_ewma is None:
            return False, 0

        # Determine peak expected demand (pessimistic: use current rate and p90 forecast)
        peak_demand = self.tps_sliding_window.average() or 0.0
        if predictions:
            peak_demand = max(predictions.get("0.75", [0.0]))
        if peak_demand <= 0:
            return False, 0

        # Can n workers handle peak demand with headroom?
        to_remove = 0
        for n in range(n_workers - 1, 1, -1):
            capacity_n = self.system_capacity_estimator.capacity_for_workers(n)
            if capacity_n is None:
                break
            estimated_capacity = capacity_n * self.downscale_safety_factor
            logging.warning(f"SCALE DOWN: estimated_capacity={estimated_capacity:.0f} | peak_demand={peak_demand:.0f}")
            if peak_demand < estimated_capacity:
                logging.warning(f"SCALE DOWN: removing {n_workers - n} workers")
                to_remove = n_workers - n
            else:
                break

        perform_downscale = to_remove > 0
        return perform_downscale, to_remove
