from __future__ import annotations


class WorkerCapacityModel:
    """Estimates per-worker max throughput via batch-weighted EWMA of per-txn cost."""

    def __init__(
        self,
        epoch_max_size: int,
        min_batch_threshold: int = 50,
        base_alpha: float = 0.15,
    ) -> None:
        self.epoch_max_size = epoch_max_size
        self.min_batch_threshold = min_batch_threshold
        self.base_alpha = base_alpha
        self.min_weight_threshold = 0.5

        self._per_txn_cost_ewma: float | None = None
        self._max_observed_batch: int = 0

    def record(self, batch_size: int, epoch_latency_ms: float) -> None:
        """Record an epoch's metrics and update the per-txn cost EWMA.
        Args:
            batch_size: Number of transactions processed in this epoch (total_txns)
            epoch_latency_ms: Total epoch latency in milliseconds
        """
        if batch_size < self.min_batch_threshold or epoch_latency_ms <= 0 or batch_size == 0:
            return

        per_txn_cost = epoch_latency_ms / batch_size

        # Track max observed batch for confidence calculation
        self._max_observed_batch = max(self._max_observed_batch, batch_size)

        # Weight the EWMA alpha by batch size -- larger batches (more accurate)
        # should dominate the estimate
        weight = min(batch_size / self.epoch_max_size, 1.0)
        effective_alpha = self.base_alpha * weight

        if self._per_txn_cost_ewma is None:
            self._per_txn_cost_ewma = per_txn_cost
        else:
            # Small batches are poor capacity estimates - ignore them.
            if weight < self.min_weight_threshold:
                return
            self._per_txn_cost_ewma += effective_alpha * (per_txn_cost - self._per_txn_cost_ewma)

    def estimate_max_tps(self) -> float | None:
        """Estimate maximum sustainable TPS for this worker.
        Returns:
            Estimated max TPS, or None if no data recorded yet.
        """
        if self._per_txn_cost_ewma is None or self._per_txn_cost_ewma <= 0:
            return None
        return 1000.0 / self._per_txn_cost_ewma  # ms to second conversion

    @property
    def confidence(self) -> float:
        """Confidence score based on max observed batch size.
        Returns a value in [0, 1] indicating how reliable the estimate is.
        At confidence=0.25 (batch=250/1000), predictive scaling can kick in.
        """
        if self._max_observed_batch == 0:
            return 0.0
        return min(self._max_observed_batch / self.epoch_max_size, 1.0)

    def reset(self) -> None:
        """Reset the model state."""
        self._per_txn_cost_ewma = None
        self._max_observed_batch = 0


class SystemCapacityEstimator:
    """Aggregates per-worker models into a system-level capacity estimate."""

    def __init__(
        self,
        sequence_max_size: int = 1000,
        min_batch_threshold: int = 50,
        base_alpha: float = 0.15,
        scaling_exponent: float = 0.85,
        capacity_by_n_alpha: float = 0.3,
    ) -> None:
        self.sequence_max_size = sequence_max_size
        self.min_batch_threshold = min_batch_threshold
        self.base_alpha = base_alpha
        self._models: dict[int, WorkerCapacityModel] = {}

        self._capacity_by_n: dict[int, float] = {}
        self._capacity_by_n_alpha = capacity_by_n_alpha
        self.scaling_exponent = scaling_exponent

    def get_model(self, worker_id: int) -> WorkerCapacityModel:
        if worker_id not in self._models:
            self._models[worker_id] = WorkerCapacityModel(
                self.sequence_max_size,
                self.sequence_max_size // 10,
                self.base_alpha,
            )
        return self._models[worker_id]

    def record(
        self,
        worker_id: int,
        total_txns: int,
        epoch_latency_ms: float,
    ) -> None:
        """Record epoch metrics for a worker.
        Args:
            worker_id: The worker ID
            total_txns: Total transactions processed in this epoch
            epoch_latency_ms: Total epoch latency in milliseconds
        """
        self.get_model(worker_id).record(total_txns, epoch_latency_ms)

    @staticmethod
    def _median(values: list[float]) -> float:
        ordered = sorted(values)
        n = len(ordered)
        mid = n // 2
        if n % 2 == 1:
            return ordered[mid]
        return (ordered[mid - 1] + ordered[mid]) / 2.0

    def estimate_per_worker_capacity(self) -> float | None:
        """Median per-worker max TPS across all workers.

        Median (instead of min) prevents a single unlucky/noisy worker epoch
        from dominating the whole-system estimate and being amplified by N.
        """
        if not self._models:
            return None
        per_worker = [est for m in self._models.values() if (est := m.estimate_max_tps()) is not None]
        if not per_worker:
            return None
        return self._median(per_worker)

    def estimate_system_capacity(self) -> float | None:
        """Return estimated total system TPS across all workers.
        Uses the minimum per-worker capacity (the bottleneck) multiplied
        by the number of workers.
        """
        n_workers = len(self._models)
        per_worker_cap = self.estimate_per_worker_capacity()
        if per_worker_cap is None:
            return None
        return per_worker_cap * n_workers

    def observe_saturated_capacity(self, n_workers: int, achieved_tps: float) -> None:
        """Add an observed saturated system TPS into the per-N capacity table.
        Should only be called by the coordinator when the cluster is actually the
        bottleneck (backlog high, not migrating), so achieved TPS ~= capacity.
        """
        if n_workers <= 0 or achieved_tps <= 0:
            return
        current = self._capacity_by_n.get(n_workers)
        if current is None:
            self._capacity_by_n[n_workers] = achieved_tps
        else:
            self._capacity_by_n[n_workers] = current + self._capacity_by_n_alpha * (achieved_tps - current)

    def capacity_for_workers(self, n_workers: int) -> float | None:
        """Best estimate of total system TPS achievable with n_workers"""
        if n_workers <= 0:
            return None

        if n_workers in self._capacity_by_n:
            return self._capacity_by_n[n_workers]

        if self._capacity_by_n:
            ref_n = min(self._capacity_by_n, key=lambda k: abs(k - n_workers))
            ref_cap = self._capacity_by_n[ref_n]
            # Sub-linear: capacity(n) ~= capacity(ref) * (n / ref) ** exponent
            return ref_cap * (n_workers / ref_n) ** self.scaling_exponent

        per_worker_cap = self.estimate_per_worker_capacity()
        if per_worker_cap is None:
            return None
        return per_worker_cap * n_workers

    @property
    def confidence(self) -> float:
        """System-level confidence as the minimum worker confidence"""
        if not self._models:
            return 0.0

        confidences = [model.confidence for model in self._models.values()]
        if not confidences:
            return 0.0

        return min(confidences)

    def remove_worker(self, worker_id: int) -> None:
        self._models.pop(worker_id, None)
