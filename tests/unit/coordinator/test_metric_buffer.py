"""Unit tests for coordinator/autoscaler/metric_buffer.py"""

import time

from autoscaler.metric_buffer import AggregatingMetricBuffer, MetricBuffer

# ---------------------------------------------------------------------------
# MetricBuffer — bounded ring buffer of named series
# ---------------------------------------------------------------------------


class TestMetricBuffer:
    def test_add_and_snapshot(self):
        buf = MetricBuffer()
        buf.add("input_rate", 1.0)
        buf.add("input_rate", 2.0)
        assert buf.snapshot()["input_rate"] == [1.0, 2.0]

    def test_length(self):
        buf = MetricBuffer()
        buf.add("x", 1.0)
        buf.add("x", 2.0)
        assert buf.length("x") == 2
        assert buf.length("missing") == 0

    def test_bounded_by_max_len(self):
        buf = MetricBuffer(max_len=2)
        buf.add("x", 1.0)
        buf.add("x", 2.0)
        buf.add("x", 3.0)  # evicts the oldest
        assert buf.snapshot()["x"] == [2.0, 3.0]

    def test_snapshot_with_timestamps(self):
        buf = MetricBuffer()
        buf.add("x", 5.0, ts=123.0)
        assert buf.snapshot_with_timestamps()["x"] == [(123.0, 5.0)]

    def test_independent_series(self):
        buf = MetricBuffer()
        buf.add("a", 1.0)
        buf.add("b", 2.0)
        snap = buf.snapshot()
        assert snap["a"] == [1.0]
        assert snap["b"] == [2.0]


# ---------------------------------------------------------------------------
# AggregatingMetricBuffer — per-bucket accumulation + finalization
# ---------------------------------------------------------------------------


class TestAggregatingMetricBuffer:
    def test_current_bucket_stays_pending(self):
        buf = AggregatingMetricBuffer(bucket_interval=1.0)
        buf.add("r", 5.0)  # lands in the live bucket -> not yet finalized
        assert buf.snapshot() == {}
        assert buf.length("r") == 0

    def test_past_bucket_is_finalized(self):
        buf = AggregatingMetricBuffer(bucket_interval=1.0)
        buf.add("r", 5.0, ts=time.time() - 100)  # older than current bucket
        assert buf.snapshot()["r"] == [5.0]
        assert buf.length("r") == 1

    def test_values_in_same_bucket_are_summed(self, monkeypatch):
        # Freeze time so both samples share one live bucket, then advance to finalize.
        fake_now = {"t": 1000.0}
        monkeypatch.setattr(time, "time", lambda: fake_now["t"])

        buf = AggregatingMetricBuffer(bucket_interval=1.0)
        buf.add("r", 5.0)  # bucket 1000
        buf.add("r", 3.0)  # same bucket 1000 -> accumulates
        assert buf.snapshot() == {}  # still pending

        fake_now["t"] = 1002.0  # advance past the bucket
        buf.add("r", 1.0)  # bucket 1002 -> finalizes bucket 1000
        assert buf.snapshot()["r"] == [8.0]

    def test_snapshot_with_timestamps_scales_bucket(self):
        buf = AggregatingMetricBuffer(bucket_interval=2.0)
        buf.add("r", 7.0, ts=time.time() - 100)
        series = buf.snapshot_with_timestamps()["r"]
        assert len(series) == 1
        bucket_ts, value = series[0]
        assert value == 7.0
        # timestamp is bucket index * interval
        assert bucket_ts % 2.0 == 0.0
