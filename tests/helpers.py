import contextlib
import os
from pathlib import Path
import re
import socket
import subprocess
import threading
import time
import urllib.error
import urllib.request

# S3 defaults for local development (RustFS via docker-compose-s3.yml).
_S3_DEFAULTS = {
    "S3_ENDPOINT": "http://localhost:9000",
    "S3_ACCESS_KEY": "rustfsadmin",
    "S3_SECRET_KEY": "rustfsadmin",
    "S3_REGION": "us-east-1",
}


def make_test_env() -> dict:
    """Return a copy of os.environ with S3 vars forced to local defaults.

    Uses explicit assignment (not setdefault) so that stale or empty values
    injected by other conftest files (e.g. coordinator conftest sets
    S3_ACCESS_KEY=testkey) are always overwritten with the correct values for
    the local RustFS instance.
    """
    env = os.environ.copy()
    env.update(_S3_DEFAULTS)
    return env


def resolve_run_dir(results_dir: Path) -> Path:
    results_dir = Path(results_dir)
    if (results_dir / "client_requests.csv").exists():
        return results_dir

    subdirs = [p for p in results_dir.iterdir() if p.is_dir()] if results_dir.is_dir() else []
    if not subdirs:
        raise AssertionError(f"No artifacts and no run sub-directory under {results_dir}")
    return max(subdirs, key=lambda p: p.stat().st_mtime)


def wait_port(host: str, port: int, timeout_s: float = 120.0) -> None:
    deadline = time.time() + timeout_s
    last_err = None
    while time.time() < deadline:
        try:
            with socket.create_connection((host, port), timeout=1.0):
                return
        except OSError as e:
            last_err = e
            time.sleep(0.5)
    raise TimeoutError(f"Timed out waiting for {host}:{port} (last error: {last_err})")


COORDINATOR_METRICS_URL = "http://localhost:8000/metrics"
_LIVE_WORKERS_RE = re.compile(r"^live_worker_count\s+([0-9.eE+-]+)$", re.MULTILINE)


def wait_for_workers(
    expected: int,
    *,
    metrics_url: str = COORDINATOR_METRICS_URL,
    timeout_s: float = 300.0,
    log=None,
) -> None:
    """Block until the coordinator reports at least *expected* registered workers.

    The demo clients publish their initial state to S3 as snapshot 0 and then send
    InitDataComplete, which only promotes workers the coordinator already knows about.
    A client that starts before the workers have registered therefore has its
    notification silently dropped, and every worker boots with empty state, so waiting
    for the coordinator port alone is not enough.
    """
    deadline = time.time() + timeout_s
    last_seen: float | None = None
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(metrics_url, timeout=5) as resp:  # noqa: S310
                body = resp.read().decode("utf-8", errors="replace")
        except OSError, urllib.error.URLError:
            time.sleep(1)
            continue

        match = _LIVE_WORKERS_RE.search(body)
        if match is not None:
            last_seen = float(match.group(1))
            if last_seen >= expected:
                if log is not None:
                    log.info("Coordinator reports %s registered worker(s).", int(last_seen))
                return
        if log is not None:
            log.info("Waiting for %s workers to register (coordinator reports %s)...", expected, last_seen)
        time.sleep(1)

    raise TimeoutError(
        f"Timed out after {timeout_s}s waiting for {expected} workers to register "
        f"(coordinator last reported {last_seen}). Starting the client now would lose "
        f"the InitDataComplete notification and leave every worker with empty state.",
    )


def run_and_stream(cmd, *, cwd: str, env: dict, timeout: float, banner: str, log):
    log.info("=" * 100)
    log.info("RUNNING: %s", banner)
    log.info("CMD: %s", " ".join(cmd))
    log.info("CWD: %s", cwd)
    log.info("=" * 100)

    proc = subprocess.Popen(
        cmd,
        cwd=cwd,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )

    output_lines: list[str] = []

    try:
        assert proc.stdout is not None
        start = time.time()
        for line in proc.stdout:
            output_lines.append(line)
            log.info("[subprocess] %s", line.rstrip("\n"))

            if (time.time() - start) > timeout:
                proc.kill()
                raise TimeoutError(f"Timeout while running {banner} (>{timeout}s)")
    finally:
        with contextlib.suppress(Exception):
            proc.stdout.close() if proc.stdout else None

    rc = proc.wait(timeout=30)

    log.info("=" * 100)
    log.info("FINISHED: %s (rc=%s)", banner, rc)
    log.info("=" * 100)

    return rc, "".join(output_lines)


def run_and_stream_with_timed_action(
    cmd,
    *,
    cwd: str,
    env: dict,
    timeout: float,
    banner: str,
    action_at_s: float,
    action_cmd: list[str],
    action_banner: str,
    log,
):
    """
    Streams stdout like run_and_stream, but triggers action_cmd at action_at_s seconds
    after process start, regardless of stdout activity.
    """
    log.info("=" * 100)
    log.info("RUNNING: %s", banner)
    log.info("CMD: %s", " ".join(cmd))
    log.info("CWD: %s", cwd)
    log.info("=" * 100)

    proc = subprocess.Popen(
        cmd,
        cwd=cwd,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )

    output_lines: list[str] = []
    action_done = threading.Event()
    action_result = {"rc": None, "out": ""}

    def _timer_action():
        # Sleep independently of stdout streaming
        time.sleep(action_at_s)
        if proc.poll() is not None:
            # Process already finished; nothing to do.
            action_done.set()
            return

        log.info("=" * 100)
        log.info("TIMED ACTION: %s", action_banner)
        log.info("CMD: %s", " ".join(action_cmd))
        log.info("=" * 100)

        run = subprocess.run(
            action_cmd,
            cwd=cwd,
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )
        action_result["rc"] = run.returncode
        action_result["out"] = run.stdout or ""

        log.info("TIMED ACTION FINISHED: rc=%s", action_result["rc"])
        if action_result["out"].strip():
            for l_line in action_result["out"].splitlines():
                log.info("[timed-action] %s", l_line)

        action_done.set()

    t = threading.Thread(target=_timer_action, name="e2e-timed-action", daemon=True)
    t.start()

    try:
        assert proc.stdout is not None
        start = time.time()

        for line in proc.stdout:
            output_lines.append(line)
            log.info("[subprocess] %s", line.rstrip("\n"))

            if (time.time() - start) > timeout:
                proc.kill()
                raise TimeoutError(f"Timeout while running {banner} (>{timeout}s)")

    finally:
        with contextlib.suppress(Exception):
            proc.stdout.close() if proc.stdout else None

    rc = proc.wait(timeout=30)

    # Ensure the action thread has had a chance to run (or decide not to)
    action_done.wait(timeout=5)

    log.info("=" * 100)
    log.info("FINISHED: %s (rc=%s)", banner, rc)
    if action_result["rc"] is None:
        log.info(
            "TIMED ACTION SUMMARY: %s was NOT run (process ended before %ss)",
            action_banner,
            action_at_s,
        )
    else:
        log.info("TIMED ACTION SUMMARY: %s rc=%s", action_banner, action_result["rc"])
    log.info("=" * 100)

    # If the timed action ran and failed, fail the test even if the client returned 0
    if action_result["rc"] not in (None, 0):
        raise AssertionError(
            f"Timed action failed (rc={action_result['rc']}): {action_banner}\nOutput:\n{action_result['out']}",
        )

    return rc, "".join(output_lines)
