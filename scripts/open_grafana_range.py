#!/usr/bin/env python3
"""
Helper script to quickly open the Styx Grafana dashboard for a given experiment run.

Workflow:
  - You pass a workload key like "ycsb" or "dhr"
  - The script scans the results/ directory for matching subdirectories
  - It reads each run's metadata.json (start/end timestamps and other info)
  - It shows you an indexed list of runs and lets you pick one
  - It then opens Grafana in your browser with from/to set to that run's time range

Assumptions:
  - Grafana is reachable at http://localhost:3001 (as in docker-compose.yml)
  - Dashboard UID is taken from grafana/dashboards/styx.json
"""

import argparse
import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import List
import webbrowser


REPO_ROOT = Path(__file__).parent.parent # Adjust if needed
RESULTS_DIR = REPO_ROOT / "results"

# From grafana/dashboards/styx.json
GRAFANA_BASE_URL = "http://localhost:3001"
OVERVIEW_DASHBOARD_UID = "beckc0nxpeupsf"
OVERVIEW_DASHBOARD_SLUG = "styx-system-overview"


@dataclass
class RunMetadata:
    dir_name: str
    path: Path
    workload: str
    messages_per_second: int | None
    n_partitions: int | None
    n_keys: int | None
    start: datetime
    end: datetime
    duration_s: float | None
    increase_interval: int | None
    increase_amount: int | None
    n_threads: int | None
    epoch_size: int | None
    extra: dict

class COLORS:
    RESET = "\033[0m"
    BOLD = "\033[1m"
    GREEN = "\033[32m"
    YELLOW = "\033[33m"
    CYAN = "\033[36m"

def load_metadata(run_dir: Path) -> RunMetadata | None:
    meta_path = run_dir / "metadata.json"
    if not meta_path.is_file():
        return None

    try:
        with meta_path.open() as f:
            data = json.load(f)
    except Exception as e:
        print(f"Skipping {run_dir.name}: failed to read metadata.json ({e})")
        return None

    # Required fields
    try:
        start_str = data["start"]
        end_str = data["end"]
        # Interpret naive timestamps as local time (matches how you type them into Grafana)
        start = datetime.fromisoformat(start_str)
        end = datetime.fromisoformat(end_str)
    except Exception as e:
        print(f"Skipping {run_dir.name}: invalid start/end in metadata.json ({e})")
        return None

    workload = str(data.get("workload", ""))
    mps = data.get("messages_per_second")
    n_partitions = data.get("n_partitions")
    n_keys = data.get("n_keys")
    duration_s = data.get("duration (s)")
    increase_interval = data.get("increase_interval")
    increase_amount = data.get("increase_amount")
    n_threads = data.get("n_threads")
    epoch_size = data.get("epoch_size")

    known_keys = {
        "workload",
        "messages_per_second",
        "n_partitions",
        "n_keys",
        "start",
        "end",
        "duration (s)",
        "increase_interval",
        "increase_amount",
        "n_threads",
        "epoch_size",
    }
    extra = {k: v for k, v in data.items() if k not in known_keys}

    return RunMetadata(
        dir_name=run_dir.name,
        path=run_dir,
        workload=workload,
        messages_per_second=mps,
        n_partitions=n_partitions,
        n_keys=n_keys,
        start=start,
        end=end,
        duration_s=duration_s,
        increase_interval=increase_interval,
        increase_amount=increase_amount,
        n_threads=n_threads,
        epoch_size=epoch_size,
        extra=extra,
    )


def find_runs_by_keyword(keywords: List[str]) -> List[RunMetadata]:
    keywords = [keyword.lower() for keyword in keywords]
    runs: List[RunMetadata] = []
    if not RESULTS_DIR.is_dir():
        print(f"Results directory not found: {RESULTS_DIR}")
        return runs

    for child in sorted(RESULTS_DIR.iterdir()):
        if not child.is_dir():
            continue
        # Basic matching: directory name contains the keyword (e.g. "ycsb", "dhr")
        if not all(keyword in child.name.lower() for keyword in keywords):
            continue
        meta = load_metadata(child)
        if meta:
            runs.append(meta)

    return runs


def format_run_line(idx: int, meta: RunMetadata) -> str:
    idx_part = f"{COLORS.YELLOW}[{idx:>2}]{COLORS.RESET}"
    name_part = f"{COLORS.BOLD}{meta.dir_name}{COLORS.RESET}"

    details: list[str] = []
    if meta.workload:
        details.append(f"workload={meta.workload}")
    if meta.messages_per_second is not None:
        details.append(f"tps={meta.messages_per_second}")
    if meta.n_partitions is not None:
        details.append(f"partitions={meta.n_partitions}")
    if meta.duration_s is not None:
        details.append(f"duration={meta.duration_s:.0f}s")
    if meta.n_threads is not None:
        details.append(f"threads={meta.n_threads}")
    if meta.increase_interval is not None:
        details.append(f"increase_interval={meta.increase_interval}s")
    if meta.increase_amount is not None:
        details.append(f"increase_amount={meta.increase_amount}")
    if meta.extra.get("zipf_const") is not None and meta.extra['zipf_const'] != 0:
        details.append(f"zipf={meta.extra['zipf_const']}")
    if meta.epoch_size is not None:
        details.append(f"epoch_size={meta.epoch_size}")
    start_str = meta.start.strftime("%Y-%m-%d %H:%M:%S")
    end_str = meta.end.strftime("%H:%M:%S")
    time_part = f"{start_str} → {end_str}{COLORS.RESET}"

    return f"{idx_part} {name_part}  ({', '.join(details)})  {time_part}"


def datetime_to_epoch_ms(dt: datetime) -> int:
    # For naive datetimes, timestamp() assumes local time, which matches user behavior in Grafana UI.
    return int(dt.timestamp() * 1000)


def build_grafana_url(start: datetime, end: datetime) -> str:
    from_ms = datetime_to_epoch_ms(start)
    to_ms = datetime_to_epoch_ms(end)
    return (
        f"{GRAFANA_BASE_URL}/d/"
        f"{OVERVIEW_DASHBOARD_UID}/"
        f"{OVERVIEW_DASHBOARD_SLUG}"
        f"?from={from_ms}&to={to_ms}"
    )


def interactive_select(runs: List[RunMetadata]) -> RunMetadata | None:
    if not runs:
        print("No matching runs found.")
        return None

    print("Matching runs:")
    for i, meta in enumerate(runs, start=1):
        print("  " + format_run_line(i, meta))

    while True:
        choice = input("\nSelect run number to open in Grafana (or 'q' to quit): ").strip()
        if choice.lower() in {"q", "quit", "exit"}:
            return None

        if not choice.isdigit():
            print("Please enter a valid number or 'q'.")
            continue

        idx = int(choice)
        if not (1 <= idx <= len(runs)):
            print(f"Please enter a number between 1 and {len(runs)}.")
            continue

        return runs[idx - 1]


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Open Styx Grafana dashboard for an experiment run based on metadata.json."
    )
    parser.add_argument(
        "keyword",
        nargs="+",
        help="Keywords to filter runs by directory name (e.g. 'ycsb', 'dhr'). Or by number of partitions used (e.g. '2part', '4part').",
    )
    args = parser.parse_args()

    while True:
        runs = find_runs_by_keyword(args.keyword)
        selected = interactive_select(runs)
        if not selected:
            return

        url = build_grafana_url(selected.start, selected.end)
        print(f"\nOpening Grafana URL:\n  {url}\n")
        webbrowser.open(url)


if __name__ == "__main__":
    main()


