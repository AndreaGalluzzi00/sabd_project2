#!/usr/bin/env python3
"""Compare Q3 merged output completeness against a baseline experiment."""

from __future__ import annotations

import argparse
import csv
import sys
from datetime import datetime, timezone
from pathlib import Path

from merge_utils import (
    PROJECT_ROOT,
    add_experiment_name_to_output_file,
    configure_config_path,
    get_experiment_name,
    resolve_project_path,
)

# Merged CSV header written by merge_q3.py: ts,airline,hour,count,min,...
COUNT_COLUMN = "count"

# Window label -> config key of its merged output path (same keys as merge_q3).
WINDOW_PATH_KEYS = {
    "1d": "q3_merged_output_host_path_1d",
    "7d": "q3_merged_output_host_path_7d",
    "global": "q3_merged_output_host_path_global",
    "cumulative": "q3_merged_output_host_path_cumulative",
}
Q3_WINDOW_CHOICES = ("1d", "7d", "global", "cumulative", "all")

DEFAULT_BASELINE = "01_baseline"
DEFAULT_OUTPUT = PROJECT_ROOT / "Results" / "late_drops_q3.csv"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__.strip().splitlines()[0])

    parser.add_argument(
        "--experiment",
        "--exp",
        "-e",
        type=str,
        default=None,
        dest="experiment",
        help="Experiment name under config/experiments (default: base).",
    )

    parser.add_argument(
        "--baseline",
        type=str,
        default=DEFAULT_BASELINE,
        help=(
            "Baseline experiment to compare against, i.e. the run with no "
            f"injected delay (default: {DEFAULT_BASELINE})."
        ),
    )

    parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_OUTPUT,
        help=f"CSV file the per-window rows are appended to (default: {DEFAULT_OUTPUT}).",
    )

    return parser.parse_args()


def selected_q3_window(cfg: dict) -> str:
    window = str(cfg.get("q3", {}).get("window", "all")).strip().lower()
    if window not in Q3_WINDOW_CHOICES:
        raise ValueError(
            "q3.window must be one of: "
            + ", ".join(Q3_WINDOW_CHOICES)
        )
    return window


def resolve_window_files(experiment: str | None) -> tuple[str, dict[str, Path]]:
    """Return (experiment_name, {window_label: merged_csv_path}).

    Mirrors merge_q3.load_merge_config so the paths match byte-for-byte what
    the merge produced: same config keys, same experiment suffix.
    """
    configure_config_path(experiment)

    from common.config import load_config  # noqa: E402  (needs CONFIG_PATH set first)

    cfg = load_config()
    paths = cfg["paths"]
    experiment_name = get_experiment_name(cfg)
    selected_window = selected_q3_window(cfg)
    path_keys = {
        label: key
        for label, key in WINDOW_PATH_KEYS.items()
        if label != "cumulative"
    }
    if selected_window != "all":
        path_keys = {selected_window: WINDOW_PATH_KEYS[selected_window]}

    files = {
        label: add_experiment_name_to_output_file(
            resolve_project_path(paths[key]), experiment_name
        )
        for label, key in path_keys.items()
    }

    return experiment_name, files


def sum_count(path: Path) -> tuple[int, int]:
    """Return (sum of `count`, number of data rows) for a merged Q3 CSV."""
    total = 0
    rows = 0

    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)

        if not reader.fieldnames or COUNT_COLUMN not in reader.fieldnames:
            raise ValueError(
                f"'{COUNT_COLUMN}' column not found in {path} "
                f"(header: {reader.fieldnames})"
            )

        for record in reader:
            value = (record.get(COUNT_COLUMN) or "").strip()
            if value:
                total += int(value)
                rows += 1

    return total, rows


OUTPUT_HEADER = [
    "timestamp_utc",
    "experiment",
    "baseline",
    "window",
    "count_baseline",
    "count_kept",
    "late_dropped",
    "late_dropped_pct",
]


def rotate_if_stale_schema(output_file: Path) -> None:
    """Move aside an output file written with a previous schema.

    Appending under a mismatched header would silently misalign columns
    (e.g. late_drops_q3.csv left over from the discarded intra-run method).
    """
    if not output_file.exists():
        return

    with output_file.open("r", encoding="utf-8", newline="") as handle:
        first_line = handle.readline().strip()

    if first_line != ",".join(OUTPUT_HEADER):
        stale = output_file.with_suffix(output_file.suffix + ".old")
        output_file.replace(stale)
        print(f"NOTE: existing {output_file.name} had a stale schema — moved to {stale.name}")


def append_output_row(
    output_file: Path,
    experiment: str,
    baseline: str,
    window: str,
    count_baseline: int,
    count_kept: int,
    late_dropped: int,
) -> None:
    output_file.parent.mkdir(parents=True, exist_ok=True)
    write_header = not output_file.exists()

    pct = (100.0 * late_dropped / count_baseline) if count_baseline else 0.0

    with output_file.open("a", encoding="utf-8", newline="") as out:
        writer = csv.writer(out)

        if write_header:
            writer.writerow(OUTPUT_HEADER)

        writer.writerow(
            [
                datetime.now(timezone.utc).isoformat(timespec="seconds"),
                experiment,
                baseline,
                window,
                count_baseline,
                count_kept,
                late_dropped,
                f"{pct:.4f}",
            ]
        )


def main() -> None:
    args = parse_args()

    # Baseline first: resolve_window_files reconfigures CONFIG_PATH, so the
    # experiment resolution must come last for any downstream config use.
    baseline_name, baseline_files = resolve_window_files(args.baseline)
    experiment_name, experiment_files = resolve_window_files(args.experiment)

    print(f"Q3 completeness | experiment: {experiment_name}  vs  baseline: {baseline_name}")

    rotate_if_stale_schema(args.output)

    if experiment_name == baseline_name:
        print(
            "  NOTE: experiment IS the baseline — expecting 0 dropped everywhere "
            "(sanity check)."
        )

    exit_code = 0

    windows = [window for window in WINDOW_PATH_KEYS if window in baseline_files and window in experiment_files]
    if not windows:
        print(
            "  No common Q3 window outputs between experiment and baseline configs.",
            file=sys.stderr,
        )
        sys.exit(1)

    for window in windows:
        base_path = baseline_files[window]
        exp_path = experiment_files[window]

        missing = [p for p in (base_path, exp_path) if not p.exists()]
        if missing:
            for path in missing:
                print(f"  [{window}] merged file not found: {path}", file=sys.stderr)
            print(
                f"  [{window}] skipped — run the merge for both '{experiment_name}' "
                f"and '{baseline_name}' first.",
                file=sys.stderr,
            )
            exit_code = 1
            continue

        count_baseline, base_rows = sum_count(base_path)
        count_kept, exp_rows = sum_count(exp_path)

        if count_baseline <= 0:
            print(
                f"  [{window}] baseline count is {count_baseline} in {base_path} — "
                "nothing to compare against, skipping.",
                file=sys.stderr,
            )
            exit_code = 1
            continue

        late_dropped = count_baseline - count_kept
        pct = 100.0 * late_dropped / count_baseline

        note = ""
        if late_dropped < 0:
            # More records than the baseline: duplicated rows in the merge or
            # mismatched runs (different dataset/producer settings).
            note = "  <-- NEGATIVE: experiment exceeds baseline, investigate"
            exit_code = 1
        elif window == "global" and late_dropped > 0:
            # The global window closes only at EOS: nothing should be late.
            note = "  <-- unexpected: global should drop ~0"

        print(
            f"  [{window}] baseline={count_baseline} ({base_rows} rows)  "
            f"kept={count_kept} ({exp_rows} rows)  "
            f"late_dropped={late_dropped} ({pct:.4f}%){note}"
        )

        append_output_row(
            output_file=args.output,
            experiment=experiment_name,
            baseline=baseline_name,
            window=window,
            count_baseline=count_baseline,
            count_kept=count_kept,
            late_dropped=late_dropped,
        )

    print(f"Appended to {args.output}")
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
