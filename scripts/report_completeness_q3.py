#!/usr/bin/env python3
"""
Report Q3 late-record drops from the MERGED output (single run, no metric).

Flink's 'numLateRecordsDropped' is registered only by the Table/SQL
WindowOperator (Q1/Q2 'table'). Q3 computes its windows in the DataStream API
(DDSketch aggregator), which does not register that counter, so we recover the
same quantity from the output itself — in the very same experiment (same
watermark) used to collect numLateRecordsDropped for the other queries. No
safe/aggressive watermark pair is needed.

Trick: the three Q3 windows partition the *same* filtered input, so in a
loss-free run the sum of the exact `count` column is identical across them.
The 'global' window is a single bucket over the whole dataset and closes only
when the EOS marker pushes the watermark past its end, hence it never drops a
late record -> its total is the run's ground-truth N. For every other window:

    late_dropped(w) = sum(count global) - sum(count w)
    late_dropped(global) = 0            (reported as a sanity check)

Reads the same merged CSVs that scripts/merge_q3.py writes for the given
experiment (Results/q3_<window>_flink_table_<exp>.csv) and appends one row per
window to Results/late_drops_q3.csv.

Run it AFTER the merge (the merged CSVs must exist).

Exit codes: 0 = computed; 1 = the 'global' reference or the merged files are
missing (never writes a fabricated number in that case).
"""
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
}

# The window used as ground truth: it closes only at EOS, so it loses nothing.
GROUND_TRUTH_WINDOW = "global"

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
        "--output",
        type=Path,
        default=DEFAULT_OUTPUT,
        help=f"CSV file the per-window rows are appended to (default: {DEFAULT_OUTPUT}).",
    )

    return parser.parse_args()


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

    files = {
        label: add_experiment_name_to_output_file(
            resolve_project_path(paths[key]), experiment_name
        )
        for label, key in WINDOW_PATH_KEYS.items()
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


def append_output_row(
    output_file: Path,
    experiment: str,
    window: str,
    rows: int,
    count_kept: int,
    ground_truth: int,
    late_dropped: int,
) -> None:
    output_file.parent.mkdir(parents=True, exist_ok=True)
    write_header = not output_file.exists()

    pct = (100.0 * late_dropped / ground_truth) if ground_truth else 0.0

    with output_file.open("a", encoding="utf-8", newline="") as out:
        writer = csv.writer(out)

        if write_header:
            writer.writerow(
                [
                    "timestamp_utc",
                    "experiment",
                    "window",
                    "rows",
                    "count_kept",
                    "ground_truth_global",
                    "late_dropped",
                    "late_dropped_pct",
                ]
            )

        writer.writerow(
            [
                datetime.now(timezone.utc).isoformat(timespec="seconds"),
                experiment,
                window,
                rows,
                count_kept,
                ground_truth,
                late_dropped,
                f"{pct:.4f}",
            ]
        )


def main() -> None:
    args = parse_args()

    experiment_name, files = resolve_window_files(args.experiment)

    print(f"Q3 completeness | experiment: {experiment_name}")

    # ── Ground truth from the 'global' window (loses nothing) ─────────────────
    gt_path = files[GROUND_TRUTH_WINDOW]
    if not gt_path.exists():
        print(
            f"ERROR: ground-truth window '{GROUND_TRUTH_WINDOW}' file not found: "
            f"{gt_path}\nRun the merge first (the merged CSVs must exist).",
            file=sys.stderr,
        )
        sys.exit(1)

    ground_truth, gt_rows = sum_count(gt_path)
    if ground_truth <= 0:
        print(
            f"ERROR: ground-truth count is {ground_truth} in {gt_path} — no data "
            "to compare against; cannot distinguish '0 drops' from 'not measured'.",
            file=sys.stderr,
        )
        sys.exit(1)

    print(
        f"  [ground truth] window '{GROUND_TRUTH_WINDOW}': "
        f"{ground_truth} records over {gt_rows} rows  ->  N = {ground_truth}"
    )

    # ── Late drops per window = N - sum(count) ────────────────────────────────
    exit_code = 0
    for window, path in files.items():
        if not path.exists():
            print(f"  [{window}] merged file not found: {path} — skipping.",
                  file=sys.stderr)
            exit_code = 1
            continue

        count_kept, rows = sum_count(path)
        late_dropped = ground_truth - count_kept
        pct = 100.0 * late_dropped / ground_truth

        note = ""
        if late_dropped < 0:
            # sum(count) > N means 'global' is not the true maximum: duplicated
            # rows, or a record slipped past the global window. Worth a look.
            note = "  <-- NEGATIVE: global is not the max, investigate"
            exit_code = 1

        print(
            f"  [{window}] kept={count_kept}  late_dropped={late_dropped} "
            f"({pct:.2f}%)  rows={rows}{note}"
        )

        append_output_row(
            output_file=args.output,
            experiment=experiment_name,
            window=window,
            rows=rows,
            count_kept=count_kept,
            ground_truth=ground_truth,
            late_dropped=late_dropped,
        )

    print(f"Appended to {args.output}")
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
