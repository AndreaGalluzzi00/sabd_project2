#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import sys
from collections import Counter, defaultdict
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Iterable


EXPECTED_HEADER = [
    "window_start",
    "window_end",
    "airline",
    "num_flights",
    "completed",
    "cancelled",
    "diverted",
    "dep_delay_mean",
    "cancellation_rate",
    "late_departure_rate",
]
KEY_COLUMNS = (0, 1, 2)
INTEGER_COLUMNS = {3, 4, 5, 6}
DECIMAL_COLUMNS = {7, 8, 9}


@dataclass(frozen=True)
class DatasetAudit:
    part_files: int
    raw_rows: int
    unique_rows: int
    unique_keys: int
    duplicate_rows: int
    duplicate_keys: int
    conflicting_keys: int
    malformed_rows: int
    canonical_sha256: str
    rows_by_key: dict[tuple[str, str, str], tuple[str, ...]]

    def summary(self) -> dict[str, object]:
        return {
            "part_files": self.part_files,
            "raw_rows": self.raw_rows,
            "unique_rows": self.unique_rows,
            "unique_keys": self.unique_keys,
            "duplicate_rows": self.duplicate_rows,
            "duplicate_keys": self.duplicate_keys,
            "conflicting_keys": self.conflicting_keys,
            "malformed_rows": self.malformed_rows,
            "canonical_sha256": self.canonical_sha256,
        }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Compare raw finalized Q1 part files from a no-fault run and a "
            "checkpoint-recovery run. Duplicate keys are failures even when "
            "the duplicated rows are identical."
        )
    )
    parser.add_argument("--baseline-dir", type=Path, required=True)
    parser.add_argument("--fault-dir", type=Path, required=True)
    parser.add_argument("--report-json", type=Path, required=True)
    parser.add_argument("--report-csv", type=Path, required=True)
    parser.add_argument("--baseline-merged-output", type=Path)
    parser.add_argument("--fault-merged-output", type=Path)
    return parser.parse_args()


def finalized_part_files(directory: Path) -> list[Path]:
    return sorted(
        path
        for path in directory.glob("part-*")
        if path.is_file() and ".inprogress" not in path.name
    )


def normalized_decimal(value: str) -> str:
    value = value.strip()
    if not value:
        return ""
    try:
        number = Decimal(value)
    except InvalidOperation as exc:
        raise ValueError(f"invalid decimal value {value!r}") from exc
    if not number.is_finite():
        return str(number)
    normalized = number.normalize()
    if normalized == 0:
        return "0"
    return format(normalized, "f")


def normalize_row(row: list[str]) -> tuple[str, ...]:
    if len(row) != len(EXPECTED_HEADER):
        raise ValueError(
            f"expected {len(EXPECTED_HEADER)} columns, found {len(row)}"
        )

    normalized: list[str] = []
    for index, value in enumerate(row):
        value = value.strip()
        if index == 0:
            value = value.lstrip("\ufeff")
        if index in INTEGER_COLUMNS:
            try:
                value = str(int(value))
            except ValueError as exc:
                raise ValueError(f"invalid integer value {value!r}") from exc
        elif index in DECIMAL_COLUMNS:
            value = normalized_decimal(value)
        normalized.append(value)
    return tuple(normalized)


def canonical_digest(rows: Iterable[tuple[str, ...]]) -> str:
    digest = hashlib.sha256()
    for row in sorted(rows, key=lambda item: (item[0], item[1], item[2])):
        digest.update(",".join(row).encode("utf-8"))
        digest.update(b"\n")
    return digest.hexdigest()


def audit_dataset(directory: Path) -> DatasetAudit:
    part_files = finalized_part_files(directory)
    if not part_files:
        raise FileNotFoundError(f"no finalized part files found in {directory}")

    rows: list[tuple[str, ...]] = []
    malformed_rows = 0
    for part_file in part_files:
        with part_file.open("r", encoding="utf-8", newline="") as handle:
            for line_number, row in enumerate(csv.reader(handle), start=1):
                if not row or not any(cell.strip() for cell in row):
                    continue
                try:
                    rows.append(normalize_row(row))
                except ValueError as exc:
                    malformed_rows += 1
                    print(
                        f"Malformed row {part_file}:{line_number}: {exc}",
                        file=sys.stderr,
                    )

    row_counts = Counter(rows)
    rows_for_key: dict[tuple[str, str, str], list[tuple[str, ...]]] = defaultdict(list)
    for row in rows:
        key = tuple(row[index] for index in KEY_COLUMNS)
        rows_for_key[key].append(row)

    duplicate_keys = sum(len(values) - 1 for values in rows_for_key.values())
    conflicting_keys = sum(
        1 for values in rows_for_key.values() if len(set(values)) > 1
    )
    canonical_rows = {
        key: sorted(set(values))[0]
        for key, values in rows_for_key.items()
    }

    return DatasetAudit(
        part_files=len(part_files),
        raw_rows=len(rows),
        unique_rows=len(row_counts),
        unique_keys=len(rows_for_key),
        duplicate_rows=sum(count - 1 for count in row_counts.values()),
        duplicate_keys=duplicate_keys,
        conflicting_keys=conflicting_keys,
        malformed_rows=malformed_rows,
        canonical_sha256=canonical_digest(canonical_rows.values()),
        rows_by_key=canonical_rows,
    )


def write_csv_report(path: Path, report: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    baseline = report["baseline"]
    fault = report["fault"]
    row = {
        "verification_pass": report["verification_pass"],
        "baseline_raw_rows": baseline["raw_rows"],
        "fault_raw_rows": fault["raw_rows"],
        "baseline_duplicate_keys": baseline["duplicate_keys"],
        "fault_duplicate_keys": fault["duplicate_keys"],
        "baseline_conflicting_keys": baseline["conflicting_keys"],
        "fault_conflicting_keys": fault["conflicting_keys"],
        "missing_keys_in_fault": report["missing_keys_in_fault"],
        "extra_keys_in_fault": report["extra_keys_in_fault"],
        "value_mismatches": report["value_mismatches"],
        "baseline_sha256": baseline["canonical_sha256"],
        "fault_sha256": fault["canonical_sha256"],
    }
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(row))
        writer.writeheader()
        writer.writerow(row)


def write_merged_output(path: Path, audit: DatasetAudit) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(EXPECTED_HEADER)
        for key in sorted(audit.rows_by_key):
            writer.writerow(audit.rows_by_key[key])


def main() -> int:
    args = parse_args()
    baseline = audit_dataset(args.baseline_dir)
    fault = audit_dataset(args.fault_dir)

    baseline_keys = set(baseline.rows_by_key)
    fault_keys = set(fault.rows_by_key)
    missing_keys = sorted(baseline_keys - fault_keys)
    extra_keys = sorted(fault_keys - baseline_keys)
    common_keys = sorted(baseline_keys & fault_keys)
    mismatched_keys = [
        key
        for key in common_keys
        if baseline.rows_by_key[key] != fault.rows_by_key[key]
    ]

    verification_pass = all(
        (
            baseline.malformed_rows == 0,
            fault.malformed_rows == 0,
            baseline.duplicate_keys == 0,
            fault.duplicate_keys == 0,
            baseline.conflicting_keys == 0,
            fault.conflicting_keys == 0,
            not missing_keys,
            not extra_keys,
            not mismatched_keys,
            baseline.canonical_sha256 == fault.canonical_sha256,
        )
    )

    report: dict[str, object] = {
        "verification_pass": verification_pass,
        "baseline_directory": str(args.baseline_dir.resolve()),
        "fault_directory": str(args.fault_dir.resolve()),
        "baseline": baseline.summary(),
        "fault": fault.summary(),
        "missing_keys_in_fault": len(missing_keys),
        "extra_keys_in_fault": len(extra_keys),
        "value_mismatches": len(mismatched_keys),
        "missing_key_examples": [list(key) for key in missing_keys[:10]],
        "extra_key_examples": [list(key) for key in extra_keys[:10]],
        "mismatch_examples": [
            {
                "key": list(key),
                "baseline": list(baseline.rows_by_key[key]),
                "fault": list(fault.rows_by_key[key]),
            }
            for key in mismatched_keys[:10]
        ],
    }

    args.report_json.parent.mkdir(parents=True, exist_ok=True)
    args.report_json.write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    write_csv_report(args.report_csv, report)
    if args.baseline_merged_output:
        write_merged_output(args.baseline_merged_output, baseline)
    if args.fault_merged_output:
        write_merged_output(args.fault_merged_output, fault)

    print("Fault-tolerance output comparison")
    print(
        "  baseline: "
        f"rows={baseline.raw_rows}, keys={baseline.unique_keys}, "
        f"duplicate_keys={baseline.duplicate_keys}, "
        f"sha256={baseline.canonical_sha256}"
    )
    print(
        "  fault:    "
        f"rows={fault.raw_rows}, keys={fault.unique_keys}, "
        f"duplicate_keys={fault.duplicate_keys}, "
        f"sha256={fault.canonical_sha256}"
    )
    print(
        "  compare:  "
        f"missing={len(missing_keys)}, extra={len(extra_keys)}, "
        f"value_mismatches={len(mismatched_keys)}"
    )
    print(f"  result:   {'PASS' if verification_pass else 'FAIL'}")
    print(f"  report:   {args.report_json}")

    return 0 if verification_pass else 1


if __name__ == "__main__":
    raise SystemExit(main())
