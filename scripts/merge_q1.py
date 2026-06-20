#!/usr/bin/env python3
"""Merge all Q1 part files into a single sorted CSV."""
import glob
import os
import sys

RESULTS_DIR = os.path.join(os.path.dirname(__file__), "..", "Results", "q1")
OUTPUT_FILE = os.path.join(os.path.dirname(__file__), "..", "Results", "q1.csv")

HEADER = (
    "window_start,window_end,airline,num_flights,completed,"
    "cancelled,diverted,dep_delay_mean,cancellation_rate,late_departure_rate"
)


def main() -> None:
    part_files = sorted(glob.glob(os.path.join(RESULTS_DIR, "part-*")))
    part_files = [f for f in part_files if ".inprogress" not in f]

    if not part_files:
        print(f"No finalized part files found in {RESULTS_DIR}")
        sys.exit(1)

    print(f"Found {len(part_files)} part file(s) — merging...")

    rows = []
    for path in part_files:
        with open(path, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    rows.append(line)

    # Sort by window_start, then airline
    rows = list(dict.fromkeys(rows))  # remove duplicates, preserve order
    rows.sort(key=lambda r: (r.split(",")[0], r.split(",")[2]))

    with open(OUTPUT_FILE, "w", encoding="utf-8") as out:
        out.write(HEADER + "\n")
        for row in rows:
            out.write(row + "\n")

    print(f"Written {len(rows)} rows → {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
