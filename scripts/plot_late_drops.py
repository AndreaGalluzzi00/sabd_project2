#!/usr/bin/env python3

from __future__ import annotations

import argparse
import csv
import re
from collections import defaultdict
from dataclasses import dataclass
from html import escape
from pathlib import Path

from merge_utils import PROJECT_ROOT

METRIC = "numLateRecordsDropped"
DEFAULT_INPUT = PROJECT_ROOT / "Results" / "Esperimenti completezza" / "late_drops.csv"
DEFAULT_OUTPUT = (
    PROJECT_ROOT
    / "Results"
    / "Esperimenti completezza"
    / "late_drops_numLateRecordsDropped.png"
)

EXPERIMENT_RE = re.compile(r"^(?P<window>[^_]+)_wm_[^_]+_d(?P<delay>\d+)(?:_|$)")
QUERY_COLORS = {
    "q1": "#4e79a7",
    "q2": "#59a14f",
    "q3": "#e15759",
}
FALLBACK_COLORS = ["#4e79a7", "#f28e2b", "#59a14f", "#e15759", "#b07aa1"]


@dataclass(frozen=True)
class Point:
    query: str
    window: str
    delay_seconds: int
    delay_label: str
    value: int
    timestamp: str
    operator: str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate a PNG or SVG chart from Results/Esperimenti completezza/late_drops.csv."
    )
    parser.add_argument(
        "--input",
        "-i",
        type=Path,
        default=DEFAULT_INPUT,
        help=f"Input CSV path (default: {DEFAULT_INPUT}).",
    )
    parser.add_argument(
        "--output",
        "-o",
        type=Path,
        default=DEFAULT_OUTPUT,
        help=f"Output PNG/SVG path (default: {DEFAULT_OUTPUT}).",
    )
    parser.add_argument(
        "--include-operators",
        action="store_true",
        help="Plot operator-level rows instead of only TOTAL rows.",
    )
    parser.add_argument(
        "--shared-scale",
        action="store_true",
        help="Use the same y-axis scale for every panel.",
    )
    return parser.parse_args()


def is_true(value: str) -> bool:
    return value.strip().lower() in {"1", "true", "yes", "y"}


def parse_int(value: str) -> int:
    if not value:
        return 0
    return int(float(value))


def infer_window(row: dict[str, str]) -> str:
    window = row.get("window", "").strip()
    if window and window != "all_operators":
        return window

    match = EXPERIMENT_RE.search(row.get("experiment", ""))
    if match:
        return match.group("window")

    return "unknown"


def infer_delay_seconds(row: dict[str, str]) -> int:
    match = EXPERIMENT_RE.search(row.get("experiment", ""))
    if not match:
        return 0
    return int(match.group("delay"))


def format_delay(seconds: int) -> str:
    if seconds == 0:
        return "0"
    if seconds % 86_400 == 0:
        days = seconds // 86_400
        return f"{days}d"
    if seconds % 3_600 == 0:
        hours = seconds // 3_600
        return f"{hours}h"
    if seconds % 60 == 0:
        minutes = seconds // 60
        return f"{minutes}m"
    return f"{seconds}s"


def read_points(csv_path: Path, *, include_operators: bool) -> list[Point]:
    with csv_path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        missing = {METRIC, "query", "experiment", "is_total"} - set(reader.fieldnames or [])
        if missing:
            columns = ", ".join(sorted(missing))
            raise ValueError(f"{csv_path} is missing required column(s): {columns}")

        latest_by_key: dict[tuple[str, str, str, int], Point] = {}
        for row in reader:
            is_total = is_true(row.get("is_total", ""))
            if include_operators:
                if is_total:
                    continue
                operator = row.get("operator", "").strip() or row.get("metric_id", "").strip()
            else:
                if not is_total:
                    continue
                operator = "TOTAL"

            query = row.get("query", "").strip() or "unknown"
            window = infer_window(row)
            delay_seconds = infer_delay_seconds(row)
            point = Point(
                query=query,
                window=window,
                delay_seconds=delay_seconds,
                delay_label=format_delay(delay_seconds),
                value=parse_int(row.get(METRIC, "0")),
                timestamp=row.get("timestamp_utc", ""),
                operator=operator,
            )

            key = (point.query, point.window, point.operator, point.delay_seconds)
            previous = latest_by_key.get(key)
            if previous is None or point.timestamp >= previous.timestamp:
                latest_by_key[key] = point

    points = sorted(
        latest_by_key.values(),
        key=lambda p: (p.query, window_sort_key(p.window), p.operator, p.delay_seconds),
    )
    if not points:
        row_type = "operator-level" if include_operators else "TOTAL"
        raise ValueError(f"No {row_type} rows found in {csv_path}")
    return points


def window_sort_key(window: str) -> tuple[int, str]:
    match = re.fullmatch(r"(\d+)([hd])", window)
    if not match:
        return (10**12, window)
    value = int(match.group(1))
    unit = match.group(2)
    seconds = value * (3_600 if unit == "h" else 86_400)
    return (seconds, window)


def group_points(points: list[Point]) -> list[tuple[tuple[str, str, str], list[Point]]]:
    groups: dict[tuple[str, str, str], list[Point]] = defaultdict(list)
    for point in points:
        groups[(point.query, point.window, point.operator)].append(point)

    return [
        (key, sorted(values, key=lambda p: p.delay_seconds))
        for key, values in sorted(
            groups.items(),
            key=lambda item: (item[0][0], window_sort_key(item[0][1]), item[0][2]),
        )
    ]


def nice_axis_max(value: int) -> int:
    if value <= 0:
        return 1

    magnitude = 10 ** (len(str(value)) - 1)
    for multiplier in (1, 2, 5, 10):
        candidate = multiplier * magnitude
        if candidate >= value:
            return candidate
    return 10 * magnitude


def fmt_number(value: float) -> str:
    return f"{value:,.0f}"


def svg_text(
    x: float,
    y: float,
    text: str,
    *,
    size: int = 14,
    weight: int = 400,
    anchor: str = "start",
    fill: str = "#2f3437",
) -> str:
    return (
        f'<text x="{x:.1f}" y="{y:.1f}" font-size="{size}" '
        f'font-weight="{weight}" text-anchor="{anchor}" fill="{fill}">'
        f"{escape(text)}</text>"
    )


def render_svg(
    points: list[Point],
    *,
    shared_scale: bool,
    source_name: str,
    row_kind: str,
) -> str:
    groups = group_points(points)
    width = 1120
    left = 105
    right = 55
    top = 100
    panel_height = 190
    plot_height = 112
    plot_width = width - left - right
    height = top + panel_height * len(groups) + 50

    global_max = nice_axis_max(max(point.value for point in points))
    rows = [
        '<?xml version="1.0" encoding="UTF-8"?>',
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">',
        '<rect width="100%" height="100%" fill="#ffffff"/>',
        '<style>'
        "text { font-family: Segoe UI, Arial, sans-serif; } "
        ".axis { stroke: #2f3437; stroke-width: 1; } "
        ".grid { stroke: #d8dee4; stroke-width: 1; }"
        "</style>",
        svg_text(42, 40, "numLateRecordsDropped per ritardo watermark", size=26, weight=700),
        svg_text(
            42,
            66,
            f"Righe {row_kind} da {source_name}; duplicati per esperimento risolti usando il timestamp piu recente.",
            size=13,
            fill="#57606a",
        ),
    ]

    for index, ((query, window, operator), values) in enumerate(groups):
        panel_top = top + panel_height * index
        axis_top = panel_top + 36
        axis_bottom = axis_top + plot_height
        panel_max = global_max if shared_scale else nice_axis_max(max(point.value for point in values))
        color = QUERY_COLORS.get(query, FALLBACK_COLORS[index % len(FALLBACK_COLORS)])

        title = f"{query.upper()} - finestra {window}"
        if operator != "TOTAL":
            title = f"{title} - {operator}"

        rows.extend(
            [
                svg_text(42, panel_top + 18, title, size=17, weight=700),
                svg_text(left, axis_top - 10, METRIC, size=12, fill="#57606a"),
                f'<line class="axis" x1="{left}" y1="{axis_bottom}" x2="{left + plot_width}" y2="{axis_bottom}"/>',
                f'<line class="axis" x1="{left}" y1="{axis_top}" x2="{left}" y2="{axis_bottom}"/>',
            ]
        )

        for step in range(5):
            fraction = step / 4
            y = axis_bottom - fraction * plot_height
            tick_value = panel_max * fraction
            rows.append(
                f'<line class="grid" x1="{left}" y1="{y:.1f}" x2="{left + plot_width}" y2="{y:.1f}"/>'
            )
            rows.append(
                svg_text(left - 12, y + 4, fmt_number(tick_value), size=11, anchor="end", fill="#57606a")
            )

        slot = plot_width / max(len(values), 1)
        bar_width = min(92, max(28, slot * 0.42))
        for pos, point in enumerate(values):
            center = left + slot * pos + slot / 2
            bar_height = 0 if point.value <= 0 else (point.value / panel_max) * plot_height
            bar_height = max(bar_height, 2 if point.value > 0 else 0)
            x = center - bar_width / 2
            y = axis_bottom - bar_height

            if point.value > 0:
                rows.append(
                    f'<rect x="{x:.1f}" y="{y:.1f}" width="{bar_width:.1f}" height="{bar_height:.1f}" '
                    f'rx="3" fill="{color}"/>'
                )
            else:
                rows.append(
                    f'<line x1="{x:.1f}" y1="{axis_bottom:.1f}" x2="{x + bar_width:.1f}" '
                    f'y2="{axis_bottom:.1f}" stroke="{color}" stroke-width="3"/>'
                )

            label_y = max(axis_top + 12, y - 7)
            rows.append(
                svg_text(center, label_y, fmt_number(point.value), size=12, weight=700, anchor="middle")
            )
            rows.append(
                svg_text(center, axis_bottom + 22, point.delay_label, size=12, anchor="middle", fill="#57606a")
            )

        rows.append(svg_text(left + plot_width / 2, axis_bottom + 45, "Ritardo watermark", size=12, anchor="middle", fill="#57606a"))

    rows.append("</svg>")
    return "\n".join(rows)


def render_png(
    points: list[Point],
    output_path: Path,
    *,
    shared_scale: bool,
    source_name: str,
    row_kind: str,
) -> None:
    try:
        from PIL import Image, ImageDraw, ImageFont
    except ImportError as exc:
        raise SystemExit(
            "PNG output requires Pillow. Install it or use an output path ending in .svg."
        ) from exc

    def load_font(size: int, *, bold: bool = False):
        font_names = (
            ["segoeuib.ttf", "arialbd.ttf", "DejaVuSans-Bold.ttf"]
            if bold
            else ["segoeui.ttf", "arial.ttf", "DejaVuSans.ttf"]
        )
        font_paths = [
            Path("C:/Windows/Fonts") / name
            for name in font_names
        ] + [Path(name) for name in font_names]

        for font_path in font_paths:
            try:
                return ImageFont.truetype(str(font_path), size=size)
            except OSError:
                continue
        return ImageFont.load_default()

    def draw_text(
        x: float,
        y: float,
        text: str,
        *,
        size: int = 14,
        bold: bool = False,
        anchor: str = "start",
        fill: str = "#2f3437",
    ) -> None:
        font = load_font(size, bold=bold)
        anchor_map = {"start": "ls", "middle": "ms", "end": "rs"}
        try:
            draw.text((x, y), text, font=font, fill=fill, anchor=anchor_map[anchor])
        except (TypeError, ValueError):
            bbox = draw.textbbox((0, 0), text, font=font)
            width = bbox[2] - bbox[0]
            height = bbox[3] - bbox[1]
            tx = x
            if anchor == "middle":
                tx -= width / 2
            elif anchor == "end":
                tx -= width
            draw.text((tx, y - height), text, font=font, fill=fill)

    groups = group_points(points)
    width = 1120
    left = 105
    right = 55
    top = 100
    panel_height = 190
    plot_height = 112
    plot_width = width - left - right
    height = top + panel_height * len(groups) + 50

    image = Image.new("RGB", (width, height), "#ffffff")
    draw = ImageDraw.Draw(image)

    draw_text(42, 40, "numLateRecordsDropped per ritardo watermark", size=26, bold=True)
    draw_text(
        42,
        66,
        f"Righe {row_kind} da {source_name}; duplicati per esperimento risolti usando il timestamp piu recente.",
        size=13,
        fill="#57606a",
    )

    global_max = nice_axis_max(max(point.value for point in points))
    for index, ((query, window, operator), values) in enumerate(groups):
        panel_top = top + panel_height * index
        axis_top = panel_top + 36
        axis_bottom = axis_top + plot_height
        panel_max = global_max if shared_scale else nice_axis_max(max(point.value for point in values))
        color = QUERY_COLORS.get(query, FALLBACK_COLORS[index % len(FALLBACK_COLORS)])

        title = f"{query.upper()} - finestra {window}"
        if operator != "TOTAL":
            title = f"{title} - {operator}"

        draw_text(42, panel_top + 18, title, size=17, bold=True)
        draw_text(left, axis_top - 10, METRIC, size=12, fill="#57606a")
        draw.line((left, axis_bottom, left + plot_width, axis_bottom), fill="#2f3437", width=1)
        draw.line((left, axis_top, left, axis_bottom), fill="#2f3437", width=1)

        for step in range(5):
            fraction = step / 4
            y = axis_bottom - fraction * plot_height
            tick_value = panel_max * fraction
            draw.line((left, y, left + plot_width, y), fill="#d8dee4", width=1)
            draw_text(left - 12, y + 4, fmt_number(tick_value), size=11, anchor="end", fill="#57606a")

        slot = plot_width / max(len(values), 1)
        bar_width = min(92, max(28, slot * 0.42))
        for pos, point in enumerate(values):
            center = left + slot * pos + slot / 2
            bar_height = 0 if point.value <= 0 else (point.value / panel_max) * plot_height
            bar_height = max(bar_height, 2 if point.value > 0 else 0)
            x = center - bar_width / 2
            y = axis_bottom - bar_height

            if point.value > 0:
                draw.rounded_rectangle(
                    (x, y, x + bar_width, axis_bottom),
                    radius=3,
                    fill=color,
                )
            else:
                draw.line((x, axis_bottom, x + bar_width, axis_bottom), fill=color, width=3)

            label_y = max(axis_top + 12, y - 7)
            draw_text(center, label_y, fmt_number(point.value), size=12, bold=True, anchor="middle")
            draw_text(center, axis_bottom + 22, point.delay_label, size=12, anchor="middle", fill="#57606a")

        draw_text(
            left + plot_width / 2,
            axis_bottom + 45,
            "Ritardo watermark",
            size=12,
            anchor="middle",
            fill="#57606a",
        )

    image.save(output_path)


def main() -> None:
    args = parse_args()
    input_path = args.input
    output_path = args.output

    points = read_points(input_path, include_operators=args.include_operators)
    row_kind = "operator" if args.include_operators else "TOTAL"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    suffix = output_path.suffix.lower()

    if suffix == ".svg":
        svg = render_svg(
            points,
            shared_scale=args.shared_scale,
            source_name=input_path.name,
            row_kind=row_kind,
        )
        output_path.write_text(svg, encoding="utf-8", newline="\n")
    elif suffix == ".png":
        render_png(
            points,
            output_path,
            shared_scale=args.shared_scale,
            source_name=input_path.name,
            row_kind=row_kind,
        )
    else:
        raise SystemExit("Output path must end with .png or .svg")

    print(f"Chart written to {output_path}")


if __name__ == "__main__":
    main()
