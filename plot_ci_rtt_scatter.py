#!/usr/bin/env python3
import argparse
import sys
from ast import literal_eval
from pathlib import Path
from typing import List, Optional, Tuple


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Create an aggregated scatter plot of CI vs RTT from an RTT-enriched CSV. "
            "Each point corresponds to one IP candidate in a row: (ci_list[i], rtt_list[i])."
        )
    )
    parser.add_argument(
        "--csv",
        default=str(Path("output") / "rtt_enriched_correlated_ping_dns.csv"),
        help="Path to RTT-enriched CSV (with ci_list and rtt_list columns)",
    )
    parser.add_argument(
        "--out",
        default=str(Path("output") / "ci_vs_rtt_scatter.png"),
        help="Output image file (default: output/ci_vs_rtt_scatter.png)",
    )
    parser.add_argument(
        "--alpha",
        type=float,
        default=0.25,
        help="Point transparency (0..1). Default: 0.25",
    )
    parser.add_argument(
        "--dpi",
        type=int,
        default=180,
        help="Figure DPI. Default: 180",
    )
    parser.add_argument(
        "--max-points",
        type=int,
        default=2_000_000,
        help="Optional cap on number of plotted points for very large datasets.",
    )
    parser.add_argument(
        "--xmax",
        type=float,
        default=None,
        help="Optional max X (CI) to clip/filter outliers.",
    )
    parser.add_argument(
        "--ymax",
        type=float,
        default=None,
        help="Optional max Y (RTT ms) to clip/filter outliers.",
    )
    return parser.parse_args()


def parse_list_of_numbers(s: Optional[str]) -> List[Optional[float]]:
    if s is None:
        return []
    s = str(s).strip()
    if not s:
        return []
    try:
        parsed = literal_eval(s)
        if isinstance(parsed, (list, tuple)):
            out: List[Optional[float]] = []
            for x in parsed:
                if x is None:
                    out.append(None)
                else:
                    try:
                        out.append(float(x))
                    except Exception:
                        out.append(None)
            return out
    except Exception:
        return []
    return []


def main() -> int:
    args = parse_args()
    csv_path = Path(args.csv)
    if not csv_path.exists():
        print(f"Input CSV not found: {csv_path}", file=sys.stderr)
        return 1

    try:
        import pandas as pd  # type: ignore
        import matplotlib.pyplot as plt  # type: ignore
    except Exception as e:
        print(f"Required packages missing (pandas/matplotlib). Error: {e}", file=sys.stderr)
        return 1

    df = pd.read_csv(csv_path)
    for col in ("ci_list", "rtt_list"):
        if col not in df.columns:
            print(f"Input missing column: {col}", file=sys.stderr)
            return 1

    ci_lists = df["ci_list"].apply(parse_list_of_numbers)
    rtt_lists = df["rtt_list"].apply(parse_list_of_numbers)

    xs: List[float] = []
    ys: List[float] = []

    # Aggregate aligned pairs per row
    for cis, rtts in zip(ci_lists, rtt_lists):
        if not isinstance(cis, list) or not isinstance(rtts, list):
            continue
        n = min(len(cis), len(rtts))
        for i in range(n):
            ci = cis[i]
            rtt = rtts[i]
            if ci is None or rtt is None:
                continue
            # filter negative/invalid
            if ci < 0 or rtt < 0:
                continue
            xs.append(float(ci))
            ys.append(float(rtt))

    # Optional filters for outliers
    if args.xmax is not None or args.ymax is not None:
        fxs: List[float] = []
        fys: List[float] = []
        for cx, cy in zip(xs, ys):
            if args.xmax is not None and cx > args.xmax:
                continue
            if args.ymax is not None and cy > args.ymax:
                continue
            fxs.append(cx)
            fys.append(cy)
        xs, ys = fxs, fys

    # Cap total points to protect from rendering overload
    if len(xs) > args.max_points:
        xs = xs[: args.max_points]
        ys = ys[: args.max_points]

    if not xs:
        print("No points to plot after parsing/filters.")
        return 0

    plt.figure(figsize=(8, 5), dpi=args.dpi)
    plt.scatter(xs, ys, s=5, alpha=args.alpha, edgecolors="none")
    plt.xlabel("Carbon intensity (gCO2eq/kWh)")
    plt.ylabel("RTT (ms)")
    plt.title("Aggregated CI vs RTT (each point: one IP candidate)")
    plt.grid(True, alpha=0.2)

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    plt.tight_layout()
    plt.savefig(out_path)
    print(f"Saved scatter plot to: {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


