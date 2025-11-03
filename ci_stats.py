#!/usr/bin/env python3
import argparse
import csv
import sys
from ast import literal_eval
from pathlib import Path
from typing import List, Optional


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Compute aggregated carbon intensity totals from correlated_ping_dns.csv: "
            "sum of selected_ip_ci vs best-case (min of ci_list per row)."
        )
    )
    parser.add_argument(
        "--csv",
        default=str(Path("output") / "correlated_ping_dns.csv"),
        help="Path to correlated CSV (default: output/correlated_ping_dns.csv)",
    )
    parser.add_argument(
        "--out-csv",  # kept for backward-compat but not used anymore
        default=str(Path("output") / "rtt_enriched_correlated_ping_dns.csv"),
        help="(Deprecated) No longer used here.",
    )
    parser.add_argument(
        "--rtt",
        action="store_true",
        help=(
            "Use RTT-enriched data to compare latency of selected IP vs latency at per-row min CI."
        ),
    )
    parser.add_argument(
        "--rtt-csv",
        default=str(Path("output") / "rtt_enriched_correlated_ping_dns.csv"),
        help="Path to RTT-enriched CSV (default: output/rtt_enriched_correlated_ping_dns.csv)",
    )
    return parser.parse_args()


def to_list_of_ints(value: str) -> list:
    if value is None:
        return []
    value = value.strip()
    if not value:
        return []
    try:
        parsed = literal_eval(value)
        if isinstance(parsed, (list, tuple)):
            return [int(x) for x in parsed if x is not None]
    except Exception:
        pass
    return []


def main() -> int:
    args = parse_args()
    csv_path = Path(args.csv)
    if not csv_path.exists():
        print(f"Input CSV not found: {csv_path}", file=sys.stderr)
        return 1

    # Always use pandas
    try:
        import pandas as pd  # type: ignore
    except Exception as e:
        print(
            "Pandas is required. Install pandas and retry. "
            f"Error: {e}",
            file=sys.stderr,
        )
        return 1

    df = pd.read_csv(csv_path)


    # Helpers to parse list-like columns (not retained in final output)
    def parse_list_col(series: "pd.Series"):
        return series.apply(lambda s: to_list_of_ints(s))

    def parse_ip_list(series: "pd.Series"):
        def _parse(s: Optional[str]) -> List[str]:
            if s is None:
                return []
            s = str(s).strip()
            if not s:
                return []
            try:
                parsed = literal_eval(s)
                if isinstance(parsed, (list, tuple)):
                    return [str(x) for x in parsed if x is not None]
            except Exception:
                return []
            return []

        return series.apply(_parse)

    ci_list_parsed = parse_list_col(df["ci_list"]) if "ci_list" in df else None
    resolved_set_parsed = (
        parse_ip_list(df["resolved_set"]) if "resolved_set" in df else None
    )

    # Compute CI aggregates (skip negative selected CI)
    df_ci = df.copy()
    df_ci["selected_ip_ci"] = pd.to_numeric(df_ci["selected_ip_ci"], errors="coerce")
    df_ci = df_ci[df_ci["selected_ip_ci"] >= 0]
    if ci_list_parsed is not None:
        df_ci = df_ci.assign(best_ci=ci_list_parsed.apply(lambda xs: min(xs) if xs else None))
    else:
        df_ci = df_ci.assign(best_ci=None)
    sum_selected = df_ci["selected_ip_ci"].sum()
    sum_best = df_ci["best_ci"].fillna(0).sum()
    abs_savings = sum_selected - sum_best
    pct_savings = (abs_savings / sum_selected * 100) if sum_selected > 0 else 0
    print("Carbon intensity aggregation (selected vs best-case) [pandas]")
    print(f"Rows considered: {len(df_ci)} (of {len(df)})")
    # print(f"Total selected CI: {sum_selected:.2f}")
    # print(f"Total best-case CI: {sum_best:.2f}")
    # print(f"Absolute savings: {abs_savings:.2f}")
    print(f"Percent savings: {pct_savings:.2f}%")
    print(
        f"Average selected CI per row: {sum_selected/max(len(df_ci),1):.2f}\n"
        f"Average best-case CI per row: {sum_best/max(len(df_ci),1):.2f}"
    )

    # --- Per-hour minimum CI saving: use hour-local min CI (over all ci_list for all rows in the same hour) for each row ---
    # We'll need to:
    # - Extract hour from timestamp for each row
    # - For every hour, find the lowest CI in all ci_list entries that fall in that hour
    # - For every row, use its hour-global min in aggregate sum
    
    per_hour_min_ci = {}  # hour: min overall ci value from all ci_list for that hour
    if ci_list_parsed is not None and "timestamp" in df_ci:
        import math
        # Build a DataFrame {timestamp, ci_list} for rows considered in df_ci
        df_hour = df_ci[["timestamp"]].copy()
        df_hour["ci_list"] = ci_list_parsed.loc[df_ci.index].reset_index(drop=True)

        # Hour: integer division by 3600 (assumes timestamp is UNIX epoch seconds)
        df_hour["hour"] = df_hour["timestamp"].apply(lambda ts: int(ts) // 3600 if not pd.isna(ts) else math.nan)
        # Build hour->min-ci mapping
        for hour, group in df_hour.groupby("hour"):
            # flatten ci_list entries across all rows in this hour
            flattened = [ci for subl in group["ci_list"] if isinstance(subl, list) for ci in subl]
            if flattened:
                per_hour_min_ci[hour] = min(flattened)

    # For each row, look up its per-hour min CI
    per_row_hour_min = []
    per_row_hour = []
    if ci_list_parsed is not None and "timestamp" in df_ci:
        import math
        # We use the filtered df_ci, so timestamp and ci_list_parsed are aligned
        for ts in df_ci["timestamp"]:
            hour = int(ts) // 3600 if not pd.isna(ts) else None
            per_row_hour.append(hour)
            per_row_hour_min.append(per_hour_min_ci[hour] if hour in per_hour_min_ci else None)
        # Total sum (skip None rows for safety)
        valid_per_row_hour_min = [ci for ci in per_row_hour_min if ci is not None]
        sum_per_hour_min = sum(valid_per_row_hour_min)
        abs_savings_hour = sum_selected - sum_per_hour_min
        pct_savings_hour = (abs_savings_hour / sum_selected * 100) if sum_selected > 0 else 0
        # best vs hour-min
        abs_savings_best_vs_hr = sum_best - sum_per_hour_min
        pct_savings_best_vs_hr = (abs_savings_best_vs_hr / sum_best * 100) if sum_best > 0 else 0
        print("\nHourly minimum CI saving (for each row, use min CI among all ci_list in same hour):")
        print(f"Percent savings vs selected: {pct_savings_hour:.2f}%")
        print(f"Percent savings vs best-case: {pct_savings_best_vs_hr:.2f}%")
        print(f"Average per-hour best-case CI per row: {(sum_per_hour_min/max(len(df_ci),1)):.2f}")
    else:
        print("\n[Hourly minimum CI saving]: Not enough data to compute (no timestamp or ci_list found)")

    # Optional: latency comparison using RTT-enriched data
    if args.rtt:
        rtt_csv_path = Path(args.rtt_csv)
        if not rtt_csv_path.exists():
            print(f"RTT-enriched CSV not found: {rtt_csv_path}", file=sys.stderr)
            return 1
        df_rtt = pd.read_csv(rtt_csv_path)

        # Ensure required columns
        required_cols = ["avg_rtt", "ci_list", "rtt_list"]
        missing = [c for c in required_cols if c not in df_rtt.columns]
        if missing:
            print(f"RTT CSV missing columns: {missing}", file=sys.stderr)
            return 1

        # Parse lists
        def parse_ci_list(s: str):
            try:
                xs = literal_eval(s) if isinstance(s, str) else s
                if isinstance(xs, (list, tuple)):
                    return [int(x) if x is not None else None for x in xs]
            except Exception:
                return []
            return []

        def parse_rtt_list(s: str):
            try:
                xs = literal_eval(s) if isinstance(s, str) else s
                if isinstance(xs, (list, tuple)):
                    out = []
                    for x in xs:
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

        ci_lists = df_rtt["ci_list"].apply(parse_ci_list)
        rtt_lists = df_rtt["rtt_list"].apply(parse_rtt_list)

        # Identify best CI index per row and fetch corresponding RTT
        import numpy as np

        def best_idx_of_ci(xs):
            best_val = None
            best_idx = None
            for i, v in enumerate(xs):
                if v is None:
                    continue
                if best_val is None or v < best_val:
                    best_val = v
                    best_idx = i
            return best_idx

        # Compute best indices as a plain Python list to avoid float upcasting/NaN
        best_indices = [best_idx_of_ci(xs) for xs in ci_lists]
        df_rtt["best_rtt"] = [
            (r[int(i)] if (i is not None and not pd.isna(i) and isinstance(r, list) and int(i) < len(r)) else None)
            for r, i in zip(rtt_lists, best_indices)
        ]

        df_rtt["avg_rtt"] = pd.to_numeric(df_rtt["avg_rtt"], errors="coerce")
        df_latency = df_rtt[df_rtt["best_rtt"].notna() & df_rtt["avg_rtt"].notna()].copy()
        if len(df_latency) == 0:
            print("\n[RTT] No usable rows for latency comparison.")
            return 0

        mean_selected = float(df_latency["avg_rtt"].mean())
        mean_best = float(pd.to_numeric(df_latency["best_rtt"], errors="coerce").mean())
        median_selected = float(df_latency["avg_rtt"].median())
        median_best = float(pd.to_numeric(df_latency["best_rtt"], errors="coerce").median())

        pct_mean_change = ((mean_best - mean_selected) / mean_selected * 100.0) if mean_selected > 0 else 0.0
        pct_median_change = ((median_best - median_selected) / median_selected * 100.0) if median_selected > 0 else 0.0

        print("\nRTT comparison (selected avg_rtt vs RTT at per-row min CI):")
        print(f"Rows considered for RTT: {len(df_latency)}")
        print(f"Mean selected RTT: {mean_selected:.3f} ms")
        print(f"Mean RTT at min CI: {mean_best:.3f} ms")
        print(f"Percent change in mean RTT: {pct_mean_change:.2f}%")
        print(f"Median selected RTT: {median_selected:.3f} ms")
        print(f"Median RTT at min CI: {median_best:.3f} ms")
        print(f"Percent change in median RTT: {pct_median_change:.2f}%")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())


