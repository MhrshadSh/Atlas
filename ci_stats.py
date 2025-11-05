#!/usr/bin/env python3
import argparse
import math
import sys
from ast import literal_eval
from pathlib import Path
from typing import List, Optional

try:
    import pandas as pd  # type: ignore
except ImportError:
    pd = None


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


def parse_list_of_ints(value: str) -> List[int]:
    """Parse a string representation of a list/tuple into a list of integers."""
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


def parse_list_of_floats(value: str) -> List[Optional[float]]:
    """Parse a string representation of a list/tuple into a list of floats."""
    try:
        xs = literal_eval(value) if isinstance(value, str) else value
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


def load_dataframe(csv_path: Path) -> "pd.DataFrame":
    """Load and validate CSV file."""
    if not csv_path.exists():
        raise FileNotFoundError(f"Input CSV not found: {csv_path}")
    
    if pd is None:
        raise ImportError("Pandas is required. Install pandas and retry.")
    
    return pd.read_csv(csv_path)


def compute_ci_aggregates(df: "pd.DataFrame") -> tuple:
    """
    Compute CI aggregates: selected vs best-case.
    Returns: (df_ci, ci_list_parsed, sum_selected, sum_best, abs_savings, pct_savings)
    """
    # Parse CI lists
    ci_list_parsed = None
    if "ci_list" in df.columns:
        ci_list_parsed = df["ci_list"].apply(parse_list_of_ints)
    
    # Filter and compute best CI per row
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
    
    return df_ci, ci_list_parsed, sum_selected, sum_best, abs_savings, pct_savings


def compute_hourly_min_ci(df_ci: "pd.DataFrame", ci_list_parsed: "pd.Series") -> dict:
    """
    Compute per-hour minimum CI from all ci_list entries in each hour.
    Returns: dict mapping hour -> min CI value
    """
    per_hour_min_ci = {}
    
    if ci_list_parsed is None or "timestamp" not in df_ci.columns:
        return per_hour_min_ci
    
    # Build DataFrame with timestamp and ci_list
    df_hour = df_ci[["timestamp"]].copy()
    df_hour["ci_list"] = ci_list_parsed.loc[df_ci.index].reset_index(drop=True)
    
    # Extract hour from timestamp (assumes UNIX epoch seconds)
    df_hour["hour"] = df_hour["timestamp"].apply(
        lambda ts: int(ts) // 3600 if not pd.isna(ts) else math.nan
    )
    
    # Build hour -> min CI mapping
    for hour, group in df_hour.groupby("hour"):
        flattened = [
            ci for subl in group["ci_list"] 
            if isinstance(subl, list) 
            for ci in subl
        ]
        if flattened:
            per_hour_min_ci[hour] = min(flattened)
    
    return per_hour_min_ci


def compute_hourly_savings(
    df_ci: "pd.DataFrame",
    per_hour_min_ci: dict,
    sum_selected: float,
    sum_best: float,
) -> Optional[tuple]:
    """
    Compute hourly minimum CI savings.
    Returns: (sum_per_hour_min, abs_savings_hour, pct_savings_hour, 
              abs_savings_best_vs_hr, pct_savings_best_vs_hr) or None if no data
    """
    if "timestamp" not in df_ci.columns:
        return None
    
    per_row_hour_min = []
    for ts in df_ci["timestamp"]:
        hour = int(ts) // 3600 if not pd.isna(ts) else None
        per_row_hour_min.append(per_hour_min_ci.get(hour) if hour in per_hour_min_ci else None)
    
    valid_per_row_hour_min = [ci for ci in per_row_hour_min if ci is not None]
    if not valid_per_row_hour_min:
        return None
    
    sum_per_hour_min = sum(valid_per_row_hour_min)
    abs_savings_hour = sum_selected - sum_per_hour_min
    pct_savings_hour = (abs_savings_hour / sum_selected * 100) if sum_selected > 0 else 0
    abs_savings_best_vs_hr = sum_best - sum_per_hour_min
    pct_savings_best_vs_hr = (abs_savings_best_vs_hr / sum_best * 100) if sum_best > 0 else 0
    
    return (sum_per_hour_min, abs_savings_hour, pct_savings_hour, 
            abs_savings_best_vs_hr, pct_savings_best_vs_hr)


def find_best_ci_index(ci_list: List[Optional[int]]) -> Optional[int]:
    """Find the index of the minimum CI value in a list."""
    best_val = None
    best_idx = None
    for i, v in enumerate(ci_list):
        if v is None:
            continue
        if best_val is None or v < best_val:
            best_val = v
            best_idx = i
    return best_idx


def compare_rtt_latency(rtt_csv_path: Path) -> int:
    """
    Compare RTT latency: selected avg_rtt vs RTT at per-row min CI.
    Returns: 0 on success, 1 on error
    """
    try:
        df_rtt = load_dataframe(rtt_csv_path)
    except (FileNotFoundError, ImportError) as e:
        print(f"Error loading RTT CSV: {e}", file=sys.stderr)
        return 1
    
    # Validate required columns
    required_cols = ["avg_rtt", "ci_list", "rtt_list"]
    missing = [c for c in required_cols if c not in df_rtt.columns]
    if missing:
        print(f"RTT CSV missing columns: {missing}", file=sys.stderr)
        return 1
    
    # Parse lists
    ci_lists = df_rtt["ci_list"].apply(parse_list_of_ints)
    rtt_lists = df_rtt["rtt_list"].apply(parse_list_of_floats)
    
    # Find best CI index per row and get corresponding RTT
    best_indices = [find_best_ci_index(xs) for xs in ci_lists]
    df_rtt["best_rtt"] = [
        (r[int(i)] if (i is not None and not pd.isna(i) and isinstance(r, list) 
                       and int(i) < len(r)) else None)
        for r, i in zip(rtt_lists, best_indices)
    ]
    
    # Filter to rows with valid RTT data
    df_rtt["avg_rtt"] = pd.to_numeric(df_rtt["avg_rtt"], errors="coerce")
    df_latency = df_rtt[df_rtt["best_rtt"].notna() & df_rtt["avg_rtt"].notna()].copy()
    
    if len(df_latency) == 0:
        print("\n[RTT] No usable rows for latency comparison.")
        return 0
    
    # Compute statistics
    mean_selected = float(df_latency["avg_rtt"].mean())
    mean_best = float(pd.to_numeric(df_latency["best_rtt"], errors="coerce").mean())
    median_selected = float(df_latency["avg_rtt"].median())
    median_best = float(pd.to_numeric(df_latency["best_rtt"], errors="coerce").median())
    
    pct_mean_change = ((mean_best - mean_selected) / mean_selected * 100.0) if mean_selected > 0 else 0.0
    pct_median_change = ((median_best - median_selected) / median_selected * 100.0) if median_selected > 0 else 0.0
    
    # Print results
    print("\nRTT comparison (selected avg_rtt vs RTT at per-row min CI):")
    print(f"Rows considered for RTT: {len(df_latency)}")
    print(f"Mean selected RTT: {mean_selected:.3f} ms")
    print(f"Mean RTT at min CI: {mean_best:.3f} ms")
    print(f"Percent change in mean RTT: {pct_mean_change:.2f}%")
    print(f"Median selected RTT: {median_selected:.3f} ms")
    print(f"Median RTT at min CI: {median_best:.3f} ms")
    print(f"Percent change in median RTT: {pct_median_change:.2f}%")
    
    return 0


def main() -> int:
    """Main entry point for CI statistics computation."""
    args = parse_args()
    
    # Load main CSV
    try:
        df = load_dataframe(Path(args.csv))
    except (FileNotFoundError, ImportError) as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1
    
    # Compute CI aggregates
    df_ci, ci_list_parsed, sum_selected, sum_best, abs_savings, pct_savings = compute_ci_aggregates(df)
    
    # Print CI aggregation results
    print("Carbon intensity aggregation (selected vs best-case) [pandas]")
    print(f"Rows considered: {len(df_ci)} (of {len(df)})")
    print(f"Percent savings: {pct_savings:.2f}%")
    print(
        f"Average selected CI per row: {sum_selected/max(len(df_ci),1):.2f}\n"
        f"Average best-case CI per row: {sum_best/max(len(df_ci),1):.2f}"
    )
    
    # Compute hourly minimum CI savings
    per_hour_min_ci = compute_hourly_min_ci(df_ci, ci_list_parsed)
    hourly_savings = compute_hourly_savings(df_ci, per_hour_min_ci, sum_selected, sum_best)
    
    if hourly_savings:
        sum_per_hour_min, abs_savings_hour, pct_savings_hour, abs_savings_best_vs_hr, pct_savings_best_vs_hr = hourly_savings
        print("\nHourly minimum CI saving (for each row, use min CI among all ci_list in same hour):")
        print(f"Percent savings vs selected: {pct_savings_hour:.2f}%")
        print(f"Percent savings vs best-case: {pct_savings_best_vs_hr:.2f}%")
        print(f"Average per-hour best-case CI per row: {(sum_per_hour_min/max(len(df_ci),1)):.2f}")
    else:
        print("\n[Hourly minimum CI saving]: Not enough data to compute (no timestamp or ci_list found)")
    
    # Optional: RTT latency comparison
    if args.rtt:
        result = compare_rtt_latency(Path(args.rtt_csv))
        if result != 0:
            return result
    
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


