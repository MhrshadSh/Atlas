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
        "--write-rtt-list-csv",
        action="store_true",
        help=(
            "Build RTT lists per row using pandas and write a CSV with a new rtt_list column."
        ),
    )
    parser.add_argument(
        "--out-csv",
        default=str(Path("output") / "rtt_enriched_correlated_ping_dns.csv"),
        help="Output CSV path when --write-rtt-list-csv is used.",
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

    # Compute CI aggregates via pandas (skip negative selected CI)
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
    print(f"Total selected CI: {sum_selected:.2f}")
    print(f"Total best-case CI: {sum_best:.2f}")
    print(f"Absolute savings: {abs_savings:.2f}")
    print(f"Percent savings: {pct_savings:.2f}%")
    print(
        f"Average selected CI per row: {sum_selected/max(len(df_ci),1):.2f}\n"
        f"Average best-case CI per row: {sum_best/max(len(df_ci),1):.2f}"
    )

    if args.write_rtt_list_csv:
        # Build per-IP RTT averages using rows where that IP is selected
        df_rtt = df.copy()
        df_rtt["avg_rtt"] = pd.to_numeric(df_rtt["avg_rtt"], errors="coerce")
        df_rtt = df_rtt[df_rtt["avg_rtt"] >= 0]
        ip_mean_rtt = (
            df_rtt.groupby("selected_ip")["avg_rtt"].mean().to_dict()
            if "selected_ip" in df_rtt
            else {}
        )

        # For each row, map resolved_set -> rtt_list by IP mean
        def build_rtt_list(resolved_ips: List[str], selected_ip: Optional[str], row_avg_rtt: Optional[float]):
            rtts: List[Optional[float]] = []
            for ip in resolved_ips:
                rtt_val = ip_mean_rtt.get(ip)
                rtts.append(float(rtt_val) if rtt_val is not None else None)
            # Prefer row's actual avg_rtt for its selected_ip if present
            if selected_ip and selected_ip in resolved_ips and pd.notna(row_avg_rtt):
                idx = resolved_ips.index(selected_ip)
                rtts[idx] = float(row_avg_rtt)
            return rtts

        resolved_lists = resolved_set_parsed if resolved_set_parsed is not None else [[] for _ in range(len(df))]
        df = df.assign(
            rtt_list=[
                build_rtt_list(resolved_ips, sel_ip, row_rtt)
                for resolved_ips, sel_ip, row_rtt in zip(
                    resolved_lists, df.get("selected_ip", []), df.get("avg_rtt", [])
                )
            ]
        )

        # Write output CSV with new rtt_list column without intermediate parsed columns
        out_path = Path(args.out_csv)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        df_out = df.copy()
        # Ensure parsed helper columns are not present
        for col in ("ci_list_parsed", "resolved_set_parsed", "best_ci"):
            if col in df_out.columns:
                df_out = df_out.drop(columns=[col])
        df_out.to_csv(out_path, index=False)
        print(f"\nWrote RTT-enriched CSV with rtt_list column to: {out_path}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())


