#!/usr/bin/env python3
import argparse
import sys
from ast import literal_eval
from pathlib import Path
from typing import List, Optional


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Create RTT-enriched CSV by adding rtt_list aligned to resolved_set. "
            "Per-IP RTT is computed as the mean avg_rtt over rows where that IP was selected."
        )
    )
    parser.add_argument(
        "--csv",
        default=str(Path("output") / "correlated_ping_dns.csv"),
        help="Input correlated CSV path (default: output/correlated_ping_dns.csv)",
    )
    parser.add_argument(
        "--out-csv",
        default=str(Path("output") / "rtt_enriched_correlated_ping_dns.csv"),
        help="Output CSV path (default: output/rtt_enriched_correlated_ping_dns.csv)",
    )
    return parser.parse_args()


def parse_ip_list(value: Optional[str]) -> List[str]:
    if value is None:
        return []
    s = str(value).strip()
    if not s:
        return []
    try:
        parsed = literal_eval(s)
        if isinstance(parsed, (list, tuple)):
            return [str(x) for x in parsed if x is not None]
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
    except Exception as e:
        print(f"Pandas is required. Error: {e}", file=sys.stderr)
        return 1

    df = pd.read_csv(csv_path)

    # Build per-IP RTT averages using rows where that IP is selected
    df_rtt = df.copy()
    df_rtt["avg_rtt"] = pd.to_numeric(df_rtt["avg_rtt"], errors="coerce")
    df_rtt = df_rtt[df_rtt["avg_rtt"] >= 0]
    ip_mean_rtt = (
        df_rtt.groupby("selected_ip")["avg_rtt"].mean().to_dict()
        if "selected_ip" in df_rtt
        else {}
    )

    # Parse resolved_set and build rtt_list per row
    resolved_sets = df.get("resolved_set")
    if resolved_sets is None:
        print("Input CSV lacks 'resolved_set' column", file=sys.stderr)
        return 1

    parsed_resolved = resolved_sets.apply(parse_ip_list)

    def build_rtt_list(resolved_ips: List[str], selected_ip: Optional[str], row_avg_rtt: Optional[float]):
        rtts: List[Optional[float]] = []
        for ip in resolved_ips:
            rtt_val = ip_mean_rtt.get(ip)
            rtts.append(float(rtt_val) if rtt_val is not None else None)
        if selected_ip and selected_ip in resolved_ips and pd.notna(row_avg_rtt):
            idx = resolved_ips.index(selected_ip)
            rtts[idx] = float(row_avg_rtt)
        return rtts

    df = df.assign(
        rtt_list=[
            build_rtt_list(ips, sel_ip, row_rtt)
            for ips, sel_ip, row_rtt in zip(
                parsed_resolved, df.get("selected_ip", []), df.get("avg_rtt", [])
            )
        ]
    )

    out_path = Path(args.out_csv)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out_path, index=False)
    print(f"Wrote RTT-enriched CSV with rtt_list column to: {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


