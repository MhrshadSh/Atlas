#!/usr/bin/env python3
import argparse
import csv
import sys
from ast import literal_eval
from pathlib import Path


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

    total_selected = 0
    total_best = 0
    num_rows_used = 0
    num_rows_total = 0

    with csv_path.open("r", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            num_rows_total += 1

            # Parse selected CI
            try:
                selected_ci = float(row.get("selected_ip_ci", "-1").strip() or -1)
            except Exception:
                selected_ci = -1

            # Parse list of CI values
            ci_list = to_list_of_ints(row.get("ci_list"))
            best_ci = min(ci_list) if ci_list else None

            # Skip rows without any usable data
            if best_ci is None and (selected_ci is None or selected_ci < 0):
                continue

            # Always skip rows where selected CI is negative/unknown
            if selected_ci is None or selected_ci < 0:
                continue

            # Aggregate
            if selected_ci is not None and selected_ci >= 0:
                total_selected += selected_ci
            elif best_ci is not None:
                # If selected is missing/negative but best exists, treat selected as 0 contribution
                # to keep denominators consistent only when explicitly desired; otherwise
                # just add best to best total below.
                pass

            if best_ci is not None:
                total_best += best_ci

            num_rows_used += 1

    if num_rows_used == 0:
        print("No usable rows found.")
        return 0

    absolute_savings = total_selected - total_best
    percent_savings = (
        (absolute_savings / total_selected * 100) if total_selected > 0 else 0
    )

    print("Carbon intensity aggregation (selected vs best-case)")
    print(f"Rows considered: {num_rows_used} (of {num_rows_total})")
    print(f"Total selected CI: {total_selected:.2f}")
    print(f"Total best-case CI: {total_best:.2f}")
    print(f"Absolute savings: {absolute_savings:.2f}")
    print(f"Percent savings: {percent_savings:.2f}%")

    # Averages per used row
    print(f"Average selected CI per row: {total_selected/num_rows_used:.2f}")
    print(f"Average best-case CI per row: {total_best/num_rows_used:.2f}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())


