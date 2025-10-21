import pandas as pd
import ast
import sys
from collections import Counter

def probe_statistics(csv_file, probe_id):
    # Load CSV
    df = pd.read_csv(csv_file)

    # Filter for given probe
    probe_df = df[df["probe_id"] == probe_id]

    if probe_df.empty:
        print(f"No data found for probe_id {probe_id}")
        return

    # --- Unique selected IPs ---
    unique_selected_ips = probe_df["selected_ip"].dropna().unique()
    num_unique_selected_ips = len(unique_selected_ips)

    # --- Unique resolved IPs ---
    all_resolved_ips = []
    for resolved_str in probe_df["resolved_set"].dropna():
        try:
            resolved_list = ast.literal_eval(resolved_str)
            all_resolved_ips.extend(resolved_list)
        except Exception:
            pass
    unique_resolved_ips = set(all_resolved_ips)
    num_unique_resolved_ips = len(unique_resolved_ips)

    # --- Histogram of selected IPs ---
    hist = Counter(probe_df["selected_ip"].dropna())

    # # --- Print results ---
    print(f"\n Statistics for probe_id {probe_id}")
    print("-" * 40)
    print(f"Total measurements: {len(probe_df)}")
    print(f"Unique selected IPs: {num_unique_selected_ips}")
    print(f"Unique IPs in resolved sets: {num_unique_resolved_ips}\n")

    print("Histogram of selected IPs:")
    for ip, count in hist.most_common():
        print(f"  {ip:20} {count}")
    
    # import matplotlib.pyplot as plt
    # plt.bar(hist.keys(), hist.values())
    # plt.xticks(rotation=90)
    # plt.show()


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python probe_stats.py <csv_file> <probe_id>")
        sys.exit(1)

    csv_file = sys.argv[1]
    probe_id = int(sys.argv[2])
    probe_statistics(csv_file, probe_id)
