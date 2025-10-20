import json
import csv
from typing import Dict, Any, List, Tuple
from datetime import datetime

# Local import
from dns import extract_probe_resolved_ips


def build_dns_index(dns_results: Dict[int, Dict[str, Any]]) -> Dict[int, List[Tuple[int, set]]]:
    """
    Build an index per probe: list of (timestamp, set(resolved_ips)) sorted by timestamp.
    """
    probe_to_measurements: Dict[int, List[Tuple[int, set]]] = {}
    for prb_id, data in dns_results.items():
        time_points: List[Tuple[int, set]] = []
        for ts, meas in data["measurements"].items():
            resolved = set(meas.get("resolved_ips", []))
            time_points.append((int(ts), resolved))
        time_points.sort(key=lambda x: x[0])
        probe_to_measurements[int(prb_id)] = time_points
    return probe_to_measurements


def _common_prefix_length(a: int, b: int) -> int:
    sa = str(int(a))
    sb = str(int(b))
    i = 0
    for ca, cb in zip(sa, sb):
        if ca != cb:
            break
        i += 1
    return i


def find_latest_resolved_set(time_points: List[Tuple[int, set]], ts: int) -> set:
    """
    From list of (timestamp, ips), select the entry whose timestamp has the
    longest decimal common prefix with ts. If ties occur, pick the one with the
    smallest absolute time difference; if still tied, pick the greatest t.
    Return its IP set, or empty if list is empty.
    """
    if not time_points:
        return set()

    best_ips = set()
    best_prefix = -1
    best_time_diff = None
    best_t = None

    for t, ips in time_points:
        prefix_len = _common_prefix_length(t, ts)
        time_diff = abs(int(t) - int(ts))

        select = False
        if prefix_len > best_prefix:
            select = True
        elif prefix_len == best_prefix:
            if best_time_diff is None or time_diff < best_time_diff:
                select = True
            elif time_diff == best_time_diff and (best_t is None or t > best_t):
                select = True

        if select:
            best_prefix = prefix_len
            best_time_diff = time_diff
            best_t = t
            best_ips = ips

    return best_ips


def correlate(dns_json_path: str, ping_json_path: str, output_csv_path: str) -> None:
    dns_results = extract_probe_resolved_ips(dns_json_path)
    dns_index = build_dns_index(dns_results)

    with open(ping_json_path, "r") as fin, open(output_csv_path, "w", newline="") as fout:
        writer = csv.writer(fout)
        writer.writerow(["probe_id", "timestamp", "readable_time", "selected_ip", "in_dns_set", "avg_rtt", "resolved_set"]) 

        for line_num, line in enumerate(fin, 1):
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except json.JSONDecodeError:
                continue

            if obj.get("type") != "ping":
                continue

            prb_id = obj.get("prb_id")
            ts = obj.get("timestamp")
            dst_addr = obj.get("dst_addr")
            avg = obj.get("avg", None)

            # Skip if required fields missing
            if prb_id is None or ts is None:
                continue

            selected_set = find_latest_resolved_set(dns_index.get(int(prb_id), []), int(ts))
            in_dns = dst_addr in selected_set if dst_addr else False
            resolved_list = sorted(list(selected_set)) if selected_set else []

            readable_time = datetime.fromtimestamp(int(ts)).strftime("%Y-%m-%d %H:%M:%S")

            writer.writerow([
                prb_id,
                ts,
                readable_time,
                dst_addr if dst_addr else "",
                int(in_dns),
                avg if avg is not None else "",
                json.dumps(resolved_list)
            ])


def main():
    # File names from workspace
    dns_json = "RIPE-Atlas-measurement-131389881-1759824000-to-1759910400.json"
    ping_json = "RIPE-Atlas-measurement-131389882-1759788000-to-1759935240.json"
    output_csv = "correlated_ping_dns.csv"

    correlate(dns_json, ping_json, output_csv)
    print(f"Written: {output_csv}")


if __name__ == "__main__":
    main()


