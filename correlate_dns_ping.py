import json
import csv
import sys
import os
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime
from ip2ci import ip_to_loc, loc_to_ci, load_cache, save_cache

# Local import
from dns import extract_probe_resolved_ips

# Constants
IPGEO_TOKEN = "052fb585189d4d6fb728f2cabb73a255"
EM_TOKEN = "ptTcw6cZ9zS07WgBYgXP"
CACHE_FILE = "./output/ip2ci_cache.json"
DNS_CACHE_FILE = "./output/dns_extract_cache.json"

# Load caches
ip2loc_cache, loc2ci_cache = load_cache(CACHE_FILE)


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


def _file_fingerprint(path: str) -> str:
    """
    Create a simple fingerprint for a file based on absolute path, size and mtime.
    """
    ap = os.path.abspath(path)
    try:
        st = os.stat(ap)
        size = st.st_size
        mtime = int(st.st_mtime)
    except FileNotFoundError:
        size = -1
        mtime = -1
    return f"{ap}|{size}|{mtime}"


def _dns_cache_load(cache_path: str) -> Dict[str, Any]:
    try:
        if not os.path.exists(cache_path):
            return {}
        with open(cache_path, "r", encoding="utf-8") as f:
            blob = json.load(f)
        return blob if isinstance(blob, dict) else {}
    except Exception:
        return {}


def _dns_cache_save(cache_path: str, cache_obj: Dict[str, Any]) -> None:
    os.makedirs(os.path.dirname(cache_path) or ".", exist_ok=True)
    tmp = f"{cache_path}.tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(cache_obj, f, ensure_ascii=False)
    os.replace(tmp, cache_path)


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

def add_ci_to_row(ip_list: List[str], dst_ip: str, time: Optional[str]) -> Tuple[list, float]:
    """
    Given a list of IPs and a destination IP, return a tuple containing:
    - A list of carbon intensities corresponding to each IP in the list.
    - The carbon intensity of the destination IP if it exists in the list; otherwise, -1.
    """
    global ip2loc_cache, loc2ci_cache
    ci_list = []
    dst_ci = -1.0

    for ip in ip_list:
        # get location
        if ip in ip2loc_cache:
            loc_data = ip2loc_cache[ip]
        else:
            loc_data, loc_err = ip_to_loc(ip,IPGEO_TOKEN) # loc is (location_Data, error)    
            if loc_err is not None:
                print(f"Error fetching location for IP {ip}: {loc_err}")
                ci_list.append(None)
                continue
            else:
                ip2loc_cache[ip] = loc_data

        lat = loc_data.get("latitude")
        lon = loc_data.get("longitude")

        # get carbon intensity
        if ip in loc2ci_cache:
            cached_ci = loc2ci_cache[ip]
            cached_time = cached_ci.get("datetime")  # Assuming datetime is stored in the cache
            if cached_time and cached_time[:13] == time[:13]:  # Compare until the hour
                ci_data = cached_ci
            else:
                ci_data, ci_err = loc_to_ci(lat, lon, EM_TOKEN, time)  # ci is (data, error), where data is Dict[str, Any]
                if ci_err is not None:
                    print(f"Error fetching carbon intensity for IP {ip}: {ci_err}")
                    ci_list.append(None)
                    continue
                else:
                    loc2ci_cache[ip] = ci_data
        else:
            ci_data, ci_err = loc_to_ci(lat, lon, EM_TOKEN, time)  # ci is (data, error), where data is Dict[str, Any]
            if ci_err is not None:
                print(f"Error fetching carbon intensity for IP {ip}: {ci_err}")
                ci_list.append(None)
                continue
            else:
                loc2ci_cache[ip] = ci_data

        carbon_intensity = ci_data.get("carbonIntensity")
        ci_list.append(carbon_intensity)

        if ip == dst_ip:
            dst_ci = carbon_intensity
        
    
         
    return ci_list, dst_ci


def correlate(dns_json_path: str, ping_json_path: str, output_csv_path: str, max_rows: int) -> None:
    """
    Correlate DNS and ping measurements and write to CSV.
    """
    # Try DNS extraction cache first
    dns_cache = _dns_cache_load(DNS_CACHE_FILE)
    dns_key = _file_fingerprint(dns_json_path)
    if dns_key in dns_cache:
        dns_results = dns_cache[dns_key]
    else:
        dns_results = extract_probe_resolved_ips(dns_json_path)
        dns_cache[dns_key] = dns_results
        _dns_cache_save(DNS_CACHE_FILE, dns_cache)
    dns_index = build_dns_index(dns_results)

    with open(ping_json_path, "r") as fin, open(output_csv_path, "w", newline="") as fout:
        writer = csv.writer(fout)
        writer.writerow(["probe_id", "timestamp", "readable_time", "src_ip", "selected_ip", "in_dns_set", "avg_rtt", "resolved_set", "ci_list", "selected_ip_ci"]) 

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
            src_addr = obj.get("src_addr")
            dst_addr = obj.get("dst_addr")
            avg = obj.get("avg", None)

            # Skip if required fields missing
            if prb_id is None or ts is None:
                continue

            selected_set = find_latest_resolved_set(dns_index.get(int(prb_id), []), int(ts))
            in_dns = dst_addr in selected_set if dst_addr else False
            resolved_list = sorted(list(selected_set)) if selected_set else []

            readable_time = datetime.fromtimestamp(int(ts)).isoformat()
            
            # Add carbon intensity information
            ci_list, dst_ci = add_ci_to_row(resolved_list, dst_addr if dst_addr else "", readable_time)

            writer.writerow([
                prb_id,
                ts,
                readable_time,
                src_addr if src_addr else "",
                dst_addr if dst_addr else "",
                int(in_dns),
                avg if avg is not None else "",
                json.dumps(resolved_list),
                json.dumps(ci_list),
                dst_ci
            ])
            if line_num >= max_rows:
                break


def main():
    # File names from workspace
    dns_json = "RIPE-Atlas-measurement-131389881-1759824000-to-1759910400.json"
    ping_json = "RIPE-Atlas-measurement-131389882-1759788000-to-1759935240.json"
    output_csv = "./output/correlated_ping_dns.csv"
    max_rows = 2 # Limit number of processed rows for testing

    correlate(dns_json, ping_json, output_csv, max_rows)
    print(f"Written: {output_csv}")
    # Save caches at the end
    try:
        save_cache(CACHE_FILE, ip2loc_cache, loc2ci_cache)
    except Exception as e:
        sys.stderr.write(f"Failed to write cache: {e}\n")


if __name__ == "__main__":
    main()


