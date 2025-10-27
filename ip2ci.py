#!/usr/bin/env python3
import argparse
import csv
import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Dict, Optional, Tuple, Any, Set


IPLOCATION_ENDPOINT = "https://api.ipgeolocation.io/v2/ipgeo"
ELECTRICITYMAPS_ENDPOINT = "https://api.electricitymaps.com/v3/carbon-intensity"


def http_get_json(url: str, headers: Optional[Dict[str, str]] = None, timeout: float = 10.0) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    req = urllib.request.Request(url)
    if headers:
        for k, v in headers.items():
            req.add_header(k, v)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            charset = resp.headers.get_content_charset() or "utf-8"
            data = resp.read().decode(charset, errors="replace")
            return json.loads(data), None
    except urllib.error.HTTPError as e:
        try:
            body = e.read().decode("utf-8", errors="replace")
        except Exception:
            body = str(e)
        return None, f"HTTPError {e.code}: {body}"
    except urllib.error.URLError as e:
        return None, f"URLError: {e.reason}"
    except Exception as e:
        return None, f"Error: {e}"


def ip_to_loc(ip: str, token:str) -> Tuple[Optional[Dict], Optional[str]]:
    """
    Returns (location_Data, error)
    """
    url = f"{IPLOCATION_ENDPOINT}?apiKey={urllib.parse.quote(token)}&ip={urllib.parse.quote(ip)}"
    data, err = http_get_json(url)
    if err:
        return None, err
    if not isinstance(data, dict):
        return None, "invalid JSON"
    if "location" not in data:
        return None, "missing location field"

    return (data["location"], None)


def loc_to_ci(lat: str, lon: str, token: str, time: Optional[str]=None) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    """
    Query ElectricityMaps for carbon intensity of a geographical location. Returns (data, error).
    If time is None, fetch latest; otherwise fetch past data for the given ISO 8601 datetime string.
    """
    global ELECTRICITYMAPS_ENDPOINT

    if time is None:
        query = urllib.parse.urlencode({"lat": lat, "lon": lon})
        ELECTRICITYMAPS_ENDPOINT += "/latest"
    else:
        query = urllib.parse.urlencode({"lat": lat, "lon": lon, "datetime": time})
        ELECTRICITYMAPS_ENDPOINT += "/past"
    
    url = f"{ELECTRICITYMAPS_ENDPOINT}?{query}"
    headers = {"auth-token": token}
    data, err = http_get_json(url, headers=headers)
    if err:
        return None, err
    if not isinstance(data, dict):
        return None, "invalid JSON"
    return data, None


def load_cache(cache_path: Optional[str]) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    if not cache_path or not os.path.exists(cache_path):
        return {}, {}
    try:
        with open(cache_path, "r", encoding="utf-8") as f:
            blob = json.load(f)
        return blob.get("ip_country", {}), blob.get("country_carbon", {})
    except Exception:
        return {}, {}


def save_cache(cache_path: Optional[str], ip_country: Dict[str, Any], country_carbon: Dict[str, Any]) -> None:
    if not cache_path:
        return
    tmp_path = f"{cache_path}.tmp"
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump({"ip_country": ip_country, "country_carbon": country_carbon}, f, ensure_ascii=False, indent=2)
    os.replace(tmp_path, cache_path)


def read_unique_ips_from_dns_csv(dns_csv_path: str) -> Set[str]:
    unique_ips: Set[str] = set()
    with open(dns_csv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        if "ips" not in reader.fieldnames:
            raise ValueError("Input CSV missing required 'ips' column")
        for row in reader:
            ips_field = row.get("ips", "").strip()
            if not ips_field:
                continue
            for ip in ips_field.split(";"):
                ip_s = ip.strip()
                if ip_s:
                    unique_ips.add(ip_s)
    return unique_ips


def write_output_csv(rows: Any, out_csv_path: str) -> None:
    headers = [
        "ip",
        "country_code",
        "country_name",
        "carbonIntensity",
        "datetime",
        "updatedAt",
        "emissionFactorType",
        "isEstimated",
        "error",
    ]
    os.makedirs(os.path.dirname(out_csv_path) or ".", exist_ok=True)
    with open(out_csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        for r in rows:
            writer.writerow(r)


def main() -> None:
    parser = argparse.ArgumentParser(description="Map IPs from DNS CSV to country and carbon intensity")
    parser.add_argument("-i", "--input", default="./output/dns_a_records.csv", help="Path to dns_a_records.csv")
    parser.add_argument("-o", "--output", default="./output/ip_country_carbon.csv", help="Path to write output CSV")
    parser.add_argument("--token", required=True, help="ElectricityMaps API auth-token")
    parser.add_argument("--cache", default="./output/ip_country_carbon_cache.json", help="Optional cache JSON path")
    parser.add_argument("--sleep", type=float, default=0.2, help="Sleep seconds between API calls (rate limiting)")
    args = parser.parse_args()

    # Load inputs
    try:
        unique_ips = read_unique_ips_from_dns_csv(args.input)
    except Exception as e:
        sys.stderr.write(f"Failed to read input CSV: {e}\n")
        sys.exit(1)

    # Load caches
    ip_country_cache, country_carbon_cache = load_cache(args.cache)

    rows = []
    for ip in sorted(unique_ips):
        # IP -> country
        if ip in ip_country_cache:
            country_code = ip_country_cache[ip].get("country_code")
            country_name = ip_country_cache[ip].get("country_name")
            ip_country_err = None
        else:
            code, name, ip_err = ip_to_loc(ip)
            ip_country_cache[ip] = {"country_code": code, "country_name": name, "error": ip_err}
            country_code, country_name, ip_country_err = code, name, ip_err
            # Rate limiting friendly
            time.sleep(args.sleep)

        # Country -> carbon
        carbon = None
        carbon_err = None
        if country_code:
            if country_code in country_carbon_cache:
                carbon = country_carbon_cache[country_code]
                carbon_err = carbon.get("error")
            else:
                data, err = loc_to_ci(country_code, args.token)
                if err:
                    country_carbon_cache[country_code] = {"error": err}
                    carbon_err = err
                else:
                    country_carbon_cache[country_code] = data
                    carbon = data
                time.sleep(args.sleep)
        else:
            carbon_err = ip_country_err or "no country code"

        # Compose row
        row = {
            "ip": ip,
            "country_code": country_code or "",
            "country_name": country_name or "",
            "carbonIntensity": (carbon or {}).get("carbonIntensity", ""),
            "datetime": (carbon or {}).get("datetime", ""),
            "updatedAt": (carbon or {}).get("updatedAt", ""),
            "emissionFactorType": (carbon or {}).get("emissionFactorType", ""),
            "isEstimated": (carbon or {}).get("isEstimated", ""),
            "error": carbon_err or ip_country_err or "",
        }
        rows.append(row)

    # Save outputs
    try:
        write_output_csv(rows, args.output)
    except Exception as e:
        sys.stderr.write(f"Failed to write output CSV: {e}\n")
        sys.exit(1)

    # Save cache at the end
    try:
        save_cache(args.cache, ip_country_cache, country_carbon_cache)
    except Exception as e:
        sys.stderr.write(f"Failed to write cache: {e}\n")


if __name__ == "__main__":
    main()


