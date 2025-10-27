import sys
from numpy.ma import count
from ip2ci import ip_to_loc, loc_to_ci, save_cache, load_cache

TOKEN = "052fb585189d4d6fb728f2cabb73a255"
EM_TOKEN = "ptTcw6cZ9zS07WgBYgXP"
CACHE_FILE = "./output/ip2ci_cache.json"
time = "2025-10-07T16:37:31"
# # ip_list = ["142.250.178.238", "142.250.203.110", "172.217.168.46", "172.217.168.78"]
ip_list = ["142.250.203.110"]
res = {}
# Load caches
ip2loc_cache, loc2ci_cache = load_cache(CACHE_FILE)

for ip in ip_list:
    # get location
    if ip in ip2loc_cache:
        loc = ip2loc_cache[ip]
    else:
        loc = ip_to_loc(ip,TOKEN) # loc is (location_Data, error)
        ip2loc_cache[ip] = loc

    if loc[1] is not None:
        print(f"Error fetching location for IP {ip}: {loc[1]}")
        continue

    lat = loc[0].get("latitude")
    lon = loc[0].get("longitude")

    # get carbon intensity
    if ip in loc2ci_cache:
        cached_ci = loc2ci_cache[ip]
        cached_time = cached_ci[0].get("datetime")  # Assuming datetime is stored in the cache
        if cached_time and cached_time[:13] == time[:13]:  # Compare until the hour
            ci = cached_ci
        else:
            ci = loc_to_ci(lat, lon, EM_TOKEN, time)  # ci is (data, error), where data is Dict[str, Any]
            loc2ci_cache[ip] = ci
    else:
        ci = loc_to_ci(lat, lon, EM_TOKEN, time)  # ci is (data, error), where data is Dict[str, Any]
        loc2ci_cache[ip] = ci
    if ci[1] is not None:
        print(f"Error fetching carbon intensity for IP {ip}: {ci[1]}")
        continue
    res[ip] = ci[0].get("carbonIntensity")

# Save cache at the end
try:
    save_cache(CACHE_FILE, ip2loc_cache, loc2ci_cache)
except Exception as e:
    sys.stderr.write(f"Failed to write cache: {e}\n")

print(res)


