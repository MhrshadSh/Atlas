from numpy.ma import count
from ip2ci import ip_to_loc, loc_to_ci, save_cache, load_cache

TOKEN = "052fb585189d4d6fb728f2cabb73a255"
EM_TOKEN = "ptTcw6cZ9zS07WgBYgXP"
CACHE_FILE = "./output/ip2ci_cache.json"
time = "2025-10-07T15:37:31"
# # ip_list = ["142.250.178.238", "142.250.203.110", "172.217.168.46", "172.217.168.78"]
ip_list = ["142.250.203.110"]
res = {}


for ip in ip_list:
    # get location
    loc = ip_to_loc(ip,TOKEN) # loc is (location_Data, error)
    if loc[1] is not None:
        print(f"Error fetching location for IP {ip}: {loc[1]}")
        continue
    lat = loc[0].get("latitude")
    lon = loc[0].get("longitude")
    # get carbon intensity
    ci = loc_to_ci(lat, lon, EM_TOKEN, time) # ci is (data, error), where data is Dict[str, Any]
    if ci[1] is not None:
        print(f"Error fetching carbon intensity for IP {ip}: {ci[1]}")
        continue
    res[ip] = ci[0].get("carbonIntensity")

print(res)


