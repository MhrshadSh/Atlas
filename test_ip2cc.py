from numpy.ma import count
from ip2ci import ip_to_country
from ip2ci import country_to_carbon_intensity

TOKEN = "052fb585189d4d6fb728f2cabb73a255"
EM_TOKEN = "ptTcw6cZ9zS07WgBYgXP"
# # ip_list = ["142.250.178.238", "142.250.203.110", "172.217.168.46", "172.217.168.78"]
ip_list = ["142.250.203.110"]
res = {}


for ip in ip_list:
    loc = ip_to_country(ip,TOKEN)
    lat = float(loc[0])
    lon = float(loc[1])
    res[ip] = country_to_carbon_intensity(lat, lon, EM_TOKEN)[0]["carbonIntensity"]
    print(res)


