## Atlas

Analyze RIPE Atlas DNS results and correlate them with ping measurements.

### Overview
- **`dns.py`**: Parses RIPE Atlas DNS measurement lines using `ripe.atlas.sagan` and builds, per probe, timestamped sets of resolved IPs. Includes a small analysis helper.
- **`correlate_dns_ping.py`**: For each ping result, finds the most relevant DNS resolution set for the same probe and nearby timestamp, then writes a correlation CSV.

### Requirements
- Python 3.9+
- pip

Install the required library:

```bash
pip install ripe.atlas.sagan
```

### Data Inputs
Place RIPE Atlas measurement files (line-delimited JSON) in the project root. The default filenames used by the scripts are:
- `RIPE-Atlas-measurement-131389881-1759824000-to-1759910400.json` (DNS)
- `RIPE-Atlas-measurement-131389882-1759788000-to-1759935240.json` (Ping)

You can change these by editing the `main()` functions in the scripts.

### Usage
Run DNS parsing and quick inspection:

```bash
python dns.py
```

Correlate ping with DNS and produce a CSV:

```bash
python correlate_dns_ping.py
```

This writes `correlated_ping_dns.csv` with columns:
- `probe_id`
- `timestamp`
- `readable_time`
- `selected_ip`
- `in_dns_set` (1/0)
- `avg_rtt`
- `resolved_set` (JSON array)

### Notes
- Input files are expected to be one JSON object per line.
- Correlation selects the DNS result whose timestamp shares the longest decimal prefix with the ping timestamp; ties break by nearest absolute time, then by latest timestamp.

### Repository Hygiene
Large data artifacts like `*.csv` and `*.json` are ignored via `.gitignore` by default. If you need to commit sample data, rename or adjust `.gitignore` accordingly.


