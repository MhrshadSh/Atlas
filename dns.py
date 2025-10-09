import json
from ripe.atlas.sagan import DnsResult
from collections import defaultdict

def extract_probe_resolved_ips(json_file_path):
    """
    Extract resolved IP addresses from each probe in the RIPE Atlas DNS measurement
    using the ripe.atlas.sagan library, associating each set of IPs with their timestamp.
    """
    probe_results = {}
    
    with open(json_file_path, 'r') as file:
        for line_num, line in enumerate(file, 1):
            try:
                # Parse the measurement data using sagan
                dns_result = DnsResult(line.strip())
                
                # Get probe information
                prb_id = dns_result.probe_id
                from_ip = dns_result.origin
                measurement_timestamp = dns_result.created_timestamp
                
                # Initialize probe info if not exists
                if prb_id not in probe_results:
                    probe_results[prb_id] = {
                        'probe_id': prb_id,
                        'probe_ip': from_ip,
                        'measurements': {}  # Changed from 'resolved_ips' to 'measurements'
                    }
                
                # Initialize timestamp entry if not exists
                if measurement_timestamp not in probe_results[prb_id]['measurements']:
                    probe_results[prb_id]['measurements'][measurement_timestamp] = {
                        'timestamp': measurement_timestamp,
                        'resolved_ips': set(),
                        'query_times': []  # Store individual query timestamps
                    }
                
                # Extract resolved IP addresses from DNS responses and associate with timestamps
                for response in dns_result.responses:
                    if response.abuf and response.abuf.raw_data:
                        # Get answers from the parsed DNS message
                        answers = response.abuf.raw_data.get('AnswerSection', [])
                        for answer in answers:
                            # Check if this is an A record (IPv4)
                            if answer.get('Type') == 'A' and answer.get('Address'):
                                probe_results[prb_id]['measurements'][measurement_timestamp]['resolved_ips'].add(answer['Address'])
                            
                            # Check if this is an AAAA record (IPv6)
                            elif answer.get('Type') == 'AAAA' and answer.get('Address'):
                                probe_results[prb_id]['measurements'][measurement_timestamp]['resolved_ips'].add(answer['Address'])
                
                # Store individual query timestamps from the resultset
                for response in dns_result.responses:
                    if hasattr(response, 'time') and response.time:
                        probe_results[prb_id]['measurements'][measurement_timestamp]['query_times'].append(response.time)
                
            except Exception as e:
                print(f"Error processing line {line_num}: {e}")
                continue
    
    # Convert sets to sorted lists for consistent output and sort by timestamp
    for prb_id in probe_results:
        for timestamp in probe_results[prb_id]['measurements']:
            measurement = probe_results[prb_id]['measurements'][timestamp]
            measurement['resolved_ips'] = sorted(list(measurement['resolved_ips']))
            measurement['query_times'] = sorted(list(set(measurement['query_times'])))  # Remove duplicates and sort
    
    # Only return probes that have resolved IPs
    filtered_results = {}
    for prb_id, data in probe_results.items():
        if any(len(measurement['resolved_ips']) > 0 for measurement in data['measurements'].values()):
            filtered_results[prb_id] = data
    
    return filtered_results

def analyze_dns_responses(json_file_path, sample_size=5):
    """
    Analyze DNS responses in detail for a sample of probes.
    """
    print(f"\nDetailed DNS Response Analysis (Sample of {sample_size} probes):")
    print("=" * 80)
    
    sample_count = 0
    with open(json_file_path, 'r') as file:
        for line_num, line in enumerate(file, 1):
            if sample_count >= sample_size:
                break
                
            try:
                dns_result = DnsResult(line.strip())
                
                print(f"Probe ID: {dns_result.probe_id}")
                print(f"Probe IP: {dns_result.origin}")
                print(f"Query Time: {dns_result.created}")
                
                # Show query details
                for response in dns_result.responses:
                    if response.abuf and response.abuf.raw_data:
                        queries = response.abuf.raw_data.get('QuestionSection', [])
                        for query in queries:
                            print(f"Query: {query['Qname']} (Type: {query['Qtype']})")
                        
                        # Show answer details
                        answers = response.abuf.raw_data.get('AnswerSection', [])
                        print(f"Answers ({len(answers)}):")
                        for i, answer in enumerate(answers, 1):
                            answer_type = answer.get('Type')
                            address = answer.get('Address')
                            ttl = answer.get('TTL')
                            if answer_type in ['A', 'AAAA'] and address:
                                print(f"  {i}. Type: {answer_type}, Address: {address}, TTL: {ttl}")
                        
                        # Show response time
                        if response.response_time:
                            print(f"Response Time: {response.response_time} ms")
                        break
                
                # Show error information if any
                if dns_result.is_error:
                    print(f"Error: {dns_result.error_message}")
                
                print("-" * 80)
                sample_count += 1
                
            except Exception as e:
                print(f"Error processing line {line_num}: {e}")
                continue

def main():
    """
    Main function to extract and display resolved IP addresses from each probe with timestamps.
    """
    json_file = "RIPE-Atlas-measurement-131389881-1759824000-to-1759910400.json"
    
    print("Extracting resolved IP addresses from RIPE Atlas DNS measurement using ripe.atlas.sagan...")
    probe_results = extract_probe_resolved_ips(json_file)
    
    print(f"\nFound {len(probe_results)} probes with resolved IP addresses:")
    print("=" * 80)
    
    total_measurements = 0
    total_unique_ips = set()
    
    for prb_id in sorted(probe_results.keys()):
        result = probe_results[prb_id]
        print(f"Probe ID: {result['probe_id']}")
        print(f"Probe IP: {result['probe_ip']}")
        
        # Sort measurements by timestamp
        sorted_timestamps = sorted(result['measurements'].keys())
        print(f"Measurements ({len(sorted_timestamps)}):")
        
        for timestamp in sorted_timestamps:
            measurement = result['measurements'][timestamp]
            total_measurements += 1
            
            # Convert Unix timestamp to readable format
            from datetime import datetime
            readable_time = datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')
            
            print(f"  Timestamp: {timestamp} ({readable_time})")
            print(f"  Resolved IPs ({len(measurement['resolved_ips'])}):")
            for ip in measurement['resolved_ips']:
                print(f"    {ip}")
                total_unique_ips.add(ip)
            
            if measurement['query_times']:
                print(f"  Query times: {measurement['query_times']}")
            print()
        
        print("-" * 80)
    
    print(f"\nSummary:")
    print(f"Total probes with DNS results: {len(probe_results)}")
    print(f"Total measurements: {total_measurements}")
    print(f"Total unique resolved IP addresses: {len(total_unique_ips)}")
    
    # Show detailed analysis for a sample
    analyze_dns_responses(json_file, sample_size=3)

if __name__ == "__main__":
    main()
