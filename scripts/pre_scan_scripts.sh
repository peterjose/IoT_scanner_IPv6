#! /bin/bash

echo "Pre Scan Scripts"
echo "Running Pre Scan Script"

source config.sh
source /local/ipv6-iot-scans/env1/bin/activate
python3 pre_scan_data_fetcher.py -s $hitlist_path -a $apd_db_path -e $additional_db_path -d $db_location_json
python3 pre_scan_multistage_ip_filter.py -d $db_location_json -b $IP_BLOCKLIST_FILE

processed_hitlist_path="$(find $processed_hitlist_dir_path -name "*_processed.txt")"
export processed_hitlist_path

echo "Pre script is completed"

