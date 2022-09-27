#! /bin/bash

echo "Scan Ended"
echo "Running Post Scan Script"

source config.sh
source /local/ipv6-iot-scans/env1/bin/activate
python3 post_scan_script_and_webpage_data_generator.py -O $OutputPath

echo "Post script is completed"
#TODO: Get the log of the files in the  
