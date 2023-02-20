# This file contains all the configuration details for the scan

#! /bin/bash

# Don't change
ENABLED=1
DISABLED=0

# Help option for config file
if [ $1 ]; then
  if [ "$1" == "--help" ] || [ "$1" == "-h" ]; then
    cat ../README.md
  else
    echo "Wrong Argument, use 'config.sh --help' or 'config.sh -h'"
  fi
  exit
fi

############# Enable / Disable Flags ###
##
# Use ENABLED for enabling a feature
# Use DISABLED for disabling a feature

# For IPv6 Support
IPV6_SCAN_FLAG=$ENABLED
# Enable this flag for enabling the BLOCKLIST
USE_BLOCK_IP_LIST_FLAG=$DISABLED
# Scan from a list of IPs 
USE_SCAN_IP_LIST_FLAG=$ENABLED
# Flag if enabled will use the default search list when USE_SCAN_IP_LIST_FLAG is enabled
USE_DEFAULT_SCAN_IP_LIST_FLAG=$ENABLED
# Scan IP count Limit flag, when enabled it set the cap limit of ip to be returned
IP_COUNT_LIMIT_FLAG=$DISABLED
# Enable this flag for enabling the debug print statements
DEBUG_PRINT_FLAG=$ENABLED
# Enable this to ignore error related to blocklist
IGNORE_BLOCKLIST_ERROR_FLAG=$ENABLED

## Protocol Support

SCAN_XMPP_FLAG=$ENABLED	#XMPP
SCAN_MQTT_FLAG=$ENABLED	#MQTT
SCAN_AMQP_FLAG=$ENABLED	#AMQP
SCAN_OPCUA_FLAG=$ENABLED #OPCUA
SCAN_COAP_FLAG=$ENABLED #CoAP
SCAN_TELNET_FLAG=$ENABLED #Telnet
SCAN_DTLS_FLAG=$DISABLED #DTLS

POST_SCAN_SCRIPTS=$DISABLED

##
########################################

### Change here for the number of IP to be scanned for each protocol
DEFAULT_NUMBER_OF_IP=100
DEFAULT_SCAN_RATE=15000       # packets/sec
IPv6_SOURCE_IP_ADDRESS="2a02:d480:4c0:10d4:42::1"
SCAN_INTERFACE="ens2f0"
DEFAULT_SCAN_LIST_FILENAME="hitlist_processed/file_name.txt"
DEFAULT_NO_OF_SENDERS_ZGRAB=500

########### PATH ########################

zgrab2Path=../zgrab2/
zmap6Path=../zmap6/src/
ipv4Blocklist=../blocklist/release/ipv4-bl-merged.txt
ipv6Blocklist=../blocklist/release/ipv6-bl-merged.txt
listOfIP_path_base=../searchlist
extractIP_addressScriptFile=extract_saddr.py

if [ ! $OutputPath ]; then
  if [ $IPV6_SCAN_FLAG = $ENABLED ]; then
    OutputPath=../output/ipv6/"$(date +"%d-%m-%Y")" #/"$(date +"%H%M%S")"
  else
    OutputPath=../output/ipv4/"$(date +"%d-%m-%Y")" #/"$(date +"%H%M%S")"
  fi
fi

if [ $IPV6_SCAN_FLAG = $ENABLED ]; then
  summaryDirPath=../output/ipv6/summary
else
  summaryDirPath=../output/ipv4/summary
fi

if [ ! -d $OutputPath ]; then
    mkdir -p $OutputPath;
fi;
# create summary directory
if [ ! -d $summaryDirPath ]; then
    mkdir -p $summaryDirPath;
fi;

if [ ! $listOfIP_path ]; then
  if [ $IPV6_SCAN_FLAG = $ENABLED ]; then
    listOfIP_path=$listOfIP_path_base/ipv6
  else
    listOfIP_path=$listOfIP_path_base/ipv4
  fi
fi

if [ ! -d $listOfIP_path ]; then
    mkdir -p $listOfIP_path;
fi;

if [ ! $db_location_json ]; then
  db_location_json=${listOfIP_path}/"$(date +"%d-%m-%Y")"/config_db_locations.json
fi
if [ ! -f $db_location_json ]; then
    echo {} > $db_location_json;
fi;

# location of unfiltered hitlist
if [ ! $hitlist_path ]; then
  hitlist_path=${listOfIP_path}/"$(date +"%d-%m-%Y")"/hitlist_TUM_input #/"$(date +"%H%M%S")"
fi
if [ ! -d $hitlist_path ]; then
    mkdir -p $hitlist_path;
fi;

if [ ! $processed_hitlist_dir_path ]; then
  processed_hitlist_dir_path=${listOfIP_path}/"$(date +"%d-%m-%Y")"/hitlist_processed #/"$(date +"%H%M%S")"
fi
if [ ! -d $processed_hitlist_dir_path ]; then
    mkdir -p $processed_hitlist_dir_path;
fi;


# location of Aliased Prefix Datasets
if [ ! $apd_db_path ]; then
  apd_db_path=${listOfIP_path}/"$(date +"%d-%m-%Y")"/apd_TUM #/"$(date +"%H%M%S")"
fi
if [ ! -d $apd_db_path ]; then
    mkdir -p $apd_db_path;
fi;

# Additional DB

if [ ! $additional_db_path ]; then
  if [ $IPV6_SCAN_FLAG = $ENABLED ]; then
    additional_db_path=../output/ipv6/"$(date +"%d-%m-%Y")"/additional_external_db #/"$(date +"%H%M%S")"
  else
    additional_db_path=../output/ipv4/"$(date +"%d-%m-%Y")"/additional_external_db #/"$(date +"%H%M%S")"
  fi
fi
if [ ! -d $additional_db_path ]; then
    mkdir -p $additional_db_path;
fi;

#########################################

if [ $IPV6_SCAN_FLAG = $ENABLED ]; then
  # IPV6_SCAN_CMD_OPTION="--probe-module=icmp6_echoscan"
  IPV6_SCAN_CMD_OPTION="--probe-module=ipv6_tcp_synscan"
  IP_BLOCKLIST_FILE=$ipv6Blocklist
  IPv6_SOURCE_IP_ADDRESS_OPTION_ZMAP="--ipv6-source-ip=$IPv6_SOURCE_IP_ADDRESS"
  IPv6_SOURCE_IP_ADDRESS_ZGRAB_OPTION="--source-ip=$IPv6_SOURCE_IP_ADDRESS"
  SCAN_INTERFACE_OPTION="--interface=$SCAN_INTERFACE"
else
  IP_BLOCKLIST_FILE=$ipv4Blocklist
fi

DEFAULT_ZMAP_OUTPUT_FIELDS="saddr,daddr,sport,dport,ipid,ttl,seqnum,acknum,window,classification,success,repeat,cooldown,timestamp_str"

########### PROTOCOL config #############

#XMPP
XMPP_SCAN_PORT=5222
XMPP_NUMBER_OF_IP=$DEFAULT_NUMBER_OF_IP
XMPP_SCAN_RATE=$DEFAULT_SCAN_RATE
XMPP_PROTOCOL_NAME=xmpp
XMPP_TIMEOUT=10
XMPP_SCAN_IP_LIST=$listOfIP_path/xmpp_5222.txt
# XMPP Secured
XMPP_SECURE_SCAN_PORT=5223
XMPP_SECURE_SCAN_IP_LIST=$listOfIP_path/xmpp_5223.txt

# MQTT
MQTT_SCAN_PORT=1883
MQTT_NUMBER_OF_IP=$DEFAULT_NUMBER_OF_IP
MQTT_SCAN_RATE=$DEFAULT_SCAN_RATE
MQTT_PROTOCOL_NAME=mqtt
MQTT_TIMEOUT=10
MQTT_SCAN_IP_LIST=$listOfIP_path/mqtt_1883.txt
# MQTT Secured
MQTT_SECURE_SCAN_PORT=8883
MQTT_SECURE_SCAN_IP_LIST=$listOfIP_path/mqtt_8883.txt

# AMQP 
AMQP_SCAN_PORT=5672
AMQP_NUMBER_OF_IP=$DEFAULT_NUMBER_OF_IP
AMQP_SCAN_RATE=$DEFAULT_SCAN_RATE
AMQP_PROTOCOL_NAME=amqp
AMQP_TIMEOUT=10
AMQP_SCAN_IP_LIST=$listOfIP_path/amqp_5672.txt
# AMQP Secured
AMQP_SECURE_SCAN_PORT=5671
AMQP_SECURE_SCAN_IP_LIST=$listOfIP_path/amqp_5671.txt

# OPCUA 
OPCUA_SCAN_PORT=4840
OPCUA_NUMBER_OF_IP=$DEFAULT_NUMBER_OF_IP
OPCUA_SCAN_RATE=$DEFAULT_SCAN_RATE
OPCUA_PROTOCOL_NAME=opcua
OPCUA_TIMEOUT=10
OPCUA_SCAN_IP_LIST=$listOfIP_path/opcua_4840.txt
# OPCUA Secured
OPCUA_SECURE_SCAN_PORT=4843
OPCUA_SECURE_SCAN_IP_LIST=$listOfIP_path/opcua_4843.txt

# COAP
COAP_SCAN_PORT=5683
COAP_NUMBER_OF_IP=$DEFAULT_NUMBER_OF_IP
COAP_SCAN_RATE=$DEFAULT_SCAN_RATE
COAP_PROTOCOL_NAME=coap
COAP_TIMEOUT=10
COAP_SCAN_IP_LIST=$listOfIP_path/coap_5683.txt
COAP_IPV6_SCAN_CMD_OPTION="--probe-module=ipv6_udp"
# COAP Secured
COAPS_SCAN_PORT=5684
COAPS_SCAN_IP_LIST=$listOfIP_path/coaps_5684.txt

#TELNET
TELNET_SCAN_PORT=23
TELNET_NUMBER_OF_IP=$DEFAULT_NUMBER_OF_IP
TELNET_SCAN_RATE=$DEFAULT_SCAN_RATE
TELNET_PROTOCOL_NAME=telnet
TELNET_TIMEOUT=10
TELNET_SCAN_IP_LIST=$listOfIP_path/telnet_23.txt

#TELNET
DTLS_SCAN_PORT=443
DTLS_NUMBER_OF_IP=$DEFAULT_NUMBER_OF_IP
DTLS_SCAN_RATE=$DEFAULT_SCAN_RATE
DTLS_PROTOCOL_NAME=dtls
DTLS_TIMEOUT=10
DTLS_SCAN_IP_LIST=$listOfIP_path/dtls_443.txt
DTLS_IPV6_SCAN_CMD_OPTION="--probe-module=ipv6_udp"

########### FUNCTIONS ###################

scan_function () {

  local OUTPUT_PATH=$OutputPath/$PROTOCOL_NAME
  if [ ! -d $OUTPUT_PATH ]; then
    mkdir -p $OUTPUT_PATH;
  fi;

  # check if TLS option to be enabled
  if [[ $1 = "TLS_ENABLED" ]]; then
    local TLS_OPTION="--tls"  
  fi

  # check if TLS option to be enabled
  if [[ $1 = "DTLS_ENABLED" ]]; then
    local DTLS_OPTION="--dtls"  
  fi
  
  # check for all the possible scanning options
  if [ $SCAN_PORT ]; then
    local SCAN_PORT_OPTION="-p $SCAN_PORT"  
  fi
  if [ $NUMBER_OF_IP ] && [ $IP_COUNT_LIMIT_FLAG = $ENABLED ]; then
    local NUMBER_OF_IP_OPTION="-N $NUMBER_OF_IP"  
  fi
  if [ $SCAN_RATE ]; then
    local SCAN_RATE_OPTION="--rate=$SCAN_RATE"  
  fi
  if [ $TIMEOUT ]; then
    local TIMEOUT_OPTION="--timeout $TIMEOUT"  
  fi
  # If default scan list has to be used then
  if [ $USE_DEFAULT_SCAN_IP_LIST_FLAG = $ENABLED ]; then
    # SCAN_LIST=$listOfIP_path/$DEFAULT_SCAN_LIST_FILENAME
      SCAN_LIST=$processed_hitlist_path
  fi
  if [ $SCAN_LIST ] && [ $USE_SCAN_IP_LIST_FLAG = $ENABLED ]; then
    if [ $IPV6_SCAN_FLAG = $ENABLED ]; then
	    local SCAN_IP_LIST_OPTION="--ipv6-target-file=$SCAN_LIST"
    else
      local SCAN_IP_LIST_OPTION="--allowlist-file=$SCAN_LIST"  
	    # local SCAN_IP_LIST_OPTION="--list-of-ips-file=$SCAN_LIST"       # Should be used if the number of IPs is more than 1 million
    fi
  fi
  if [ $IP_BLOCKLIST_FILE ] && [ $USE_BLOCK_IP_LIST_FLAG = $ENABLED ]; then
    local IP_BLOCKLIST_FILE_OPTION="-b $IP_BLOCKLIST_FILE"
    if [ $IGNORE_BLOCKLIST_ERROR_FLAG = $ENABLED ]; then
      local IP_BLOCKLIST_IGNORE_ERROR_OPTION="--ignore-blocklist-errors"
    fi
  fi
  if [ $DEFAULT_NO_OF_SENDERS_ZGRAB ]; then
    local NO_OF_SENDERS_ZGRAB_OPTION="--senders=$DEFAULT_NO_OF_SENDERS_ZGRAB"  
  fi
  if [ $PROBE_ARGUMENT ]; then
    local PROBE_ARGS_OPTION="--probe-args=file:$PROBE_ARGUMENT"  
  fi


  echo 
  echo "***********************************"
  echo "Scanning " $PROTOCOL_NAME " port " $SCAN_PORT
  
  if [ $DEBUG_PRINT_FLAG = $ENABLED ]; then
    echo "********** Scan options ***********"
    echo "SCAN_PORT      " $SCAN_PORT_OPTION
    echo "NUMBER_OF_IP   " $NUMBER_OF_IP_OPTION
    echo "SCAN_RATE      " $SCAN_RATE_OPTION
    echo "TIMEOUT        " $TIMEOUT_OPTION
    echo "OUTPUT_FILENAME " $OUTPUT_FILENAME 
    echo "SCAN_LIST      " $SCAN_IP_LIST_OPTION
    echo "BLOCK_LIST     " $IP_BLOCKLIST_FILE_OPTION " " $IP_BLOCKLIST_IGNORE_ERROR_OPTION
    echo "NO_OF_SENDERS_ZGRAB_OPTION" $NO_OF_SENDERS_ZGRAB_OPTION
    echo "ZMAP_OUTPUT_FIELDS" $ZMAP_OUTPUT_FIELDS
    echo "PROBE_ARGS_OPTION" $PROBE_ARGS_OPTION
    echo "TLS_OPTION" $TLS_OPTION
    echo "DTLS_OPTION" $DTLS_OPTION
  fi
  
  echo $OUTPUT_FILENAME >> $summaryDirPath/complete.json
  echo "Started "$(date +"%d-%m-%Y:%H:%M:%S")"" >> $summaryDirPath/complete.json

  # ZMAP OUTPUT File list
  ZMAP_OUT_FILE=$OUTPUT_PATH/${OUTPUT_FILENAME}_zmap_output.csv
  ZMAP_LOG_FILE=$OUTPUT_PATH/${OUTPUT_FILENAME}_zmap_log.txt
  ZMAP_META_FILE=$OUTPUT_PATH/${OUTPUT_FILENAME}_zmap_metadata.json
  ZMAP_STATUS_UPDATE_FILE=$OUTPUT_PATH/${OUTPUT_FILENAME}_zmap_status_update.csv
  ZMAP_OUTPUT_IP_LIST=$OUTPUT_PATH/${OUTPUT_FILENAME}_zmap_ip_list.csv

  # ZGRAB OUTPUT File list
  ZGRAB_OUT_FILE=$OUTPUT_PATH/${OUTPUT_FILENAME}_zgrab2_out.json
  ZGRAB_METADATA_FILE=$OUTPUT_PATH/${OUTPUT_FILENAME}_scan_summary.json
  ZGRAB_LOG_FILE=$OUTPUT_PATH/${OUTPUT_FILENAME}_zgrab2_log.json 

  if [ $IPV6_SCAN_FLAG = $ENABLED ]; then
    sudo $zmap6Path./zmap $IPV6_SCAN_CMD_OPTION $IPv6_SOURCE_IP_ADDRESS_OPTION_ZMAP $SCAN_INTERFACE_OPTION $SCAN_PORT_OPTION $NUMBER_OF_IP_OPTION $SCAN_RATE_OPTION $IP_BLOCKLIST_FILE_OPTION $SCAN_IP_LIST_OPTION $IP_BLOCKLIST_IGNORE_ERROR_OPTION $PROBE_ARGS_OPTION --output-module="csv" -f $ZMAP_OUTPUT_FIELDS -o $ZMAP_OUT_FILE -l $ZMAP_LOG_FILE -m $ZMAP_META_FILE -u $ZMAP_STATUS_UPDATE_FILE
  else
    sudo $zmap6Path./zmap $SCAN_PORT_OPTION $NUMBER_OF_IP_OPTION $SCAN_RATE_OPTION $IP_BLOCKLIST_FILE_OPTION $SCAN_IP_LIST_OPTION $IP_BLOCKLIST_IGNORE_ERROR_OPTION $PROBE_ARGS_OPTION --output-module="csv" -f $ZMAP_OUTPUT_FIELDS -o $ZMAP_OUT_FILE -l $ZMAP_LOG_FILE -m $ZMAP_META_FILE -u $ZMAP_STATUS_UPDATE_FILE
  fi

  # extract the saddr to a separate file
  python3 $extractIP_addressScriptFile -i $ZMAP_OUT_FILE -o $ZMAP_OUTPUT_IP_LIST

  sudo cat $ZMAP_OUTPUT_IP_LIST | $zgrab2Path./zgrab2 $PROTOCOL_NAME $SCAN_PORT_OPTION $IPv6_SOURCE_IP_ADDRESS_ZGRAB_OPTION $TIMEOUT_OPTION $NO_OF_SENDERS_ZGRAB_OPTION --debug -o $ZGRAB_OUT_FILE --metadata-file=$ZGRAB_METADATA_FILE --log-file=$ZGRAB_LOG_FILE $TLS_OPTION $DTLS_OPTION

  if [ -f $ZGRAB_METADATA_FILE ]; then
    cat $ZGRAB_METADATA_FILE >> $summaryDirPath/${PROTOCOL_NAME}_${SCAN_PORT}.json
    cat $ZGRAB_METADATA_FILE >> $summaryDirPath/complete.json
    echo "Ended "$(date +"%d-%m-%Y:%H:%M:%S")"" >> $summaryDirPath/complete.json
  fi;

  # Display Log, It will not be in sync with the scan, this part of log will be displayed after scan 
  if [ $DEBUG_PRINT_FLAG  = $ENABLED ]; then
    cat $ZMAP_LOG_FILE
  fi
}


# EOF