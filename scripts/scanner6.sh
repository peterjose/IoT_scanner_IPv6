# This file calls all the scanners based on the configuration in the config.sh file

#! /bin/bash

source config.sh

echo $PWD
TIME_STAMP="$(date +"%H%M%S")"
export TIME_STAMP
export OutputPath
export summaryDirPath

echo "" >> $OutputPath/config.txt
echo "********************************************************************" >> $OutputPath/config.txt 
echo TimeStamp : $TIME_STAMP >> $OutputPath/config.txt
echo "" >> $OutputPath/config.txt
cat config.sh >> $OutputPath/config.txt

echo "******************* $TIME_STAMP *******************" >> $summaryDirPath/complete.json
echo "*** Start Time : "$(date +"%d-%m-%Y:%H:%M:%S")" *****" >> $summaryDirPath/complete.json

echo Start Time : "$(date +"%d-%m-%Y:%H:%M:%S")"
startTime="$(date +%s)"

source pre_scan_scripts.sh
echo scan starts with processed hitlist present at ${processed_hitlist_path}

if [ $SCAN_XMPP_FLAG  = $ENABLED ]; then
    ./scan_xmpp.sh
fi
if [ $SCAN_MQTT_FLAG  = $ENABLED ]; then
    ./scan_mqtt.sh
fi
if [ $SCAN_AMQP_FLAG  = $ENABLED ]; then
    ./scan_amqp.sh
fi
if [ $SCAN_OPCUA_FLAG  = $ENABLED ]; then
    ./scan_opcua.sh
fi
if [ $SCAN_TELNET_FLAG  = $ENABLED ]; then
    ./scan_telnet.sh
fi
if [ $SCAN_COAP_FLAG  = $ENABLED ]; then
    ./scan_coap.sh
fi
if [ $SCAN_DTLS_FLAG  = $ENABLED ]; then
    ./scan_dtls.sh
fi
if [ $POST_SCAN_SCRIPTS  = $ENABLED ]; then
    ./post_scan_scripts.sh
fi

endTime="$(date +%s)"
deltaTime=$(expr $endTime - $startTime)
scanDays="$(expr $deltaTime / 86400)"
scanHours="$(expr $(expr $deltaTime % 86400) / 3600)"
scanMinutes="$(expr $(expr $(expr $deltaTime % 86400) % 3600) / 60)"
echo Time Elapsed : Days $scanDays, Hours $scanHours, minutes $scanMinutes  

echo End Time : "$(date +"%d-%m-%Y:%H:%M:%S")"
echo "*** End Time   : "$(date +"%d-%m-%Y:%H:%M:%S")" *****" >> $summaryDirPath/complete.json
echo Time Elapsed : Days $scanDays, Hours $scanHours, minutes $scanMinutes >> $summaryDirPath/complete.json

# EOF