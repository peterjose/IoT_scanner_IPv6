# IPv6 IoT Scans

The project is developed for scanning the internet in the IPv6 address space to find the endpoints (servers) that support IoT protocols. The scanning of the port is done using the zmap6 (https://github.com/tumi8/zmap) and application level handshake is done using a modified version of zgrab2 (https://github.com/peterjose/zgrab2).

## Scan Script
Scanning scripts are present in the folder [scripts](scripts). 

All the configuration for scanning is present in the file [config.sh](scripts/config.sh) 
In the configuration file :
* Flags have been provided to enable disable different configuration.
    * NOTE : use ENABLED (for enabling a module) and DISABLED (for disabling a module)
    * "IPV6_SCAN_FLAG" when enabled will do the zmap scan on IPv6 address space, when disabled it does the scan in ipv4 address space
    * "USE_BLOCK_IP_LIST_FLAG" when enabled it will consider the blocklist of IPs
    * "USE_SCAN_IP_LIST_FLAG" when enabled it will look into the ip list provided and does the scan only from that given list
    * "USE_DEFAULT_SCAN_IP_LIST_FLAG" when enabled will use 'DEFAULT_SCAN_LIST_FILENAME' file as the search list for all the scans
    * "IP_COUNT_LIMIT_FLAG" when enabled will set the cap limit of the number of IP addresses returned
    * "DEBUG_PRINT_FLAG" when enabled will print the scan options
    * "IGNORE_BLOCKLIST_ERROR_FLAG" when enabled will ignore the errors in parsing the blocklist

    * "SCAN_XMPP_FLAG" when enabled it checks for XMPP protocol
    * "SCAN_MQTT_FLAG" when enabled it checks for MQTT protocol
    * "SCAN_AMQP_FLAG" when enabled it checks for AMQP protocol
    * "SCAN_OPCUA_FLAG" when enabled it checks for the OPCUA protocol
    * "SCAN_COAP_FLAG" when enabled it checks for COAP protocol
    * "SCAN_TELNET_FLAG" when enabled it checks for Telnet protocol
    * "SCAN_DTLS_FLAG" when enabled it checks for DTLS protocol
    
    * Some default parameters are provided such as :
    * "DEFAULT_NUMBER_OF_IP" the scanner stops the scan when it finds these many IP address who has the port in scan as opened
    * "DEFAULT_SCAN_RATE" Set send rate in packsts/second
    * "IPv6_SOURCE_IP_ADDRESS" specify the ipv6 source address of the scanning device
    * "DEFAULT_SCAN_LIST_FILENAME" specifies the file name of the default search list
* PATH - it specifies the path of the output, zmap6, zgrab2, blocklist, iplist.
    * NOTE: the output follows the following directory structure:
        * output/< ipv4 or ipv6 >/< date >/< protocol name >/files
        * A summary of the all the scans can be obtained under output/< ipv4 or ipv6 >/summary/< protocol name >_port.json 
* PROTOCOL config section
    * This section contains the protocol related configuration information
* FUNCTIONS
    * scanner function is placed in this file, which can be called for running different scans
* Help feature is available for [config.sh](scripts/config.sh), use the command option --help or -h 

Inorder to run the scan script individually for each protocol use:
* AMQP - [scan_amqp.sh](scripts/scan_amqp.sh)
* MQTT - [scan_mqtt.sh](scripts/scan_mqtt.sh)
* OPCUA - [scan_opcua.sh](scripts/scan_opcua.sh)
* XMPP - [scan_xmpp.sh](scripts/scan_xmpp.sh)
* CoAP - [scan_coap.sh](scripts/scan_coap.sh)
* Telnet - [scan_telnet.sh](scripts/scan_telnet.sh) 
* DTLS - [scan_dtls.sh](scripts/scan_dtls.sh) 

Inorder to run the script for all the protocols use the script [scanner6.sh](scripts/scanner6.sh)

Inorder to filter hitlist based on blocklist and creating a search list use the script [filter_hitlist.py](scripts/filter_hitlist.py). Script is designed to work with python 3.
* The file provide following Flags:
    * "LIMIT_THE_IP_FILTERED_FLAG" when enabled will stop the filtering when it has found the MAX_NUMBER_OF_IP for the search list
    * "ENABLE_DEBUG_PRINT_FLAG" when enabled will print the debug statments
    * "ALLOW_INSTALLATION_OF_MISSING_PKGS_FLAG" when enabled will install if it finds a package needed for the script is missing
    * "MAX_NUMBER_OF_IP" specify the number of IPs to be filtered for Search list, this will be only considered if LIMIT_THE_IP_FILTERED_FLAG is set to True
    * Specify the file paths of the files in the next section

NEW advanced filter has been developed. The filter is designed to filter based on BLOCKLIST, Alias Prefixes and Country Code in any combination and to shuffle the IP list [pre_scan_multistage_ip_filter_v2.py](scripts/pre_scan_multistage_ip_filter_v2.py). Script is designed to work with python 3.
* The file provide following Config Flags:
    * "ALLOW_INSTALLATION_OF_MISSING_PKGS_FLAG" when enabled will install if it finds a package needed for the script is missing
    * "BLOCKLISTED_COUNTRY_CODES_LIST" list of country codes that will be filtered out by default, this can be override by providing the country code via the argument option '-c'
    * "DEFAULT_OUTPUT_DIRECTORY_NAME" This is the directory file name were the final filtered IP list will be placed. If the directory is not present then it creates one.
    * "DEFAULT_FILE_POSTPIX_FILTERING_SUMMARY" the default post fix of the file where filtering summary log will be kept.
* The file provide following Internal Flags:
    * "DEFAULT_NUMBER_OF_IP_PER_SPLIT" The script is designed based on Multiprocessing to make the entire process faster. The input file will be split internally, each having the the number of given here.
    * "NUMBER_OF_PROCESSES" The number of multiprocessing processes created in parallel for the operation. Change this if speed or memory issues are observed
* The script provides the following set of arguments
    ```
    usage: pre_scan_multistage_ip_filter_v2.py [-h] -i IP_LIST_FILE [-a AS_FILE] [-o AS_ORG_FILE] [-c [COUNTRY_CODE [COUNTRY_CODE ...]]] [-b BLOCKLIST_FILE] [-A APD_FILE] [-N NON_APD_FILE]

    optional arguments:
    -h, --help            show this help message and exit
    -i IP_LIST_FILE, --ip-list-file IP_LIST_FILE
                            Raw IP list that has to be filtered

    Country based filtering:
    NOTE: Provide files obtained from: 
        1. https://publicdata.caida.org/datasets/routing/
        2. https://publicdata.caida.org/datasets/as-organizations/
    -a AS_FILE, --as-file AS_FILE
                            File containing AS info
    -o AS_ORG_FILE, --as-org-file AS_ORG_FILE
                            JSON file containing AS-organization-country info
    -c [COUNTRY_CODE [COUNTRY_CODE ...]], --country-code [COUNTRY_CODE [COUNTRY_CODE ...]]
                            country code or codes to be blocklisted

    Blocklist based filtering:
    -b BLOCKLIST_FILE, --blocklist-file BLOCKLIST_FILE
                            File containing Blocklist ips

    Alias prefix detection based filtering:
    NOTE: Latest APD and NON APD file can be obtained from:
        1. https://ipv6hitlist.github.io/
    -A APD_FILE, --apd-file APD_FILE
                            File containing apd prefixes
    -N NON_APD_FILE, --non-apd-file NON_APD_FILE
                            File containing non-apd prefixes
    ```
    Example
    ```
    python3 pre_scan_multistage_ip_filter_v2.py -i 2022-03-15-input.txt -a routeviews-rv6-20220325-1200.pfx2as -o 20220101.as-org2info.jsonl -c RU UA US -b ipv6-bl-merged.txt -A 2022-03-15-aliased.txt -N 2022-03-15-nonaliased.txt
    ```

Post Scan scripts
* Post scan shell script where invocation of other post scan python scripts can be done is in a automated pipeline is [post_scan_scripts.sh](scripts/post_scan_scripts.sh)
* Post scan python script that can be used to plot the graphs is [post_scan_scripts_plot.py](scripts/post_scan_scripts_plot.py). Usage methods can be obtained from using help option '-h'.
    * The script provides the following set of arguments
        ```
        usage: post_scan_scripts_plot.py [-h] [--create-data-frame] [-c CATEGORIZED_AS_FILE_NAME] [-O SCAN_OUTPUT_DIR_PATH [SCAN_OUTPUT_DIR_PATH ...] | -d PROCESSED_DATA_FRAME]

        optional arguments:
        -h, --help            show this help message and exit
        --create-data-frame   When enabled it creates a data frame
        -c CATEGORIZED_AS_FILE_NAME, --categorized-as-file-name CATEGORIZED_AS_FILE_NAME
                                The latest categorized ases csv filename eg: 2022-05_categorized_ases.csv, that will be downloaded from https://asdb.stanford.edu/data/
        -O SCAN_OUTPUT_DIR_PATH [SCAN_OUTPUT_DIR_PATH ...], --scan-output-dir-path SCAN_OUTPUT_DIR_PATH [SCAN_OUTPUT_DIR_PATH ...]
                                Directory path/ list of directories paths, where the output files are stored
        -d PROCESSED_DATA_FRAME, --processed-data-frame PROCESSED_DATA_FRAME
                                Directory path/ list of directories paths, where the output files are stored
        ```
        Example
        ```
        python3 post_scan_scripts_plot.py -d DataFrame.json
        ```
* Post scan python script that can be used process the raw data and store as dataframe and also to generate the webpage (dashboard) content is [post_scan_script_and_webpage_data_generator.py](scripts/post_scan_script_and_webpage_data_generator.py)
    * The script provides the following set of arguments
        ```
            usage: post_scan_script_and_webpage_data_generator.py [-h] [--create-data-frame] [-c CATEGORIZED_AS_FILE_NAME] [-O SCAN_OUTPUT_DIR_PATH [SCAN_OUTPUT_DIR_PATH ...] | -d
                                                                PROCESSED_DATA_FRAME]

            optional arguments:
            -h, --help            show this help message and exit
            --create-data-frame   When enabled it creates a data frame
            -c CATEGORIZED_AS_FILE_NAME, --categorized-as-file-name CATEGORIZED_AS_FILE_NAME
                                    The latest categorized ases csv filename eg: 2022-05_categorized_ases.csv, that will be downloaded from https://asdb.stanford.edu/data/
            -O SCAN_OUTPUT_DIR_PATH [SCAN_OUTPUT_DIR_PATH ...], --scan-output-dir-path SCAN_OUTPUT_DIR_PATH [SCAN_OUTPUT_DIR_PATH ...]
                                    Directory path/ list of directories paths, where the output files are stored
            -d PROCESSED_DATA_FRAME, --processed-data-frame PROCESSED_DATA_FRAME
                                    Directory path/ list of directories paths, where the output files are stored
        ```
        Example
        ```
        python3 post_scan_script_and_webpage_data_generator.py -d DataFrame.json
        ```

Zgrab2
* Open source Application-Layer Network Scanner with new modules for IoT protocols: [zgrab2](zgrab2)

Dashboard
* Dashboard code is found in [webpage](webpage)
