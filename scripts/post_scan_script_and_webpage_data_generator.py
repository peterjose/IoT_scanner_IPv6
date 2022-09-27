"""@file post_scan_script_and_webpage_data_generator.py

 Post processing script to generate webpage content
 @author Peter Jose

"""

# TODO: Fix Bug in reading encoded data from the file

import ipaddress
from re import TEMPLATE
import subprocess
import pkg_resources
import argparse
import sys
import json
import pathlib
import time
import datetime
from datetime import date
import filter_package


############################# CONFIG START #############################

# Enable the flag to print the debug statments
ENABLE_DEBUG_PRINT_FLAG = True                   # True / False

# Flag to allow the installation of missing packages
ALLOW_INSTALLATION_OF_MISSING_PKGS_FLAG = True  # True / False

# File name format
ZGRAB_OUT_FILE = "_zgrab2_out.json"
ZGRAB_META_FILE = "_scan_summary.json"

AS_LINK= 'https://publicdata.caida.org/datasets/routing/routeviews6-prefix2as/'
AS_LOG_FILE='pfx2as-creation.log'
AS_DIR_NAME='AS_list'

ZGRAB_OUT_FILE_KEY = "zgrab2_out_file_path"
AS_FILE_PATH_KEY = "AS_file_path"
AS_COUNTRY_FILE_PATH_KEY = "AS_Country_file_path"
AS_CATEGORY_FILE_PATH_KEY = "AS_Category_file_path"

PROTOCOL_KEY = "protocol"
PORT_KEY = "port"
SCAN_CODE_KEY = "scan_code"

HITRATE_KEY = 'hitrate'

PROCESSED_DATA_DIR_NAME = "processed_data"

DATA_FRAME_FILENAME = "DataFrame.json"

MAX_MIND_DATABASE_USAGE_DATE = '11-06-2022'
MAX_MIND_GEOLITE2_DB_COMMON_PREFIX = 'GeoLite2-' 
MAX_MIND_GEOLITE2_ASN_DB_NAME = 'GeoLite2-ASN'
MAX_MIND_GEOLITE2_COUNTRY_DB_NAME = 'GeoLite2-Country'

MAX_MIND_LICENSE_KEY = 'YOUR_LICENSE_KEY'

MAX_MIND_AS_DB_LINK = 'https://download.maxmind.com/app/geoip_download?edition_id=GeoLite2-ASN&license_key='+MAX_MIND_LICENSE_KEY+'&suffix=tar.gz'
MAX_MIND_COUNTRY_DB_LINK = 'https://download.maxmind.com/app/geoip_download?edition_id=GeoLite2-Country&license_key='+MAX_MIND_LICENSE_KEY+'&suffix=tar.gz'

STANFORD_ASDB_DATASET_LINK = "https://asdb.stanford.edu/data/"
STANFORD_ASDB_DATASET_FILE_NAME_SUFFIX = "_categorized_ases.csv"

WEBPAGE_CONTENT_PLOT_DISCOVERY_DATA = "../webpage/plot-discovery.json"
WEBPAGE_CONTENT_PLOT_MAP_DATA = "../webpage/plot-maps.json"

TEMPLATE_PLOT_DISCOVERY_DATA_FILE_NAME = 'webpage_elements/template_plot.json'
TEMPLATE_MAP_DATA_FILE_NAME = 'webpage_elements/template_country.json'

############################## CONFIG END ##############################

# Check if the required modules for the script are installed
required = {'pysubnettree','pandas','matplotlib','seaborn','certifi','ipaddress','upsetplot','geopandas','pycountry','geoip2'}
installed = {pkg.key for pkg in pkg_resources.working_set}
missing = required - installed
if len(missing) != 0:
    print("Following packages are missing : ", missing)
    if ALLOW_INSTALLATION_OF_MISSING_PKGS_FLAG:
        print ("Installing the missing packages")
        python = sys.executable
        subprocess.check_call([python, '-m', 'pip', 'install', *missing], stdout=subprocess.DEVNULL)
    else:
        exit()

import SubnetTree
import ipaddress
import pandas as pd
import numpy as np
import geoip2.database


def data_scraper(item):
    """
    Function to extract the data from log files
    Args:
        item: item contrains the all the information regarding one protocol
    
    Returns:
        Nil
    
    Raises:
        Nil
    """
    # print(item)
    processed_data = []
    zgrab2_outFile_name = item[ZGRAB_OUT_FILE_KEY]
    AS_file_name = item[AS_FILE_PATH_KEY]
    protocol = item[PROTOCOL_KEY]
    port = item[PORT_KEY]
    UseMaxMindDB_Flag = True
    try:
        if(datetime.strptime(item[SCAN_CODE_KEY], '%d-%m-%Y').date() < datetime.strptime(MAX_MIND_DATABASE_USAGE_DATE, '%d-%m-%Y').date()):
            UseMaxMindDB_Flag = False
    except Exception as e: 
        print("Error", repr(e))
    if not UseMaxMindDB_Flag:
        as_country_df = pd.DataFrame.from_dict(filter_package.create_AS_country_dict_fn(item[AS_COUNTRY_FILE_PATH_KEY]),orient='index').reset_index()
        # subnet tree with AS info
        tree = SubnetTree.SubnetTree()
        if AS_file_name:
            with open(AS_file_name, 'r') as AS_file:
                try:
                    for line in AS_file:
                        AS_data = line.strip().split("\t")
                        try:
                            tree[AS_data[0]+"/"+AS_data[1]] = AS_data[2]
                        except ValueError as e:
                            print("Skipped line '" + line)            
                except Exception as e:
                    print("Error", repr(e))
    else:
        try:
            ip_as_reader = geoip2.database.Reader(item[AS_FILE_PATH_KEY])
            ip_as_country_reader = geoip2.database.Reader(item[AS_COUNTRY_FILE_PATH_KEY])
        except Exception as e:
            pass
            print("Error", repr(e))

    ip_list = []
    try:
        as_category_info_df = pd.read_csv(item[AS_CATEGORY_FILE_PATH_KEY])
        as_category_info_df = as_category_info_df[as_category_info_df.columns[0:5]]
        as_category_info_df.fillna('-', inplace=True)
        as_category_info_df = as_category_info_df.assign(ASN = lambda x: (x['ASN'].replace('AS','',regex=True)))
    except Exception as e:
        pass
        print("Error", repr(e))
    with open(zgrab2_outFile_name,'r') as zgrab2_outFile:
        for line in zgrab2_outFile:
            zgrab_data = json.loads(line)
            # check if the ip is already processed
            if not zgrab_data["ip"] in ip_list:
                ip_list.append(zgrab_data["ip"])
                AS_info ='unknown'
                tls,status,error,response, = "0",'-','-','-'
                ipv6_64, country ='-','-'
                clientTLS,serverTLS = '-','-'
                certi_Issuer_commonName,certi_Issuer_country,certi_Issuer_org,validityStart,validityEnd = '-','-','-','-','-'
                self_signed,browser_trusted,browser_error,scan_time,certificate_raw,fingerprint_sha256='-','-','-','-','-','-',
                asn_category1L1,asn_category1L2,asn_category2L1,asn_category2L2='-','-','-','-'
                signature_algorithm,length,server_cipher_suite='-','-','-'
                try:
                    ipv6_64 = ipaddress.ip_address(zgrab_data["ip"].strip()).exploded.upper()[0:19]
                except Exception as e:
                    # print("Error:",protocol, repr(e),line)
                    pass
                if not UseMaxMindDB_Flag:
                    try:
                        AS_info = tree[zgrab_data["ip"]].replace(","," ")
                    except Exception as e:
                        # print("Error:",protocol, repr(e),line)
                        pass
                    try:
                        AS_list = AS_info.split(" ")
                        country_names = ""
                        for AS in AS_list:
                            country_list = as_country_df[as_country_df["asn"] ==AS]["country"].tolist()
                            for country_name in country_list:
                                country_names = country_names + str(country_name) + " "
                        if country_names != "":
                            country = country_name
                    except Exception as e:
                        print("Error: country",protocol, repr(e),line)
                        pass
                else:
                    try:
                        AS_info = str(ip_as_reader.asn(zgrab_data["ip"]).autonomous_system_number)
                    except Exception as e:
                        print("AS info:",protocol, repr(e),line)
                        pass
                    try:
                        country = ip_as_country_reader.country(zgrab_data["ip"]).country.iso_code
                    except Exception as e:
                        print("AS info:",protocol, repr(e),line)
                        pass
                try:
                    row = as_category_info_df[as_category_info_df['ASN'] == AS_info]
                    if not row.empty:
                        asn_category1L1 = row.iloc[0]['Category 1 - Layer 1']
                        asn_category1L2 = row.iloc[0]['Category 1 - Layer 2']
                        asn_category2L1 = row.iloc[0]['Category 2 - Layer 1']
                        asn_category2L2 = row.iloc[0]['Category 2 - Layer 2']
                except Exception as e:
                    print(row)
                    print("AS category:",protocol, repr(e),line)
                    pass
                try:
                    status = zgrab_data["data"][protocol]["status"]
                    if not zgrab_data["data"][protocol]["status"] == "success":
                        error = zgrab_data["data"][protocol]["error"]
                        if zgrab_data["data"][protocol]["status"] == "protocol-error":
                            try:
                                pass
                                response = zgrab_data["data"][protocol]["result"][protocol].split(' ')[0]
                            except Exception as e:
                                response = 'can\'t parse'
                                # print("Error:", protocol, repr(e),line)
                                pass
                    else:
                        try:
                            for key_ in zgrab_data["data"][protocol]["result"]:
                                if key_ == protocol:
                                    response = zgrab_data["data"][protocol]["result"][protocol].strip('\n')
                                elif key_ == "tls":
                                    tls = "1"
                                    tls_pointer = zgrab_data["data"][protocol]["result"]["tls"]
                                    clientTLS = tls_pointer["handshake_log"]["client_hello"]["version"]["name"]
                                    if "supported_versions" in tls_pointer["handshake_log"]["server_hello"].keys():
                                        serverTLS = tls_pointer["handshake_log"]["server_hello"]["supported_versions"]["selected_version"]["name"]
                                    else :
                                        serverTLS = tls_pointer["handshake_log"]["server_hello"]["version"]["name"]
                                    if "cipher_suite" in tls_pointer["handshake_log"]["server_hello"].keys():
                                        server_cipher_suite = tls_pointer["handshake_log"]["server_hello"]["cipher_suite"]["name"]
                                    certificate_raw = tls_pointer["handshake_log"]["server_certificates"]["certificate"]["raw"]
                                    fingerprint_sha256 = tls_pointer["handshake_log"]["server_certificates"]["certificate"]["parsed"]["subject_key_info"]["fingerprint_sha256"]
                                    if "rsa_public_key" in tls_pointer["handshake_log"]["server_certificates"]["certificate"]["parsed"]["subject_key_info"].keys():
                                        length = tls_pointer["handshake_log"]["server_certificates"]["certificate"]["parsed"]["subject_key_info"]["rsa_public_key"]["length"]
                                    elif "ecdsa_public_key" in tls_pointer["handshake_log"]["server_certificates"]["certificate"]["parsed"]["subject_key_info"].keys():
                                        length = tls_pointer["handshake_log"]["server_certificates"]["certificate"]["parsed"]["subject_key_info"]["ecdsa_public_key"]["length"]
                                    if "self_signed" in  tls_pointer["handshake_log"]["server_certificates"]["certificate"]["parsed"]["signature"].keys():
                                        if tls_pointer["handshake_log"]["server_certificates"]["certificate"]["parsed"]["signature"]["self_signed"] == True:
                                            self_signed = 'Self-signed'
                                        else:
                                            self_signed = 'Not self-signed'
                                    if "common_name" in tls_pointer["handshake_log"]["server_certificates"]["certificate"]["parsed"]["issuer"].keys():
                                        certi_Issuer_commonName = tls_pointer["handshake_log"]["server_certificates"]["certificate"]["parsed"]["issuer"]["common_name"]
                                    if "country" in tls_pointer["handshake_log"]["server_certificates"]["certificate"]["parsed"]["issuer"].keys():
                                        certi_Issuer_country = tls_pointer["handshake_log"]["server_certificates"]["certificate"]["parsed"]["issuer"]["country"]
                                    if "organization" in tls_pointer["handshake_log"]["server_certificates"]["certificate"]["parsed"]["issuer"].keys():
                                        certi_Issuer_org = tls_pointer["handshake_log"]["server_certificates"]["certificate"]["parsed"]["issuer"]["organization"]
                                    if "signature_algorithm" in tls_pointer["handshake_log"]["server_certificates"]["certificate"]["parsed"].keys():
                                        signature_algorithm = tls_pointer["handshake_log"]["server_certificates"]["certificate"]["parsed"]["signature_algorithm"]["name"]
                                    validityStart = tls_pointer["handshake_log"]["server_certificates"]["certificate"]["parsed"]["validity"]["start"]
                                    validityEnd = tls_pointer["handshake_log"]["server_certificates"]["certificate"]["parsed"]["validity"]["end"]
                                    if tls_pointer["handshake_log"]["server_certificates"]["validation"]["browser_trusted"] == True:
                                            browser_trusted =  'Browser Trusted'
                                    else:
                                        browser_trusted =  'Browser Not Trusted'
                                    if "browser_error" in tls_pointer["handshake_log"]["server_certificates"]["validation"].keys():
                                        browser_error = tls_pointer["handshake_log"]["server_certificates"]["validation"]["browser_error"]
                                scan_time = zgrab_data["data"][protocol]["timestamp"]
                        except Exception as e:
                            print("Error 3",protocol, repr(e),line)
                            pass
                    outputDict = {"ip":zgrab_data["ip"],"port":port,"AS":AS_info,"protocol":protocol,"ipv6_64":ipv6_64,"country":country,
                                    "status":status,"error":error,"response":response,"tls":tls,"clientTLS":clientTLS,
                                    "serverTLS":serverTLS,"certificate_raw":certificate_raw, "fingerprint_sha256":fingerprint_sha256,"length":length,"server_cipher_suite":server_cipher_suite,
                                    "certi_Issuer_commonName":certi_Issuer_commonName,"certi_Issuer_country":certi_Issuer_country,
                                    "certi_Issuer_org":certi_Issuer_org,"self_signed":self_signed,"signature_algorithm":signature_algorithm,
                                    "validityStart":validityStart,"validityEnd":validityEnd, "browser_trusted":browser_trusted,
                                    "browser_error":browser_error,"scan_time":scan_time,"scan_code":item[SCAN_CODE_KEY],
                                    "asn_category1L1":asn_category1L1,"asn_category1L2":asn_category1L2,"asn_category2L1":asn_category2L1,
                                    "asn_category2L2":asn_category2L2}
                    processed_data.append(outputDict)
                except Exception as err:
                    print("Error:",protocol, err,line)
    if UseMaxMindDB_Flag:
        ip_as_reader.close()
        ip_as_country_reader.close()
    return processed_data


def create_process_list(scan_path,categorized_as_file_name):
    """Function to create the list of files to be used for further processing
    Args:
        scan_path: scan path input to the program
        categorized_as_file_name: file name of DB for categorizing AS
    
    Returns:
        Nil
    
    Raises:
        Nil
    """
    process_list= []

    # create AS directory
    AS_path = pathlib.Path(scan_path,AS_DIR_NAME)
    if not pathlib.Path.is_dir(AS_path):
        pathlib.Path.mkdir(AS_path)
    
    # download the AS log file 
    # try:
    #     with urllib.request.urlopen(AS_LINK +AS_LOG_FILE, cafile=certifi.where()) as response:
    #         html = response.read()
    #         AS_log_file_list = bytes.decode(html).split('\n')
    # except Exception as e:
    #     print("Error", repr(e))
    #     AS_log_file_list = []

    AS_log_file_list = []

    # get all the zgrab output files
    scan_output_list = ([x.name for x in scan_path.iterdir() if x.is_dir()])
    if "coap" in scan_output_list:
        pass
        # scan_output_list.remove("coap")
    
    for folder in scan_output_list:
        zgrab2_output_files_list = ([x for x in pathlib.Path(scan_path,folder).iterdir() if (x.is_file() and x.name.find(ZGRAB_META_FILE) != -1)])
        for zgrab2_summary_file in zgrab2_output_files_list:
            fileName = zgrab2_summary_file.name
            fileDetails = fileName.split('_')
            protocol = fileDetails[0]
            port = fileDetails[2]
            zgrab_summary = open(zgrab2_summary_file,'r').read()
            zgrab2_out_file = str(zgrab2_summary_file).replace(ZGRAB_META_FILE,ZGRAB_OUT_FILE)
            scanDate = date.today()
            AS_file_path_final = ''
            AS_country_file_path = ''
            AS_Category_file_path = ''
            ##### AS and AS Country
            try:
                scanDate = datetime.strptime(scan_path.name, '%d-%m-%Y').date()
            except Exception as e: 
                print("Error", repr(e))
            if(scanDate < datetime.strptime(MAX_MIND_DATABASE_USAGE_DATE, '%d-%m-%Y').date()):
                try:
                    date_ = ((json.loads(zgrab_summary))["start"].split('T')[0]).replace('-','')
                    # Get AS files
                    AS_already_downloaded_files_list = ([x for x in AS_path.iterdir() if (x.is_file() and x.name.find(date_) != -1 and x.suffix != '.gz')])
                    if not len(AS_already_downloaded_files_list):
                        for AS_log_file in AS_log_file_list:
                            if AS_log_file.find(date_) != -1: 

                                AS_file_relative_path=AS_log_file.split("\t")[-1]
                                AS_file_path=AS_file_relative_path.split('/')[-1]
                                AS_file_gz = pathlib.Path(AS_path,AS_file_path)
                                subprocess.call(["wget "+AS_LINK+AS_file_relative_path+" -P "+str(AS_path) +" --no-check-certificate && gunzip "+ str(AS_file_gz)],shell=True, timeout=60)
                                # urllib.request.urlretrieve(AS_LINK+AS_file_relative_path,AS_file_gz)
                                # with gzip.open(AS_file_gz, 'rb') as f_in:
                                #     AS_file_path_final = pathlib.Path(AS_path,AS_file_path.replace('.gz',''))
                                #     with open(AS_file_path_final, 'wb') as f_out:
                                #         shutil.copyfileobj(f_in, f_out)
                                #     AS_file_path_final = str(AS_file_path_final)
                                break
                    else:
                        AS_file_path_final = str(AS_already_downloaded_files_list[0])
                    AS_already_downloaded_files_list = ([x for x in AS_path.iterdir() if (x.is_file() and x.name.find('as-org2info') != -1 and x.suffix != '.gz')])
                    if len(AS_already_downloaded_files_list):
                        AS_country_file_path = str(AS_already_downloaded_files_list[0].resolve())
                except Exception as e: 
                    print("Error", repr(e))
            else:
                try:
                    AS_already_downloaded_files_list = ([x for x in AS_path.iterdir() if (x.is_dir() and x.name.find(MAX_MIND_GEOLITE2_DB_COMMON_PREFIX) != -1)])
                    if not len(AS_already_downloaded_files_list):
                        subprocess.call(["cd "+str(AS_path.resolve())+";"+
                                "wget \""+MAX_MIND_AS_DB_LINK+"\" -O "+MAX_MIND_GEOLITE2_ASN_DB_NAME+".tar.gz --no-check-certificate && tar -xvzf "+
                                            MAX_MIND_GEOLITE2_ASN_DB_NAME+".tar.gz"],shell=True, timeout=60)
                        subprocess.call(["cd "+str(AS_path.resolve())+";"+
                                "wget \""+MAX_MIND_COUNTRY_DB_LINK+"\" -O "+MAX_MIND_GEOLITE2_COUNTRY_DB_NAME+".tar.gz --no-check-certificate && tar -xvzf "+
                                            MAX_MIND_GEOLITE2_COUNTRY_DB_NAME+".tar.gz"],shell=True, timeout=60)
                        AS_already_downloaded_files_list = ([x for x in AS_path.iterdir() if (x.is_dir() and x.name.find(MAX_MIND_GEOLITE2_DB_COMMON_PREFIX) != -1)])
                    for GeoLite_file in AS_already_downloaded_files_list:
                        if GeoLite_file.name.find(MAX_MIND_GEOLITE2_ASN_DB_NAME) != -1:
                            AS_file_path_final = str(pathlib.Path(GeoLite_file,MAX_MIND_GEOLITE2_ASN_DB_NAME+'.mmdb').resolve())
                        elif GeoLite_file.name.find(MAX_MIND_GEOLITE2_COUNTRY_DB_NAME) != -1:
                            AS_country_file_path = str(pathlib.Path(GeoLite_file,MAX_MIND_GEOLITE2_COUNTRY_DB_NAME+'.mmdb').resolve())
                except Exception as e: 
                    print("Error", repr(e))

            ##### AS Category

            try:
                categorized_as_files_list = ([x for x in AS_path.iterdir() if (x.is_file() and x.name.find(STANFORD_ASDB_DATASET_FILE_NAME_SUFFIX) != -1 and x.suffix == '.csv')])
                if not len(categorized_as_files_list):
                    if len(categorized_as_file_name):
                        subprocess.call(["cd "+str(AS_path.resolve())+";"+
                            "wget "+STANFORD_ASDB_DATASET_LINK+categorized_as_file_name+" --no-check-certificate"],shell=True, timeout=180)
                        categorized_as_files_list = ([x for x in AS_path.iterdir() if (x.is_file() and x.name.find(STANFORD_ASDB_DATASET_FILE_NAME_SUFFIX) != -1 and x.suffix == '.csv')])
                AS_Category_file_path = str(categorized_as_files_list[0].resolve())
            except Exception as e: 
                print("Error", repr(e))
            process_list.append({ZGRAB_OUT_FILE_KEY:zgrab2_out_file,AS_FILE_PATH_KEY:AS_file_path_final,
                        PROTOCOL_KEY:protocol,PORT_KEY:port,SCAN_CODE_KEY:scan_path.name,AS_COUNTRY_FILE_PATH_KEY:AS_country_file_path,
                        AS_CATEGORY_FILE_PATH_KEY:AS_Category_file_path})
    return process_list



def generate_webpage_discovery_data(dataFrame):
    """Function to generate data for discovery plot for webpage
    Args:
        dataFrame: data frame of the latest scan
    
    Returns:
        Nil
    
    Raises:
        Nil
    """
    try:
        data = []
        try:
            # try to read already populated data else use empty template
            with open(WEBPAGE_CONTENT_PLOT_DISCOVERY_DATA, 'r') as file:
                data = json.load(file)
        except Exception as e:    
            with open(TEMPLATE_PLOT_DISCOVERY_DATA_FILE_NAME, 'r') as file:
                data = json.load(file)
        scan_code_list = dataFrame.sort_values(by = ['scan_code'],ascending=True)['scan_code'].unique().tolist()
        for scan_code in scan_code_list:
            scan_time_epoch = int(datetime.datetime.combine(scan_code,datetime.time(0,0,0),tzinfo=datetime.timezone.utc).timestamp()*1000)
            df = dataFrame[dataFrame['scan_code'] == scan_code]
            protocol_port_list = df.sort_values(by = ['Protocol, Port'],ascending=True)['Protocol, Port'].unique().tolist()
            protocol_port_list = protocol_port_list
            df = df[(df['status'] == 'success')]
            df = df.groupby(['Protocol, Port'],sort=False).count().fillna(0).reset_index()[['Protocol, Port','ip']]
            df = df.rename(columns={'ip':'count'})
            for protocol_port in protocol_port_list:
                count = 0
                count_list = df[(df['Protocol, Port'] == protocol_port)]['count'].to_list()
                if len(count_list):
                    count = count_list[0]
                found_flag = False
                for index in range(len(data['series'])):
                    if protocol_port in data['series'][index]['name']:
                        data['series'][index]['data'].append([scan_time_epoch,count])
                        found_flag = True
                        break
                if not found_flag:
                    data['series'].append({"name": protocol_port,"data": [[scan_time_epoch,count]]})
        with open(WEBPAGE_CONTENT_PLOT_DISCOVERY_DATA, 'w') as f:
            json.dump(data, f)

    except Exception as e: 
        print("Error", repr(e))


def generate_webpage_location_data(dataFrame):
    """Function generate webpage location data
    Args:
        dataFrame: data frame of the latest scan 
    
    Returns:
        Nil
    
    Raises:
        Nil
    """
    try:
        scan_code = dataFrame.sort_values(by = ['scan_code'],ascending=True)['scan_code'].unique().tolist()[-1]
        dataFrame = dataFrame[dataFrame['scan_code'] == scan_code]
        dataFrame = dataFrame[(dataFrame['status'] == 'success') & (dataFrame['country'] != '-')]
        dataFrame['country'] = dataFrame['country'].str.lower()
        dataFrame = dataFrame.groupby(['country'],sort=False).count().fillna(0).reset_index()[['country','ip']]
        dataFrame['ip'] = round(dataFrame['ip']/float(dataFrame['ip'].sum())*100,3)
        dataFrame = dataFrame.rename(columns={'ip':'percentage'})
        df = pd.read_json(TEMPLATE_MAP_DATA_FILE_NAME)
        df.columns = ['country','percentage']
        df['percentage'] = 0
        df = df.set_index('country').add(dataFrame.set_index('country'), fill_value=0)
        df = df.reset_index(level=None)
        df.to_json(WEBPAGE_CONTENT_PLOT_MAP_DATA, orient="values")  # hard coded path to be removed
    except Exception as e: 
        print("Error", repr(e))


def create_dashboard_data(dataFrame,processed_data_dir):
    generate_webpage_discovery_data(dataFrame)
    generate_webpage_location_data(dataFrame)


def main():
    """Main Function
    
    """
    parser = argparse.ArgumentParser()
    parser.add_argument('--create-data-frame', action='store_true',help="When enabled it creates a data frame")
    parser.set_defaults(create_data_frame=False)
    parser.add_argument("-c", "--categorized-as-file-name", type=str, help="The latest categorized ases csv filename eg: 2022-05_categorized_ases.csv, that will be downloaded from https://asdb.stanford.edu/data/",
                            default='2022-05_categorized_ases.csv')
    group = parser.add_mutually_exclusive_group()
    group.add_argument("-O", "--scan-output-dir-path", nargs='+', type=pathlib.Path, help="Directory path/ list of directories paths, where the output files are stored")
    group.add_argument("-d", "--processed-data-frame", type=str, help="Directory path/ list of directories paths, where the output files are stored")
    args = parser.parse_args()

    dataFrame = pd.DataFrame()

    flag_O_arg = False
    flag_d_arg = False

    if (getattr(args,'scan_output_dir_path') is not None):
        # TODO: validate the directories
        flag_O_arg = True
        outputPath = args.scan_output_dir_path[0].parents[0]

    if (getattr(args,'processed_data_frame') is not None):
        # TODO: validate the directories
        flag_d_arg = True
        if not flag_O_arg:
            outputPath = pathlib.Path(args.processed_data_frame).parents[0]

    if not (flag_d_arg | flag_O_arg):
        print("arguments missing")
        parser.print_help()
        exit()

    # create processed data directory
    processed_data_dir = pathlib.Path(outputPath,PROCESSED_DATA_DIR_NAME)
    if not pathlib.Path.is_dir(processed_data_dir):
        pathlib.Path.mkdir(processed_data_dir)
    # create time based sub directory
    processed_data_dir = pathlib.Path(processed_data_dir,str(int(time.time())))
    if not pathlib.Path.is_dir(processed_data_dir):
        pathlib.Path.mkdir(processed_data_dir)

    if (getattr(args,'scan_output_dir_path') is not None):
        process_list = []
        # create data frame of the data received
        for arg in args.scan_output_dir_path:
            process_list += create_process_list(arg, args.categorized_as_file_name)
        
        if args.create_data_frame:
            i = 0
            for item in process_list:
                i = i+1
                df = pd.DataFrame(data_scraper(item))
                #dataFrame = dataFrame.append(df, ignore_index=True)
                df.to_json(str(processed_data_dir.resolve())+"/"+str(i)+"dataFrame.json", orient='records', lines=True)
            subprocess.call(["cd "+str(processed_data_dir.resolve())+";"+
                "cat *dataFrame.json >> "+str(processed_data_dir.resolve())+"/"+DATA_FRAME_FILENAME+ ";" +
                "rm *dataFrame.json"], stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                shell=True)
            print("Created Data Frame at" + str(processed_data_dir.resolve())+"/"+DATA_FRAME_FILENAME)
            return
        
        # dataFrame = pd.DataFrame()
        for item in process_list:
            df = pd.DataFrame(data_scraper(item))
            dataFrame = dataFrame.append(df, ignore_index=True)

        try:
            dataFrame['protocol'] = dataFrame['protocol'].apply(lambda x: x.upper())
            dataFrame.loc[dataFrame['port'].astype(str) == '8883', 'protocol'] = 'MQTTS'
            dataFrame.loc[dataFrame['port'].astype(str) == '5684', 'protocol'] = 'COAPS'
            dataFrame.loc[dataFrame['port'].astype(str) == '5683', 'protocol'] = 'CoAP'
            dataFrame.loc[dataFrame['port'].astype(str) == '23', 'protocol'] = 'Telnet'
            dataFrame.loc[dataFrame['port'].astype(str) == '4840', 'protocol'] = 'OPC UA'
            dataFrame.loc[dataFrame['port'].astype(str) == '4843', 'protocol'] = 'OPC UA'
            # dataFrame['protocol'] = dataFrame['protocol'].apply(lambda x: 'MQTTS' if x == 'mqtt' else x.upper())
        except Exception as e: 
            pass
            # print("Error", repr(e))

    if (getattr(args,'processed_data_frame') is not None):
        dataFrame = pd.read_json(args.processed_data_frame, lines=True)
        dataFrame['scan_code'] = pd.to_datetime(dataFrame['scan_code'], format='%d-%m-%Y').dt.date
        outputPath = pathlib.Path(args.processed_data_frame).parents[0]
        try:
            dataFrame['protocol'] = dataFrame['protocol'].apply(lambda x: x.upper())
            dataFrame.loc[dataFrame['port'].astype(str) == '8883', 'protocol'] = 'MQTTS'
            dataFrame.loc[dataFrame['port'].astype(str) == '5684', 'protocol'] = 'CoAPs'
            dataFrame.loc[dataFrame['port'].astype(str) == '5683', 'protocol'] = 'CoAP'
            dataFrame.loc[dataFrame['port'].astype(str) == '23', 'protocol'] = 'Telnet'
            dataFrame.loc[dataFrame['port'].astype(str) == '4840', 'protocol'] = 'OPC UA'
            dataFrame.loc[dataFrame['port'].astype(str) == '4843', 'protocol'] = 'OPC UA'
            # dataFrame['protocol'] = dataFrame['protocol'].apply(lambda x: 'MQTTS' if x == 'mqtt' else x.upper())
        except Exception as e: 
            pass
            print("Error", repr(e))

    # Write processing info        
    with open(str(processed_data_dir.resolve())+"/ProcessedFileInfo.txt",'w') as processed_file_info:
        processed_file_info.write(str(time.asctime(time.localtime(time.time())))+"\n")
        if flag_O_arg:
            json.dump(process_list, processed_file_info)
        if flag_d_arg:
            processed_file_info.write(args.processed_data_frame+"\n")
    
    print("Output stored at : "+ str(processed_data_dir.resolve()))

    dataFrame = dataFrame[(dataFrame['country'] != 'RU')]
    dataFrame['Protocol, Port'] = dataFrame['protocol'] + ', '+dataFrame['port'].astype(str)
    scan_code_list = dataFrame.sort_values(by = ['scan_code'],ascending=True)['scan_code'].unique().tolist()
    print(scan_code_list)

    # get_scan_country_info(dataFrame)
    create_dashboard_data(dataFrame,processed_data_dir)

if __name__ == "__main__":
    main()

