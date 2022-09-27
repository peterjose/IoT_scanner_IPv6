"""

    Post processing script
    @author Peter Jose

"""

# TODO: Fix Bug in reading encoded data from the file

from cProfile import label
from calendar import day_abbr
from distutils.command.config import config
from fileinput import filename
import ipaddress
from os import minor
from re import A
from sqlite3 import Timestamp
import subprocess
from tkinter.tix import Tree
from turtle import width
import matplotlib
import pkg_resources
import argparse
import sys
import json
import pathlib
# import urllib.request
import time
from datetime import datetime, date

from urllib3 import Retry

############################# CONFIG START #############################

# Enable the flag to print the debug statments
ENABLE_DEBUG_PRINT_FLAG = True                   # True / False

# Flag to allow the installation of missing packages
ALLOW_INSTALLATION_OF_MISSING_PKGS_FLAG = True  # True / False

# File name format
ZGRAB_OUT_FILE = "_zgrab2_out.json"
ZGRAB_META_FILE = "_scan_summary.json"
ZMAP_META_FILE = "_zmap_metadata.json"

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
import matplotlib.pyplot as plt
from matplotlib.ticker import MaxNLocator
import pandas as pd
import numpy as np
import seaborn as sns
from upsetplot import from_contents, UpSet, plot
import geopandas
import pycountry
import geoip2.database
import filter_package
# import matplotlib.dates as mdates
plt.style.use('tableau-colorblind10')

colour_list = ['#FF5757',  # red
                '#145AE6', #blue
                '#FFDF36', #yellow
                # '#8C52FF', # purple
                '#7ED957', #'#6EB84E',#green
                '#7389ED',#'#5271FF', # royal blue
                '#FF914D', #orange
                '#727272', # grey
                '#008037','#CB6CE6','#E9E9E9',
                '#545454']
colour_alpha = 0.85
font_size_default = 10

# cycler(color, [#006BA4, #FF800E, #ABABAB, #595959,
#                  #5F9ED1, #C85200, #898989, #A2C8EC, #FFBC79, #CFCFCF])


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

def plot_hitrate(scan_path,
                processed_data_dir,
                file_name_prefix,
                legendPosition_outside = True,
                figsize = [10,10],
                display_values = True):
    zmap_data = []
    try:
        print("plot_hitrate")
        # get all the zmap output files
        scan_output_list = ([x.name for x in scan_path.iterdir() if x.is_dir()])
        
        for folder in scan_output_list:
            zmap_meta_files_list = ([x for x in pathlib.Path(scan_path,folder).iterdir() if (x.is_file() and x.name.find(ZMAP_META_FILE) != -1)])
            for zmap_meta_file in zmap_meta_files_list:
                fileName = zmap_meta_file.name
                zmap_meta_details = open(zmap_meta_file,'r').read()
                hitrate = 0
                try:
                    fileContent = json.loads(zmap_meta_details)
                    port = fileContent['target_port']
                    hitrate = fileContent['hitrate']
                except Exception as e: 
                    print("Error",fileName, repr(e))
                zmap_data.append({HITRATE_KEY:hitrate,PORT_KEY:port})
        df = pd.DataFrame(zmap_data)
        plt.rcParams["figure.figsize"] = figsize
        fig, ax = plt.subplots(1, 1)
        ax = df.plot.bar(x=PORT_KEY, y=HITRATE_KEY, rot=0,alpha=colour_alpha,log=True, width= 0.8)
        
        ax.set_axisbelow(True)
        ax.yaxis.grid(True, color='#EEEEEE')
        ax.xaxis.grid(False)

        if legendPosition_outside:
            ax.legend(loc='center left', bbox_to_anchor=(1, 0.5),prop={'size': font_size_default})
        else:
            ax.legend(prop={'size': font_size_default})
        ax.set_title("Open Port Hitrate", pad=15, fontsize = font_size_default+2)
        ax.set_xlabel("Open Port", labelpad=15, fontsize = font_size_default+2)
        plt.xticks(rotation= 45,fontsize= font_size_default)
        ax.set_ylabel("No. of IP Addresses(%)", labelpad=15, fontsize = font_size_default+2)
        # if display_values:
        #     display_values_on_graph(ax,font_size=font_size_default,print_threshold=0,location='mid', round_position=4)
        plt.savefig(str(pathlib.Path(processed_data_dir,file_name_prefix+'HitRate.png')), bbox_inches = 'tight', pad_inches = 0.1)
    except Exception as e: 
        print("Error", repr(e))


###
# Display the value on graph
###
def display_values_on_graph(
                            ax,
                            float_flag = True,
                            keep_zero_value=False,
                            print_threshold=0,
                            font_size = 12,
                            location='top',
                            round_position = 1,
                            rotation = 0,
                            total_value_list = [],
                            display_content_colour = 'black'):
    for bar in ax.patches:
        # The text annotation for each bar should be its height.
        bar_value = bar.get_height()
        if keep_zero_value or bar_value > print_threshold:
            # Format the text with commas to separate thousands. You can do
            # any type of formatting here though.
            if float_flag:
                text = f'{round(bar_value,round_position):,}'
            else:
                text = f'{int(bar_value):,}'
            # This will give the middle of each bar on the x-axis.
            text_x = bar.get_x() + bar.get_width()/ 2
            # get_y() is where the bar starts so we add the height to it.
            if location == 'top':
                text_y = bar.get_y() + bar_value
                if rotation:
                    text_y = text_y + 150
                # If we want the text to be the same color as the bar, we can
                # get the color like so:
                bar_color = 'black'#bar.get_facecolor()
                # bar_color = 'black'
                # If you want a consistent color, you can just set it as a constant, e.g. #222222
                ax.text(text_x, text_y, text, ha='center', va='bottom', color=bar_color, alpha = 1,
                        size=font_size,rotation=rotation)
            else:
                text_y = bar.get_y() + bar_value / 2
                # If we want the text to be the same color as the bar, we can
                # get the color like so:
                bar_color = display_content_colour
                # If you want a consistent color, you can just set it as a constant, e.g. #222222
                ax.text(text_x, text_y, text, ha='center', va='center', color=bar_color, alpha = 1,
                        size=font_size)
    if len(total_value_list): 
        for bar,val in zip(ax.patches,total_value_list):
            bar_value = 105
            text_x = bar.get_x() + bar.get_width()/ 2
            text_y = bar.get_y() + bar_value
            bar_color = 'black'
            ax.text(text_x, text_y, str(val), ha='center', va='center', color=bar_color, alpha = 1,rotation=90,
                size=font_size)
                            
        
####
# Function to plot the stacked Bar Plot
####
def plot_stackBarPlot(
                    plotData,
                    figName,
                    title = "",
                    labels_x = "",
                    labels_y = "",
                    legend_title = "",
                    legend_display=True,
                    legendPosition_outside = False,
                    plot_font_size = 0,
                    stacked_var = True,
                    display_values = False,
                    display_values_Float = True,
                    display_values_size = 0,
                    display_location='mid',
                    display_threshold=1,
                    figsize = [],#[12,10],
                    yticks_in_Kilo=False,
                    color_list=[],
                    rotate_value = 0,
                    total_value_list = [],
                    legend_loc = 'center left',
                    bbox_position = (1,0.5),
                    ncol = 1,
                    bar_width = 0.8,
                    total_y_label="Total"
                    ):
    if len(figsize):               
        plt.rcParams["figure.figsize"] = figsize
    # plt.rcParams["boxplot.boxprops.linewidth"] = 0.8
    
    # plt.rcParams["figure.autolayout"] = True
    if plot_font_size == 0:
        plot_font_size = font_size_default
    fig, ax_ = plt.subplots(1, 1)
    ax = ax_
    if len(total_value_list):
        fig, ax_ = plt.subplots(2, 1,sharex=True,gridspec_kw={'height_ratios': [1, 100]})
        ax = ax_[1]
        fig.tight_layout(h_pad=0.5)

    if not len(color_list):
        plotData.plot(kind='bar', fontsize=(plot_font_size-2),
                        stacked=stacked_var, alpha=colour_alpha,
                        width = bar_width,edgecolor = 'none', ax = ax,linewidth = 0.2)
    else:
        plotData.plot(kind='bar', fontsize=(plot_font_size-2),
                        stacked=stacked_var, alpha=colour_alpha,
                        width = bar_width,edgecolor = 'none',color=color_list, ax =ax,linewidth = 0.2)
    # ax.grid(which='major', color='#DDDDDD', linewidth=0.8)
    # ax.grid(which='minor', color='#EEEEEE', linestyle=':', linewidth=0.5)
    ax.minorticks_off()
    # ax.set_axisbelow(True)
    xlabel_pad = 5
    if not stacked_var:
        # for i in range(len(ax.get_xticklabels())):
        label_list = ax.get_xticklabels()
        ax.set_xticks(np.arange(-0.5,len(ax.get_xticklabels()),1))
        # ax.set_xticklabels([])

        # print(label_list)
        yticks_space = ax.get_yticks()[-1]/len(ax.get_yticks())
        for lab in label_list:
            ax.text(lab.get_position()[0],-1.5*yticks_space/10,lab.get_text(), ha='center', va='top', color='black', alpha = 1,rotation=90,
                size=plot_font_size-2)
            if len(lab.get_text())*3.5 > xlabel_pad:
                xlabel_pad = len(lab.get_text())* 3.5

        ax.set_xticklabels([])

    ax.set_axisbelow(True)
    ax.yaxis.grid(True, color='#EEEEEE')
    ax.xaxis.grid(False)
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    if legendPosition_outside:
        ax.legend(title = legend_title,loc=legend_loc, bbox_to_anchor=bbox_position,title_fontsize=plot_font_size -1,fontsize = plot_font_size-2,ncol=ncol,frameon=False)
    else:
        ax.legend(title = legend_title,title_fontsize=plot_font_size-1,fontsize = plot_font_size-2,frameon=False)
    if title != "":
        ax.set_title(title, pad=15, fontsize = plot_font_size)
    ax.set_xlabel(labels_x, labelpad=xlabel_pad, fontsize = plot_font_size-1)
    ax.tick_params(axis = 'both', which = 'major', labelsize = plot_font_size - 2)
    # plt.xticks(rotation= 45,fontsize= plot_font_size)
    ax.set_ylabel(labels_y, labelpad=5, fontsize = plot_font_size-1)
    if yticks_in_Kilo:
        yticks = ['{:,.0f}'.format(x) + 'K' for x in ax.get_yticks()/1000]
        ax.set_yticklabels(yticks)
    if display_values:
        if display_values_size == 0:
            display_values_size = plot_font_size-3
        display_values_on_graph(ax,font_size=display_values_size,
        print_threshold=display_threshold,location=display_location,float_flag= display_values_Float, rotation= rotate_value)
    if not legend_display:
        ax.get_legend().remove()

    if len(total_value_list):
        if stacked_var:
            incr = 0
        else:
            incr = int((len(ax.patches)/len(total_value_list))/2)
        for i in range(len(total_value_list)):
            val = total_value_list[i]
            bar = ax.patches[i+incr]
            text_x = bar.get_x()
            if stacked_var:
                text_x = text_x + bar.get_width()/2
            else:
                text_x = text_x + (bar.get_width()/2 if ((len(ax.patches)/len(total_value_list))%2 == 1) else bar.get_width())
            bar_color = 'black'
            ax_[0].text(text_x, 0, str(val), ha='center', va='center', color=bar_color, alpha = 1,
                size=plot_font_size - 2)
        ax_[0].set_ylabel(total_y_label, labelpad=17, fontsize = plot_font_size-1, rotation = 90)
        ax_[0].spines['top'].set_visible(False)
        ax_[0].spines['right'].set_visible(False)
        ax_[0].spines['bottom'].set_visible(False)
        ax_[0].set_yticks([])
        ax_[0].tick_params(bottom=False)
        ax_[0].xaxis.set_ticks_position('none') 

    plt.savefig(figName, bbox_inches = 'tight', pad_inches = 0.1,dpi = 600)


####
# Function to plot scan summary
####
def plot_scan_summary(
                    dataFrame,
                    processed_data_dir,
                    file_name_prefix):
    try:
        label_order = dataFrame['status'].unique().tolist()
        label_order.remove("success")
        label_order = ['success'] + label_order
        plotData = dataFrame.groupby(['status','Protocol, Port'],sort=False)['status'].count().unstack('status')[label_order].fillna(0)
        plot_stackBarPlot(plotData,str(pathlib.Path(processed_data_dir,file_name_prefix+'summary_raw.png')),
                            "","Protocol, Port","Count of IPs",legend_title="Scan output status",
                            legendPosition_outside= True, yticks_in_Kilo=True)
        plotData = plotData.transform(lambda x: (x/x.sum())*100, axis=1)
        plot_stackBarPlot(plotData,str(pathlib.Path(processed_data_dir,file_name_prefix+'summary_percentage.png')),
                            "","Protocol, Port","Percentage", legend_title="Scan output status",
                            legendPosition_outside= True,display_values=True)
    except Exception as e: 
        print("Error", repr(e))
  

####
# Function to plot scan summary
####
def plot_scan_summary_with_total(
                    dataFrame,
                    processed_data_dir,
                    file_name_prefix):
    try:
        label_order = dataFrame['status'].unique().tolist()
        label_order.remove("success")
        label_order = ['success'] + label_order
        total_value_list = dataFrame.groupby(['Protocol, Port'],sort=False)['status'].count().to_list()
        plotData = dataFrame.groupby(['status','Protocol, Port'],sort=False)['status'].count().unstack('status')[label_order].fillna(0)
        print(file_name_prefix)
        print(plotData)
        plotData = plotData.transform(lambda x: (x/x.sum())*100, axis=1)
        print(plotData)
        plot_stackBarPlot(plotData,str(pathlib.Path(processed_data_dir,file_name_prefix+'summary_percentage_with_total.pdf')),
                            "","Protocol, Port","No. of IP Addresses (%)", legend_title="Scan output status",
                            legendPosition_outside= True,display_values=True,total_value_list=total_value_list,figsize=[4.4,4], display_values_size=5,plot_font_size=8, 
                            total_y_label="Total")
    except Exception as e: 
        print("Error", repr(e))

def alpha3code(column):
    CODE=[]
    for country_code2 in column:
        try:
            code=pycountry.countries.get(alpha_2=country_code2).alpha_3
            CODE.append(code)
        except:
            CODE.append('None')
    return CODE

def ip_percentage_country_info(row,data):
    alpha3 = row['iso_a3']
    val = data[data['iso_a3'] == alpha3]
    if val.empty:
        return 0
    return val['ip'].tolist()[0]


####
# Function to plot scan summary
####
def plot_scan_country_info(
                    dataFrame,
                    processed_data_dir,
                    file_name_prefix):
    try:
        plt.rcParams["figure.figsize"] = [5.5,4]
        dataFrame = dataFrame[(dataFrame['status'] == 'success') & (dataFrame['country'] != '-') & (dataFrame['country'] != 'RU')]
        dataFrame = dataFrame[(dataFrame['country'] != '-') & (dataFrame['country'] != 'RU')]
        plotData = dataFrame.groupby(['country'],sort=False).count().fillna(0).reset_index()[['country','ip']]
        plotData['ip'] = plotData['ip']/float(plotData['ip'].sum())*100

        plotData = plotData.sort_values(by = ['ip'],ascending=False)
        
        plotData['iso_a3'] = alpha3code(plotData.country)
        fig, ax = plt.subplots(1, 1)

        world = geopandas.read_file(geopandas.datasets.get_path('naturalearth_lowres'))
        world['IP spread (%)'] = world.apply(ip_percentage_country_info,data = plotData, axis=1) 

        world = world[(world.name!="Antarctica")]

        world.plot(column='IP spread (%)', ax=ax, linewidth=0.01, legend=True,edgecolor='black',legend_kwds={
                        'orientation': "horizontal",'shrink':0.4, 'pad':0.025},cmap='OrRd')
    
        fig = ax.figure
        cb_ax = fig.axes[1] 
        cb_ax.tick_params(labelsize=6)
        cb_ax.set_xlabel('No. of IP Addresses (%)',fontsize=7)
        ax.set_yticks([])
        ax.set_xticks([])

        ax.set_axis_off()
        plt.rc('legend', fontsize=7)
        plt.savefig(str(pathlib.Path(processed_data_dir,file_name_prefix+'country_wise_distribution.pdf')), bbox_inches = 'tight', pad_inches = 0.1,dpi = 600)

    except Exception as e: 
        print("Error", repr(e))

##
# function to get the location wise statistics based on protocol
# 

def get_location_statistics(
                    dataFrame):

    protocol_port_list = dataFrame.sort_values(by = ['Protocol, Port'],ascending=True)['Protocol, Port'].unique().tolist()
    dataFrame = dataFrame[dataFrame['status'] == 'success']
    # for protocol_port in protocol_port_list:
    #     print(protocol_port)
    #     plotData = dataFrame[dataFrame['Protocol, Port'] == protocol_port].groupby(['country'],sort=True).count().fillna(0).reset_index()[['country','ip']]     
    #     plotData = plotData.sort_values(by = ['ip'],ascending=False)
    #     plotData1 = dataFrame[dataFrame['Protocol, Port'] == protocol_port].groupby(['AS'],sort=True).count().fillna(0).reset_index()[['AS','ip']]     
    #     plotData1 = plotData1.sort_values(by = ['ip'],ascending=False)
    #     print(plotData.head(n=5))
    #     print(plotData1.head(n=5))
    print('xmpp')
    ip1 = set(dataFrame[(dataFrame['port'].astype(str) == '5222')]['ip'].unique().tolist())
    ip2 = set(dataFrame[(dataFrame['port'].astype(str) == '5223')]['ip'].unique().tolist())    
    print(len(ip1.intersection(ip2)))
    print('amqp')
    ip1 = set(dataFrame[(dataFrame['port'].astype(str) == '5671')]['ip'].unique().tolist())
    ip2 = set(dataFrame[(dataFrame['port'].astype(str) == '5672')]['ip'].unique().tolist())    
    print(len(ip1.intersection(ip2)))
    print('coap')
    ip1 = set(dataFrame[(dataFrame['port'].astype(str) == '5684')]['ip'].unique().tolist())
    ip2 = set(dataFrame[(dataFrame['port'].astype(str) == '5683')]['ip'].unique().tolist())    
    print(len(ip1.intersection(ip2)))
    print('opc ua')
    ip1 = set(dataFrame[(dataFrame['port'].astype(str) == '4840')]['ip'].unique().tolist())
    ip2 = set(dataFrame[(dataFrame['port'].astype(str) == '4843')]['ip'].unique().tolist())    
    print(len(ip1.intersection(ip2)))
    print('mqtt')
    ip1 = set(dataFrame[(dataFrame['port'].astype(str) == '1883')]['ip'].unique().tolist())
    ip2 = set(dataFrame[(dataFrame['port'].astype(str) == '8883')]['ip'].unique().tolist())    
    print(len(ip1.intersection(ip2)))

####
# Function to get location inferences
####
def scan_location_inferences(
                    dataFrame,
                    file_name_prefix):
    try:
        pass
        # # To print the AS based distribution Total
        # dataFrame = dataFrame[dataFrame['status'] == 'success']
        # plotData = dataFrame.groupby(['country','AS'],sort=False)['country'].count().unstack('country').reset_index().fillna(0)
        # plotData.to_csv(str(processed_data_dir.resolve())+"/"+"Country_AS_total.csv", sep='\t', index=False)
        # print(plotData.head(n=10))
        # print(plotData.shape)
        
        # # To find Discovery Percentage
        # print('US')
        # plotData = dataFrame[dataFrame['country'] == 'US']
        # plotData = plotData.groupby(['status'],sort=False).count().fillna(0).reset_index()[['status','ip']]
        # # plotData['ip'] = plotData['ip']/float(plotData['ip'].sum())*100
        # plotData = plotData.sort_values(by = ['ip'],ascending=False)
        # print(plotData.head(n=10))
        # print('DE')
        # plotData = dataFrame[dataFrame['country'] == 'DE']
        # plotData = plotData.groupby(['status'],sort=False).count().fillna(0).reset_index()[['status','ip']]
        # # plotData['ip'] = plotData['ip']/float(plotData['ip'].sum())*100
        # plotData = plotData.sort_values(by = ['ip'],ascending=False)
        # print(plotData.head(n=10))
        # print('CN')
        # plotData = dataFrame[dataFrame['country'] == 'CN']
        # plotData = plotData.groupby(['status'],sort=False).count().fillna(0).reset_index()[['status','ip']]
        # # plotData['ip'] = plotData['ip']/float(plotData['ip'].sum())*100
        # plotData = plotData.sort_values(by = ['ip'],ascending=False)
        # print(plotData.head(n=10))
        

        # dataFrame = dataFrame[(dataFrame['status'] == 'success')]
        # plotData = dataFrame.groupby(['country'],sort=False).count().fillna(0).reset_index()[['country','ip']]
        # plotData = plotData.sort_values(by = ['ip'],ascending=False)
        # # plotData['ip'] = plotData['ip']/float(plotData['ip'].sum())*100
        # print(plotData.head(n=10))

        # # # To find Discovery Percentage
        # plotData = dataFrame.groupby(['country'],sort=False).count().fillna(0).reset_index()[['country','ip']]
        # # plotData['ip'] = plotData['ip']/float(plotData['ip'].sum())*100
        # plotData = plotData.sort_values(by = ['ip'],ascending=False)
        # print(plotData.head(n=10))
        # dataFrame = dataFrame[(dataFrame['status'] == 'success')]
        # plotData = dataFrame.groupby(['country'],sort=False).count().fillna(0).reset_index()[['country','ip']]
        # plotData = plotData.sort_values(by = ['ip'],ascending=False)
        # # plotData['ip'] = plotData['ip']/float(plotData['ip'].sum())*100
        # print(plotData.head(n=10))

        # # To discovery country wise information on prtocol port
        # dataFrame = dataFrame[(dataFrame['status'] == 'success')]
        # protocol_port_list = dataFrame.sort_values(by = ['Protocol, Port'],ascending=True)['Protocol, Port'].unique().tolist()
        # for protocol_port in protocol_port_list:
        #     df = dataFrame[(dataFrame['Protocol, Port'] != protocol_port) ]
        #     plotData = df.groupby(['country'],sort=False).count().fillna(0).reset_index()[['country','ip']]
        #     plotData['ip'] = plotData['ip']/float(plotData['ip'].sum())*100
        #     plotData = plotData.sort_values(by = ['ip'],ascending=False)
        #     print(protocol_port)
        #     print(plotData.head())

        # # To print the AS based distribution Total
        # # dataFrame = dataFrame[dataFrame['status'] == 'success']
        # plotData = dataFrame.groupby(['AS'],sort=False).count().fillna(0).reset_index()[['AS','ip']]
        # # plotData['ip'] = plotData['ip']/float(plotData['ip'].sum())*100
        # plotData = plotData.sort_values(by = ['ip'],ascending=False)
        # print(plotData.head(n=10))
        # print(plotData.shape)

        # # To print the AS based distribution
        # df = dataFrame[(dataFrame['status'] == 'success')]
        # plotData = df.groupby(['AS'],sort=False).count().fillna(0).reset_index()[['AS','ip']]
        # plotData['ip'] = plotData['ip']/float(plotData['ip'].sum())*100
        # plotData = plotData.sort_values(by = ['ip'],ascending=False)
        # print(plotData.head(n=10))

        # # To find Discovery Percentage AS
        # df = dataFrame[(dataFrame['status'] == 'success')]
        # plotData = df.groupby(['AS'],sort=False).count().fillna(0).reset_index()[['AS','ip']]
        # plotData = plotData.sort_values(by = ['ip'],ascending=False)
        # print(plotData.head(n=10))
        # top_AS_list = plotData.head(n=10)['AS'].to_list()
        # df = df[df['AS'].isin(top_AS_list)]
        # plotData = df.groupby(['AS'],sort=False).count().fillna(0).reset_index()[['AS','ip']]
        # # plotData['ip'] = plotData['ip']/float(plotData['ip'].sum())*100
        # plotData = plotData.sort_values(by = ['ip'],ascending=False)
        # print(plotData)

        # # To discovery AS wise information on prtocol port
        # dataFrame = dataFrame[(dataFrame['status'] == 'success')]
        # protocol_port_list = dataFrame.sort_values(by = ['Protocol, Port'],ascending=True)['Protocol, Port'].unique().tolist()
        # for protocol_port in protocol_port_list:
        #     df = dataFrame[(dataFrame['Protocol, Port'] != protocol_port) ]
        #     plotData = df.groupby(['AS'],sort=False).count().fillna(0).reset_index()[['AS','ip']]
        #     plotData['ip'] = plotData['ip']/float(plotData['ip'].sum())*100
        #     plotData = plotData.sort_values(by = ['ip'],ascending=False)
        #     print(protocol_port)
        #     print(plotData.head(n=10))

        # # AS categorisation
        # df = dataFrame.groupby(['asn_category1L1L2'],sort=False).count().fillna(0).reset_index()[['asn_category1L1L2','ip']]
        # df['ip'] = df['ip']/float(df['ip'].sum())*100
        # plotData = df.sort_values(by = ['ip'],ascending=False)
        # plotData.to_csv(str(processed_data_dir.resolve())+"/"+"AS_cat_total.csv", sep='\t', index=False)

        print(file_name_prefix)
        # dataFrame = dataFrame[dataFrame['status']=='success']
        df = dataFrame.groupby(['asn_category1L1L2'],sort=False).count().fillna(0).reset_index()[['asn_category1L1L2','ip']]
        df['ip'] = df['ip']/float(df['ip'].sum())*100
        df = df.sort_values(by = ['ip'],ascending=False)
        print(df.head(n=10))
        as_cat_list=df['asn_category1L1L2'].unique().tolist()
        for as_cat in as_cat_list:
            print(as_cat)
            print(len(dataFrame[dataFrame['asn_category1L1L2']==as_cat]['AS'].unique().tolist()))
            
        # # AS categorisation
        # df = dataFrame[(dataFrame['status'] == 'success')].groupby(['asn_category1L1L2'],sort=False).count().fillna(0).reset_index()[['asn_category1L1L2','ip']]
        # df['ip'] = df['ip']/float(df['ip'].sum())*100
        # plotData = df.sort_values(by = ['ip'],ascending=False)
        # plotData.to_csv(str(processed_data_dir.resolve())+"/"+"AS_cat_success.csv", sep='\t', index=False)
        
        # # AS categorisation
        # top_AS_category = dataFrame[(dataFrame['status'] == 'success')].groupby(['asn_category1L1L2'],sort=False).count().fillna(0).reset_index()[['asn_category1L1L2','ip']].sort_values(by = ['ip'],ascending=False).head(1)['asn_category1L1L2'].to_list()[0]
        # print(top_AS_category)
        # df = dataFrame[(dataFrame['status'] == 'success') & (dataFrame['asn_category1L1L2'] == top_AS_category)]
        # plotData = df.groupby(['Protocol, Port'],sort=False).count().fillna(0).reset_index()[['Protocol, Port','ip']]
        # plotData['ip'] = plotData['ip']/float(plotData['ip'].sum())*100
        # plotData = plotData.sort_values(by = ['ip'],ascending=False)
        # print(plotData.head())

        # Check io-timeout and country
        # print(file_name_prefix)
        # dataFrame = dataFrame[(dataFrame['status'] == 'io-timeout')]
        # plotData = dataFrame.groupby(['country'],sort=False).count().fillna(0).reset_index()[['country','ip']]
        # plotData = plotData.sort_values(by = ['ip'],ascending=False)
        # plotData['ip'] = plotData['ip']/float(plotData['ip'].sum())*100
        # print(plotData.head(n=10))

        # # # Check io-timeout and AS
        # print(file_name_prefix)
        # dataFrame = dataFrame[(dataFrame['status'] == 'io-timeout')]
        # plotData = dataFrame.groupby(['AS'],sort=False).count().fillna(0).reset_index()[['AS','ip']]
        # plotData = plotData.sort_values(by = ['ip'],ascending=False)
        # plotData['ip'] = plotData['ip']/float(plotData['ip'].sum())*100
        # print(plotData.head(n=10))

        # df = dataFrame[(dataFrame['status'] == 'io-timeout')]
        # plotData = df.groupby(['AS'],sort=False).count().fillna(0).reset_index()[['AS','ip']]
        # plotData = plotData.sort_values(by = ['AS'],ascending=False)
        # plotData = plotData[plotData['ip'] > 50]
        # AS_list_io = plotData['AS'].to_list()
        # print(plotData)
        # # plotData.to_csv(str(processed_data_dir.resolve())+"/"+file_name_prefix+"AS_io.csv", sep='\t', index=False)
        # df = dataFrame[dataFrame['AS'].isin(AS_list_io)]
        # plotData = df.groupby(['AS'],sort=False).count().fillna(0).reset_index()[['AS','ip']]
        # # plotData['ip'] = plotData['ip']/float(plotData['ip'].sum())*100
        # plotData = plotData.sort_values(by = ['AS'],ascending=False)
        # print(plotData)
        # # plotData.to_csv(str(processed_data_dir.resolve())+"/"+file_name_prefix+"AS.csv", sep='\t', index=False)


        # AS_list = ['58519','44002','41495','133481','24547','210','14618','1312','18126','23848','24921','16509','2527','46844','24444','24445','61967','15557']
        # # AS_list = [58519,44002,41495,133481,24547,210,14618,1312,18126,23848,24921,16509,2527,46844,24444,24445,61967,15557]
        # df = dataFrame[dataFrame['AS'].isin(AS_list)]
        # plotData = df.groupby(['AS'],sort=False).count().fillna(0).reset_index()[['AS','ip']]
        # # plotData['ip'] = plotData['ip']/float(plotData['ip'].sum())*100
        # plotData = plotData.sort_values(by = ['AS'],ascending=False)
        # print(plotData)

        # print(file_name_prefix)
        # AS_list = ['58519','44002','41495','133481','24547','210','14618','1312','18126','23848','24921','16509','2527','46844','24444','24445','61967','15557']
        # for ASN in AS_list:
        #     # AS_list = [58519,44002,41495,133481,24547,210,14618,1312,18126,23848,24921,16509,2527,46844,24444,24445,61967,15557]
        #     df = dataFrame[dataFrame['AS']== ASN]
        #     if(df.shape[0]>=1):
        #         print(df.head(n=1)[['AS','asn_category1L1L2']])
        #         plotData = df.groupby(['status'],sort=False).count().unstack('status').fillna(0)
        #         # plotData = df.groupby(['AS'],sort=False).count().fillna(0).reset_index()[['AS','ip']]
        #         # plotData['ip'] = plotData['ip']/float(plotData['ip'].sum())*100
        #         # plotData = plotData.sort_values(by = ['AS'],ascending=False)
        #         print(plotData['AS'])

        # print(file_name_prefix)
        # # AS_list = [58519,44002,41495,133481,24547,210,14618,1312,18126,23848,24921,16509,2527,46844,24444,24445,61967,15557]
        # df = dataFrame[dataFrame['status'] != "success"]
        # # print(df.head(n=1)[['status','error']])
        # # plotData = df[df['status'] == 'io-timeout'].groupby(['error'],sort=False).count().unstack('error').fillna(0)
        # # plotData = df.groupby(['AS'],sort=False).count().fillna(0).reset_index()[['AS','ip']]
        # # plotData['ip'] = plotData['ip']/float(plotData['ip'].sum())*100
        # # plotData = plotData.sort_values(by = ['AS'],ascending=False)
        # plotData = df[df['error'] !='EOF'][['status','error']]
        # plotData.to_csv(str(processed_data_dir.resolve())+"/"+file_name_prefix+"error.csv", sep='\t', index=False)

        # print(file_name_prefix)
        # AS_list = ['58519','44002','41495','133481','24547','210','14618','1312','18126','23848','24921','16509','2527','46844','24444','24445','61967','15557']
        # # df = dataFrame[dataFrame['status'] != "success"]
        # df = dataFrame
        # df = df[df['AS'].isin(AS_list)]
        # plotData = df[['status','error','AS','port','ip']]
        # plotData.to_csv(str(processed_data_dir.resolve())+"/"+file_name_prefix+"error.csv", sep='\t', index=False)


    except Exception as e: 
        print("Error", repr(e))

####
# Function to plot tls summary
####
def plot_tls_summary(
                    dataFrame,
                    processed_data_dir,
                    file_name_prefix):
    try:
        print("plot_tls_summary")
        dataFrame = dataFrame[(dataFrame['tls'].astype(str) == '1') & (dataFrame['status'] == 'success')]
        label_order = dataFrame['serverTLS'].unique().tolist()
        label_order.sort(reverse=True)
        total_value_list = dataFrame.groupby(['Protocol, Port'],sort=False)['serverTLS'].count().to_list()
        plotData = dataFrame.groupby(['serverTLS','Protocol, Port'],sort=False)['serverTLS'].count().unstack('serverTLS')[label_order].fillna(0)
        plotData = plotData.transform(lambda x: (x/x.sum())*100, axis=1)
        plot_stackBarPlot(plotData,str(pathlib.Path(processed_data_dir,file_name_prefix+'summary_tls.pdf')), 
                        "","Protocol, Port","No. of IP Addresses(%)",stacked_var=False,legend_title="TLS version",plot_font_size=9,
                        legendPosition_outside= True, display_values=True, display_location='top',figsize=[3.8,3],
                        display_threshold=-1,total_value_list=total_value_list,legend_display=False)
    except Exception as e: 
        print("Error", repr(e))

####
# Function to extract the date
####
def extract_date_YYYY(row):
    date_ = row['validityEnd']
    date_ = date_.split('T')[0][0:4]
    return date_

####
# Function to check validity of the certificate
####
def check_certificate_validity(row):
    # "2022-03-10T22:04:47+01:00"
    scan_date = row["scan_time"].split('+')[0].replace('-','')
    scan_date = scan_date.replace('T','')
    scan_date = scan_date.replace(':','')
    cert_valid_date = row['validityEnd']
    #"2022-04-18T21:05:43Z",
    cert_valid_date = cert_valid_date.split('Z')[0].replace('-','')
    cert_valid_date = cert_valid_date.replace('T','')
    cert_valid_date = cert_valid_date.replace(':','')
    if (int(cert_valid_date) < int(scan_date)):
        return 'Expired'
    else:
        return 'Not Expired'

####
# Function to plot the validity of the tls
####
def plot_tls_validity(
                    dataFrame,
                    processed_data_dir,
                    file_name_prefix,
                    plot_font_size = 8):
    try:
        dataFrame = dataFrame[(dataFrame['tls'].astype(str) == '1') & (dataFrame['status'] == 'success')]
        dataFrame['validityEnd_YYYY'] = dataFrame.apply(extract_date_YYYY,axis=1)
        dataFrame['isValid'] = dataFrame.apply(check_certificate_validity,axis=1)
        protocols = dataFrame['protocol'].unique().tolist()
        for protocol in protocols:
            df = dataFrame[dataFrame['protocol'] == protocol]
            plotData = df.groupby(['validityEnd_YYYY','isValid'],sort=True)['isValid'].count().unstack('isValid').fillna(0)
            figsize = [5,3]
            plt.rcParams["figure.figsize"] = figsize
            sns.set_context('talk')
            fig, ax = plt.subplots(1, 1)
            # cert_invalid_colour = 'red'
            # cert_valid_colour = 'blue'
            ax = plotData.plot(kind='bar', fontsize=plot_font_size-2, stacked=True, alpha=colour_alpha, edgecolor='none', color = ['#FF800E','#006BA4'])
            
            ax.set_axisbelow(True)
            ax.yaxis.grid(True, color='#EEEEEE')
            ax.xaxis.grid(False)
            ax.spines['top'].set_visible(False)
            ax.spines['right'].set_visible(False)
            plt.xticks(rotation=90)
            # ax.legend(prop={'size': plot_font_size-1})
            display_values_on_graph(ax,float_flag= False, keep_zero_value=False,font_size=plot_font_size-3)

            ax.set_xlabel('Year', labelpad=15, fontsize = plot_font_size-1)
            ax.set_ylabel('Count', labelpad=15, fontsize = plot_font_size -1)
            ax.tick_params(axis = 'both', which = 'major', labelsize = plot_font_size - 2)
            ax.legend(title = 'Certificate validity',title_fontsize=font_size_default-2,fontsize = font_size_default-3)
            plt.savefig(str(pathlib.Path(processed_data_dir,file_name_prefix+protocol+'_cert_validity.pdf')), bbox_inches = 'tight', pad_inches = 0.1,dpi = 600)
    except Exception as e: 
        print("Error", repr(e))

####
# Function to plot the validity of the tls
####
def plot_tls_validity_line(
                    dataFrame,
                    processed_data_dir,
                    file_name_prefix,
                    plot_font_size = 8):
    try:
        dataFrame = dataFrame[(dataFrame['tls'].astype(str) == '1') & (dataFrame['status'] == 'success')]
        dataFrame['validityEnd_YYYY'] = dataFrame.apply(extract_date_YYYY,axis=1)
        # dataFrame['isValid'] = dataFrame.apply(check_certificate_validity,axis=1)
        protocols = dataFrame['protocol'].unique().tolist()
    
        plotData = dataFrame.groupby(['Protocol, Port','validityEnd_YYYY'],sort=True)['Protocol, Port'].count().unstack('Protocol, Port').fillna(0)
        figsize = [5,3]
        plt.rcParams["figure.figsize"] = figsize
        sns.set_context('talk')
        fig, ax = plt.subplots(1, 1)
        # cert_invalid_colour = 'red'
        # cert_valid_colour = 'blue'
        ax = plotData.plot.line(linewidth=0.1)
        # fontsize=plot_font_size-2, stacked=True, alpha=colour_alpha, edgecolor='none', color = ['#FF800E','#006BA4'])
        
        ax.set_axisbelow(True)
        ax.yaxis.grid(True, color='#EEEEEE')
        ax.xaxis.grid(False)
        ax.spines['top'].set_visible(False)
        ax.spines['right'].set_visible(False)
        plt.xticks(rotation=90)
        # ax.legend(prop={'size': plot_font_size-1})
        display_values_on_graph(ax,float_flag= False, keep_zero_value=False,font_size=plot_font_size-3)

        ax.set_xlabel('Year', labelpad=15, fontsize = plot_font_size-1)
        ax.set_ylabel('Count', labelpad=15, fontsize = plot_font_size -1)
        ax.tick_params(axis = 'both', which = 'major', labelsize = plot_font_size - 2)
        ax.legend(title = 'Certificate validity',title_fontsize=font_size_default-2,fontsize = font_size_default-3)
        plt.savefig(str(pathlib.Path(processed_data_dir,file_name_prefix+'_cert_validity.pdf')), bbox_inches = 'tight', pad_inches = 0.1,dpi = 600)
    except Exception as e: 
        print("Error", repr(e))

####
# Function to plot the validity of the tls
####
def plot_tls_validity_category(
                    dataFrame,
                    figName,
                    legendtitle = "",
                    labels_x = "",
                    labels_y = "",
                    display_values = True,
                    percentage_flag = False,
                    plot_font_size=8):
    try:
        dataFrame = dataFrame[(dataFrame['tls'].astype(str) == '1') & (dataFrame['status'] == 'success')]
        dataFrame['isValid'] = dataFrame.apply(check_certificate_validity,axis=1)
        total_value_list = []
        dataFrame = dataFrame.sort_values(by=['Protocol, Port','isValid'],ascending=[True,False])
        plotData = dataFrame.groupby(['Protocol, Port','isValid'],sort=False)['isValid'].count().unstack('isValid').fillna(0)
        if percentage_flag:
            plotData = plotData.transform(lambda x: (x/x.sum())*100, axis=1)
            total_value_list = dataFrame.groupby(['Protocol, Port'],sort=False)['isValid'].count().to_list()
        plot_stackBarPlot(plotData,figName,"",labels_x, labels_y,legend_title=legendtitle,
                        legendPosition_outside= True,display_values=display_values,legend_display=False,
                        figsize=[2,3],color_list=['#006BA4','#FF800E'], total_value_list=total_value_list,plot_font_size=plot_font_size)
    except Exception as e: 
        print("Error", repr(e))


####
# Function to plot the self signed of the tls
####
def plot_tls_signed_category(
                    dataFrame,
                    figName,
                    legendtitle = "",
                    labels_x = "",
                    labels_y = "",
                    display_values = True,
                    percentage_flag = False,
                    plot_font_size = 8):
    try:
        dataFrame = dataFrame[(dataFrame['tls'].astype(str) == '1') & (dataFrame['status'] == 'success')]
        label_order = dataFrame['self_signed'].unique().tolist()
        label_order.sort(reverse=False)
        total_value_list = []
        plotData = dataFrame.groupby(['Protocol, Port','self_signed'],sort=False)['self_signed'].count().unstack('self_signed')[label_order].fillna(0)
        if percentage_flag:
            plotData = plotData.transform(lambda x: (x/x.sum())*100, axis=1)
            total_value_list = dataFrame.groupby(['Protocol, Port'],sort=False)['self_signed'].count().to_list()
        plot_stackBarPlot(plotData,figName,"",labels_x, labels_y,legend_title=legendtitle,
                        legendPosition_outside= True,display_values=display_values,
                        figsize=[2,2.7],color_list=['#006BA4','#FF800E'],total_value_list=total_value_list,plot_font_size=plot_font_size)
    except Exception as e: 
        print("Error", repr(e))

####
# Function to plot the self signed of the tls
####
def plot_tls_browsertrusted_category(
                    dataFrame,
                    figName,
                    legendtitle = "",
                    labels_x = "",
                    labels_y = "",
                    display_values = True,
                    percentage_flag = False,
                    plot_font_size=8):
    try:
        dataFrame = dataFrame[(dataFrame['tls'].astype(str) == '1') & (dataFrame['status'] == 'success')]
        dataFrame.loc[dataFrame['browser_trusted'] == 'Browser Trusted', 'browser_trusted'] = 'Yes'
        dataFrame.loc[dataFrame['browser_trusted'] == 'Browser Not Trusted', 'browser_trusted'] = 'No'
        plotData = dataFrame.groupby(['Protocol, Port','browser_trusted'],sort=False)['browser_trusted'].count().unstack('browser_trusted').fillna(0)
        total_value_list = []
        if percentage_flag:
            plotData = plotData.transform(lambda x: (x/x.sum())*100, axis=1)
            total_value_list = dataFrame.groupby(['Protocol, Port'],sort=False)['browser_trusted'].count().to_list()
        plot_stackBarPlot(plotData,figName,"",labels_x, labels_y,legend_title= legendtitle,
                        legendPosition_outside= True,display_values=display_values,
                        figsize=[2,2.7],color_list=['#006BA4','#FF800E'],total_value_list=total_value_list,plot_font_size=plot_font_size)
    except Exception as e: 
        print("Error", repr(e))

def browser_trusted_error_category(row):
    error_code = ""
    # if row['self_signed'] == 'Self Signed':
    #     error_code = "Self Signed & "
    if row['browser_error'] == "x509: unknown error":
        # if row['self_signed'] != 'Self Signed':
        #     print(row['protocol'],row['port'],row['ip'])
        return error_code + "x509: unknown error"
    elif (row['browser_error'] == "x509: certificate has expired or is not yet valid" or 
            row['browser_error'] == "x509: certificate will never be valid"):
        return  error_code + "x509: certificate not valid"
    else:
        return error_code + row['browser_error']

####
# Function to plot the self signed of the tls
####
def plot_tls_browser_error_category(
                    dataFrame,
                    figName,
                    legendtitle = "",
                    labels_x = "",
                    labels_y = "",
                    display_values = True,
                    percentage_flag = False,
                    plot_font_size=8):
    try:
        dataFrame = dataFrame[(dataFrame['tls'].astype(str) == '1') & (dataFrame['status'] == 'success') & (dataFrame['browser_trusted'] == 'Browser Not Trusted')]
        dataFrame['Browser_error'] = dataFrame.apply(browser_trusted_error_category,axis=1)
        plotData = dataFrame.groupby(['Protocol, Port','Browser_error'],sort=True)['Browser_error'].count().unstack('Browser_error').fillna(0)
        total_value_list = []
        if percentage_flag:
            plotData = plotData.transform(lambda x: (x/x.sum())*100, axis=1)
            total_value_list = dataFrame.groupby(['Protocol, Port'],sort=False)['Browser_error'].count().to_list()
        plot_stackBarPlot(plotData,figName,"",labels_x, labels_y,legend_title=legendtitle,
                        legendPosition_outside= True,display_values=display_values,legend_loc='upper left',bbox_position=(-0.75,1.7),
                        figsize=[2,2.5],total_value_list=total_value_list,plot_font_size=plot_font_size)
    except Exception as e: 
        print("Error", repr(e))

####
# Function to plot tls data
####
def plot_tls_data(
                dataFrame, 
                processed_data_dir,
                file_name_prefix):
    # plot_tls_validity(dataFrame,processed_data_dir,file_name_prefix)
    plot_tls_validity_line(dataFrame,processed_data_dir,file_name_prefix)
    # plot_tls_validity_category(dataFrame,str(pathlib.Path(processed_data_dir,file_name_prefix+'cert_validity_count.pdf')),
    #                             "Certificate validity","Protocol","Count of Certificates")   
    plot_tls_validity_category(dataFrame,str(pathlib.Path(processed_data_dir,file_name_prefix+'cert_validity_percentage.pdf')),
                                "Certificate validity","Protocol, Port","No. of Certificates (%)",
                                percentage_flag=True,plot_font_size=9)
    # plot_tls_signed_category(dataFrame,str(pathlib.Path(processed_data_dir,file_name_prefix+'cert_signed_count.pdf')),
    #                             "Self-signed Certificate","Protocol, Port","Count of Certificates")   
    plot_tls_signed_category(dataFrame,str(pathlib.Path(processed_data_dir,file_name_prefix+'cert_signed_percentage.pdf')),
                                "Self-signed certificate","Protocol, Port","No. of Certificates (%)",
                                percentage_flag=True,plot_font_size=9)  
    # plot_tls_browsertrusted_category(dataFrame,str(pathlib.Path(processed_data_dir,file_name_prefix+'cert_browser_trusted_count.pdf')),
    #                             "Browser Trusted Certificate","Protocol, Port","Count of Certificates")   
    plot_tls_browsertrusted_category(dataFrame,str(pathlib.Path(processed_data_dir,file_name_prefix+'cert_browser_trusted_percentage.pdf')),
                                "Trusted certificate","Protocol, Port","No. of Certificates (%)",
                                percentage_flag=True,plot_font_size=9) 
    # plot_tls_browser_error_category(dataFrame,str(pathlib.Path(processed_data_dir,file_name_prefix+'cert_browser_error_count.pdf')),
    #                             "Browser Certificate Error","Protocol, Port","Count of Certificates")   
    plot_tls_browser_error_category(dataFrame,str(pathlib.Path(processed_data_dir,file_name_prefix+'cert_browser_error_percentage.pdf')),
                                "Certificate Validation Error","Protocol, Port","No. of Certificates (%)",
                                percentage_flag=True,plot_font_size=9)

def get_cipher_info(
                dataFrame, 
                processed_data_dir,
                file_name_prefix):
    # # Cipher info
    dataFrame = dataFrame[dataFrame['tls'].astype(str) == '1']
    df = dataFrame.groupby(['signature_algorithm'],sort=False).count().fillna(0).reset_index()[['signature_algorithm','ip']]
    df['ip'] = df['ip']/float(df['ip'].sum())*100
    plotData = df.sort_values(by = ['ip'],ascending=False)
    # plotData.to_csv(str(processed_data_dir.resolve())+"/"+"AS_cat_total.csv", sep='\t', index=False)
    print(plotData)

    # # # Cipher info
    # dataFrame = dataFrame[dataFrame['tls'].astype(str) == '1']
    # df = dataFrame.groupby(['server_cipher_suite','signature_algorithm'],sort=False).count().fillna(0).reset_index()[['server_cipher_suite','signature_algorithm','ip']]
    # df['ip'] = df['ip']/float(df['ip'].sum())*100
    # plotData = df.sort_values(by = ['ip'],ascending=False)
    # # plotData.to_csv(str(processed_data_dir.resolve())+"/"+"AS_cat_total.csv", sep='\t', index=False)
    # print(plotData)

    # # Cipher info
    # dataFrame = dataFrame[(dataFrame['tls'].astype(str) == '1') & (dataFrame['server_cipher_suite'] == 'TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256') ]
    # print(dataFrame['signature_algorithm'])
    # df = dataFrame.groupby(['length'],sort=False).count().fillna(0).reset_index()[['length','ip']]
    # df['ip'] = df['ip']/float(df['ip'].sum())*100
    # plotData = df.sort_values(by = ['ip'],ascending=False)
    # # plotData.to_csv(str(processed_data_dir.resolve())+"/"+"AS_cat_total.csv", sep='\t', index=False)
    # print(plotData)

####
# Function to plot ip and port count
####
def plot_ip_port_count(
                    dataFrame,
                    processed_data_dir,
                    file_name_prefix,
                    figsize = [12,10]):
    print("plot_ip_port_count")
    try:
        df=dataFrame[dataFrame['status'] == 'success'].groupby('ip')['port'].count().reset_index()
        plotData = df.groupby('port')['ip'].count().reset_index()
        plotData = plotData[['ip','port']]
        plotData.rename(columns = {'ip':'No of IPs','port':'No of Protocols'}, inplace = True)
        plt.rcParams["figure.figsize"] = figsize
        sns.set_context('talk')
        fig, ax = plt.subplots(1, 1)
        ax = plotData.plot(kind='bar', fontsize=font_size_default)
        ax = plotData.plot.bar(x='No of Protocols', y='No of IPs', rot=0,alpha=colour_alpha, width=0.7)
        
        ax.set_axisbelow(True)
        ax.yaxis.grid(True, color='#EEEEEE')
        ax.xaxis.grid(False)

        ax.set_xlabel('No of protocols supported', labelpad=15, fontsize = font_size_default)
        ax.set_ylabel('Count', labelpad=15, fontsize = font_size_default)
        ax.set_title('IP and No of Protocols', pad=15, fontsize = font_size_default+2)
        ylabels = ['{:,.0f}'.format(x) + 'K' for x in ax.get_yticks()/1000]
        ax.set_yticklabels(ylabels)
        # ax.xaxis.set_major_formatter(ticker.FuncFormatter(lambda x, pos: '{:,.2f}'.format(x/1000) + 'K'))
        display_values_on_graph(ax,float_flag=False)
        ax.get_legend().remove()
        plt.savefig(str(pathlib.Path(processed_data_dir,file_name_prefix+'port_ip.png')), bbox_inches = 'tight', pad_inches = 0.1)
    except Exception as e: 
        print("Error", repr(e))


####
# Function to plot ip and port count
####
def plot_ip_port_upsetplot_two(
                    dataFrame,
                    processed_data_dir,
                    file_name_prefix,
                    figsize = [12,10]):
    print("plot_ip_port_upsetplot_two")
    try:
        # fig, ax = plt.subplots(1,1)
        # fig1 = plt.figure(figsize=(5.8,2.81))
        fig, ax = plt.subplots(1, 2)
        dataFrame = dataFrame[dataFrame['status'] == 'success']
        dataFrame = dataFrame.sort_values(by = ['Protocol, Port'], ascending= True)
        protocol_port_list = dataFrame['Protocol, Port'].unique().tolist()
        dict_fromContent = dict()
        for val in protocol_port_list:
            dict_fromContent[val] = dataFrame[dataFrame['Protocol, Port'] == val]['ip'].unique().tolist()
        # df  = from_contents(dict_fromContent)
        df_main=from_contents(dict_fromContent).reset_index()
        col = df_main.columns.to_list()
        col.remove('id')
        df_main['count']=df_main[col].sum(axis=1)
        
        df=df_main[df_main['count']==1]
        df.pop('count')
        df = df.set_index(col)

        upset = UpSet(df, subset_size='auto',show_counts = True,min_degree = 1,sort_categories_by = None,element_size=20)
        # upset.style_subsets(present=["(MQTT, 1883)", "(MQTTS, 8883)"],facecolor="blue")
        # upset.style_subsets(present=["(CoAP, 5683)", "(CoAPs, 5684)"],facecolor="blue")
        # upset.style_subsets(present=["(AMQP, 5671)", "(AMQP, 5672)"],facecolor="blue")
        # upset.style_subsets(present=["(XMPP, 5222)", "(XMPP, 5223)"],facecolor="blue")
        ax[0] = upset.plot()
        ax[0]['totals'].set_xlabel("No. of IP Addresses")
        ax[0]['intersections'].set_ylabel("No. of IP Addresses")

        df=df_main[df_main['count']>1]
        df.pop('count')
        df = df.set_index(col)

        upset = UpSet(df, subset_size='auto',show_counts = True,min_degree = 1,sort_categories_by = None,element_size=20)
        # upset.style_subsets(present=["(MQTT, 1883)", "(MQTTS, 8883)"],facecolor="blue")
        # upset.style_subsets(present=["(CoAP, 5683)", "(CoAPs, 5684)"],facecolor="blue")
        # upset.style_subsets(present=["(AMQP, 5671)", "(AMQP, 5672)"],facecolor="blue")
        # upset.style_subsets(present=["(XMPP, 5222)", "(XMPP, 5223)"],facecolor="blue")
        ax[1] = upset.plot()
        ax[1]['totals'].set_xlabel("No. of IP Addresses")
        ax[1]['intersections'].set_ylabel("No. of IP Addresses")
        plt.savefig(str(pathlib.Path(processed_data_dir,file_name_prefix+'upsetplot.pdf')), bbox_inches = 'tight', pad_inches = 0.1)
        
    except Exception as e: 
        print("Error", repr(e))

####
# Function to plot ip and port count
####
def plot_ip_port_upsetplot(
                    dataFrame,
                    processed_data_dir,
                    file_name_prefix,
                    figsize = [12,10]):
    print("plot_ip_port_upsetplot")
    try:
        # fig, ax = plt.subplots(1,1)
        dataFrame = dataFrame[dataFrame['status'] == 'success']
        dataFrame = dataFrame.sort_values(by = ['Protocol, Port'], ascending= True)
        protocol_port_list = dataFrame['Protocol, Port'].unique().tolist()
        dict_fromContent = dict()
        for val in protocol_port_list:
            dict_fromContent[val] = dataFrame[dataFrame['Protocol, Port'] == val]['ip'].unique().tolist()
        # df = from_contents(dict_fromContent)
        df=from_contents(dict_fromContent).reset_index()
        col = df.columns.to_list()
        col.remove('id')
        df['count']=df[col].sum(axis=1)
        df=df[df['count']>1]
        df.pop('count')
        df = df.set_index(col)
        fig1 = plt.figure(figsize=(5.8,2.81))
        upset = UpSet(df, subset_size='auto',show_counts = True,min_degree = 1,sort_categories_by = None,element_size=20)

        # upset.style_subsets(present=["(MQTT, 1883)", "(MQTTS, 8883)"],facecolor="blue")
        # upset.style_subsets(present=["(CoAP, 5683)", "(CoAPs, 5684)"],facecolor="blue")
        # upset.style_subsets(present=["(AMQP, 5671)", "(AMQP, 5672)"],facecolor="blue")
        # upset.style_subsets(present=["(XMPP, 5222)", "(XMPP, 5223)"],facecolor="blue")
        ax = upset.plot(fig=fig1)
        ax['totals'].set_xlabel("No. of IP Addresses")
        ax['intersections'].set_ylabel("No. of IP Addresses")
        plt.savefig(str(pathlib.Path(processed_data_dir,file_name_prefix+'upsetplot.pdf')), bbox_inches = 'tight', pad_inches = 0.1)
        
    except Exception as e: 
        print("Error", repr(e))

####
# Function to plot ip port count ecdf
####
def plot_ip_port_count_ecdf(
                        dataFrame,
                        processed_data_dir,
                        file_name_prefix):
    print("plot_ip_port_count_ecdf")
    try:
        df=dataFrame[dataFrame['status'] == 'success'].groupby('ip')['port'].count().reset_index()
        # plotData = df.groupby('port')['ip'].count().reset_index()
        # plotData = plotData[['ip','port']]
        plotData = df[['ip','port']]
        plotData = plotData.astype({"port":int})
        plotData.rename(columns = {'port':'No of Protocols'}, inplace = True)
        # plotData.rename(columns = {'ip':'Count of IPs','port':'No of Protocols'}, inplace = True)
        plt.rcParams["figure.figsize"] = [2, 2.3]
        sns.set_context('talk')
        fig, ax = plt.subplots(1, 1)
        # making ECDF plot 
        sns.ecdfplot(plotData, x = 'No of Protocols', ax= ax,lw=1)
        plt.gca().xaxis.set_major_locator(MaxNLocator(integer=True))

        # ax = plotData.plot(kind='bar', fontsize=18)
        # ax = plotData.plot.bar(x='No of Protocols', y='No of IPs', rot=0,color=colour_list[1],alpha=colour_alpha)
        
        ax.set_axisbelow(True)
        ax.yaxis.grid(True, color='#EEEEEE')
        ax.xaxis.grid(False)
        ax.spines['left'].set_linewidth(1)
        ax.spines['bottom'].set_linewidth(1)
        ax.spines['top'].set_visible(False)
        ax.spines['right'].set_visible(False)
        ax.tick_params(axis = 'both', which = 'major', labelsize = 8 - 1,grid_linewidth = 0.02,width=0.03, length=3)
        # ax.tick_params(direction='out', length=6, width=2, colors='r',grid_color='r', grid_alpha=0.5)

        ax.set_xlabel('No. of Protocol-Port pairs', labelpad=5, fontsize = 8)
        ax.set_ylabel('Proportion', labelpad=5, fontsize = 8)
        # ax.set_title('No of Protocols Vs IP Proportion', pad=15, fontsize = font_size_default+2)

        # display_values_on_graph(ax,float_flag=False)
        plt.savefig(str(pathlib.Path(processed_data_dir,file_name_prefix+'port_ip_ecdf.pdf')), bbox_inches = 'tight', pad_inches = 0.1,dpi = 600)
    except Exception as e: 
        print("Error", repr(e))

def plot_correlations_line_port_count(dataFrame,processed_data_dir):
    print("plot_correlations_line_port_count")
    plotData = dataFrame.groupby(['port','scan_code'],sort=True)['status'].count().reset_index()
    fx, ax = plt.subplots()
    # plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%d-%m-%Y'))
    # plt.gca().xaxis.set_major_locator(mdates.DayLocator())
    for port in plotData['port'].unique():
        p = plotData[plotData['port']==port]
        x = p['scan_code'].tolist()
        # x = [dt.datetime.strptime(d,'%d-%m-%Y').date() for d in x]
        plt.plot(x, p['status'].tolist())
    # plt.gcf().autofmt_xdate()
    plt.xticks(rotation = 90)
    plt.grid(axis='y', color='0.95')
    plt.xlabel('Scan date')
    plt.ylabel('Count')
    ax.set_axisbelow(True)
    ax.tick_params(labelsize='small')
    ax.yaxis.labelpad = 5
    plt.legend(loc='lower right')
    ax.spines['right'].set_visible(False)
    ax.spines['top'].set_visible(False)
    # ax.legend(loc='center left',bbox_to_anchor=(1.0, 0.4))
    plt.savefig(str(pathlib.Path(processed_data_dir,'port_count_line_stability.png')), bbox_inches = 'tight', pad_inches = 0.1)     

def plot_correlations_stack_port_protocol_count(
                                    dataFrame,
                                    processed_data_dir):
    print("plot_correlations_stack_port_protocol_count")
    try:
        dataFrame = dataFrame[dataFrame['status'] == "success"]
        dataFrame = dataFrame.sort_values(by = ['Protocol, Port','scan_code'],ascending=[True,True])
        plotData = dataFrame.groupby(['Protocol, Port','scan_code'],sort=False)['scan_code'].count().unstack('scan_code').fillna(0)
        plot_stackBarPlot(plotData,str(pathlib.Path(processed_data_dir,'port_protocol_count_stack_stability.pdf')),
                        "","Protocol, Port", "No. of IP Addresses", legend_title="Scan Date",
                        legendPosition_outside= True,display_values=True, stacked_var=False,yticks_in_Kilo=True,plot_font_size=8,display_threshold = -1,
                        figsize = [5.8,2.4],display_location='top',display_values_Float=False, rotate_value = 90,legend_loc='upper center',
                        bbox_position=(0.5, 1.30),ncol=len(dataFrame['scan_code'].unique().tolist()))
    except Exception as e: 
        print("Error", repr(e))

def plot_correlations_stack_port_count(
                                    dataFrame,
                                    processed_data_dir):
    print("plot_correlations_stack_port_count")
    try:
        dataFrame = dataFrame.sort_values(by = ['Protocol, Port','scan_code'],ascending=[True,True])
        plotData = dataFrame.groupby(['port','scan_code'],sort=False)['scan_code'].count().unstack('scan_code').fillna(0)
        plot_stackBarPlot(plotData,str(pathlib.Path(processed_data_dir,'port_count_stack_stability.pdf')),
                        "","Port", "No. of IP Addresses", legend_title="Scan Date",
                        legendPosition_outside= True,display_values=True, stacked_var=False,yticks_in_Kilo=True,plot_font_size=8,display_threshold=-1,
                        figsize = [5.8,2.4],display_location='top',display_values_Float=False, rotate_value = 90,legend_loc='upper center',
                        bbox_position=(0.5, 1.30),ncol=len(dataFrame['scan_code'].unique().tolist()))
    except Exception as e: 
        print("Error", repr(e))

def plot_stability_count_old(
                        dataFrame,
                        processed_data_dir,
                        percentage_flag = True):
    print("plot_stability_count_old")
    try:
        scan_code_list = dataFrame['scan_code'].unique().tolist()
        scan1 = dataFrame[(dataFrame['scan_code'] == scan_code_list[0]) & (dataFrame['status'] == 'success')]
        scan2 = dataFrame[(dataFrame['scan_code'] == scan_code_list[1]) & (dataFrame['status'] == 'success')]
        protocol_list = dataFrame['protocol'].unique().tolist()
        stats = []
        for protocol in protocol_list:
            port_list = dataFrame[dataFrame['protocol']==protocol]['port'].unique().tolist()
            for port in port_list:
                ipList1 = scan1[scan1['port']==port]['ip'].tolist()
                df = scan2[scan2['port']==port]
                df = df[df['ip'].isin(ipList1)]
                ipList2 = df['ip'].unique().tolist()
                stats.append({'Port, Protocol': "("+str(port) +", " +protocol+")",
                'Old IPs' : scan1[scan1['port']==port].shape[0] - len(ipList2),
                'Common IPs' : len(ipList2),
                'New IPs': scan2[scan2['port']==port].shape[0] - len(ipList2)})
        
        df = pd.DataFrame(stats)
        df = df.set_index('Port, Protocol')

        if percentage_flag:
            df = df.transform(lambda x: (x/x.sum())*100, axis=1)
        plot_stackBarPlot(df, str(pathlib.Path(processed_data_dir,'IP_persistance.png')),
                        "IP Persistance","Port, Protocol", "Percentage of IPs",
                        legendPosition_outside = True,display_values=True, yticks_in_Kilo=True,
                        display_values_Float = False)
    except Exception as e: 
        print("Error", repr(e))

def is_present(row, scan_data):
    ip2 = row['ip']
    as2 = row['AS']
    if ip2 in scan_data['ip'].tolist():
        sel = scan_data.loc[scan_data['ip'] == ip2]
        if as2 == (sel['AS'].values)[0]:
            return "IP and AS match"
        else:
            return "IP match"
    return "No match"

def plot_stability_count2(
                        dataFrame,
                        processed_data_dir,
                        percentage_flag = True):
    try:
        scan_code_list = dataFrame['scan_code'].unique().tolist()
        scan1 = dataFrame[(dataFrame['scan_code'] == scan_code_list[0]) & (dataFrame['status'] == 'success')]
        scan2 = dataFrame[(dataFrame['scan_code'] == scan_code_list[1]) & (dataFrame['status'] == 'success')]
        protocol_list = dataFrame['protocol'].unique().tolist()
        stats = []
        for protocol in protocol_list:
            port_list = dataFrame[dataFrame['protocol']==protocol]['port'].unique().tolist()
            for port in port_list:
                scan1_subset = scan1[scan1['port']==port]
                scan2_subset = scan2[scan2['port']==port]
                old_ip_count = 0
                ip_match_count = 0
                ip_as_match_count = 0
                new_ip = 0
                if not scan1_subset.empty:
                    scan1_list = scan1_subset.apply(is_present,scan_data = scan2_subset,axis=1).tolist()
                    old_ip_count = scan1_list.count('No match')
                if not scan2_subset.empty:
                    scan2_list = scan2_subset.apply(is_present,scan_data = scan1_subset,axis=1).tolist()
                    ip_match_count = scan2_list.count('IP match')
                    ip_as_match_count = scan2_list.count('IP and AS match')
                    new_ip = scan2_list.count('No match')
                stats.append({'Port, Protocol': "("+str(port) +", " +protocol+")",
                'Old IPs' : old_ip_count,
                'IP Match only' : ip_match_count,
                'IP and AS Match': ip_as_match_count,
                'New IPs' : new_ip,
                })
        # df = scan2['portocol','port','IP Persistance']
        # df = df.groupby(['portocol','port','IP Persistance'],sort = False)['IP Persistance'].count().unstack('IP Persistance').fillna(0)

        df = pd.DataFrame(stats)
        df = df.set_index('Port, Protocol')

        if percentage_flag:
            df = df.transform(lambda x: (x/x.sum())*100, axis=1)
        plot_stackBarPlot(df, str(pathlib.Path(processed_data_dir,'IP_persistance.png')),
                        "IP Persistance","Port, Protocol", "Percentage of IPs",
                        legendPosition_outside = True,display_values=True, yticks_in_Kilo=True,
                        display_values_Float = False)
    except Exception as e: 
        print("Error", repr(e))


def plot_protocol_stability_count(
                        dataFrame,
                        processed_data_dir,
                        percentage_flag = True):
    print("plot_protocol_stability_count")
    try:
        scan_code_list = dataFrame.sort_values(by = ['scan_code'],ascending=True)['scan_code'].unique().tolist()
        scan1 = dataFrame[(dataFrame['scan_code'] == scan_code_list[0]) & (dataFrame['status'] == 'success')]
        scan2 = dataFrame[(dataFrame['scan_code'] == scan_code_list[1]) & (dataFrame['status'] == 'success')]
        protocol_list = dataFrame['protocol'].unique().tolist()
        stats = []
        for protocol in protocol_list:
            port_list = dataFrame[dataFrame['protocol']==protocol]['port'].unique().tolist()
            for port in port_list:
                scan1_subset = scan1[scan1['port']==port]
                scan2_subset = scan2[scan2['port']==port]
                old_ip_count = 0
                ip_match_count = 0
                ip_as_match_count = 0
                new_ip = 0
                if not scan1_subset.empty:
                    scan1_subset['IP Persistance'] = scan1_subset.apply(is_present,scan_data = scan2_subset,axis=1)
                    old_ip_count = scan1_subset[scan1_subset['IP Persistance'] =='No match'].shape[0]
                if not scan2_subset.empty:
                    scan2_subset['IP Persistance'] = scan2_subset.apply(is_present,scan_data = scan1_subset,axis=1)
                    ip_match_count = scan2_subset[scan2_subset['IP Persistance'] == 'IP match'].shape[0]
                    ip_as_match_count = scan2_subset[scan2_subset['IP Persistance'] == 'IP and AS match'].shape[0]
                    new_ip = scan2_subset[scan2_subset['IP Persistance'] == 'No match'].shape[0]
                stats.append({'Port, Protocol': "("+str(port) +", " +protocol+")",
                'Old IPs' : old_ip_count,
                'IP Match only' : ip_match_count,
                'IP and AS Match': ip_as_match_count,
                'New IPs' : new_ip,
                })
        # df = scan2['portocol','port','IP Persistance']
        # df = df.groupby(['portocol','port','IP Persistance'],sort = False)['IP Persistance'].count().unstack('IP Persistance').fillna(0)

        df = pd.DataFrame(stats)
        df = df.set_index('Port, Protocol')


        plot_stackBarPlot(df, str(pathlib.Path(processed_data_dir,'IP_persistance_Protocol_count.png')),
                        "IP Persistance","Port, Protocol", "Count of IPs",
                        legendPosition_outside = True,display_values=False)
        if percentage_flag:
            df = df.transform(lambda x: (x/x.sum())*100, axis=1)
        plot_stackBarPlot(df, str(pathlib.Path(processed_data_dir,'IP_persistance_Protocol_percentage.png')),
                        "IP Persistance","Port, Protocol", "Percentage of IPs",
                        legendPosition_outside = True,display_values=True)
    except Exception as e: 
        print("Error", repr(e))

def plot_port_stability_count(
                        dataFrame,
                        processed_data_dir,
                        percentage_flag = True):
    print("plot_port_stability_count")
    try:
        scan_code_list = dataFrame.sort_values(by = ['scan_code'],ascending=True)['scan_code'].unique().tolist()
        scan1 = dataFrame[dataFrame['scan_code'] == scan_code_list[0]]
        scan2 = dataFrame[dataFrame['scan_code'] == scan_code_list[1]]
        stats = []
        port_list = dataFrame['port'].unique().tolist()
        for port in port_list:
            scan1_subset = scan1[scan1['port']==port]
            scan2_subset = scan2[scan2['port']==port]
            old_ip_count = 0
            ip_match_count = 0
            ip_as_match_count = 0
            new_ip = 0
            if not scan1_subset.empty:
                scan1_subset['IP Persistance'] = scan1_subset.apply(is_present,scan_data = scan2_subset,axis=1)
                old_ip_count = scan1_subset[scan1_subset['IP Persistance'] =='No match'].shape[0]
            if not scan2_subset.empty:
                scan2_subset['IP Persistance'] = scan2_subset.apply(is_present,scan_data = scan1_subset,axis=1)
                ip_match_count = scan2_subset[scan2_subset['IP Persistance'] == 'IP match'].shape[0]
                ip_as_match_count = scan2_subset[scan2_subset['IP Persistance'] == 'IP and AS match'].shape[0]
                new_ip = scan2_subset[scan2_subset['IP Persistance'] == 'No match'].shape[0]
            stats.append({'Port': str(port),
            'Old IPs' : old_ip_count,
            'IP Match only' : ip_match_count,
            'IP and AS Match': ip_as_match_count,
            'New IPs' : new_ip,
            })
        # df = scan2['portocol','port','IP Persistance']
        # df = df.groupby(['portocol','port','IP Persistance'],sort = False)['IP Persistance'].count().unstack('IP Persistance').fillna(0)

        df = pd.DataFrame(stats)
        df = df.set_index('Port')

        plot_stackBarPlot(df, str(pathlib.Path(processed_data_dir,'IP_persistance_PORT_count.png')),
                        "IP Persistance","Port", "Count of IPs",
                        legendPosition_outside = True,display_values=False)
        if percentage_flag:
            df = df.transform(lambda x: (x/x.sum())*100, axis=1)
        plot_stackBarPlot(df, str(pathlib.Path(processed_data_dir,'IP_persistance_PORT_percentage.png')),
                        "IP Persistance","Port", "Percentage of IPs",
                        legendPosition_outside = True,display_values=True)
    except Exception as e: 
        print("Error", repr(e))



def plot_protocol_stability_all_scan(
                        dataFrame,
                        processed_data_dir):
    print("plot_protocol_stability_all_scan")
    try:
        scan_code_list = dataFrame.sort_values(by = ['scan_code'],ascending=True)['scan_code'].unique().tolist()
        protocol_port_list = dataFrame[(dataFrame['port'].astype(str) != '5684') & (dataFrame['port'].astype(str) != '5683')].sort_values(by = ['Protocol, Port'],ascending=True)['Protocol, Port'].unique().tolist()
        dataFrame = dataFrame[dataFrame['status'] == 'success']
        scan_ref = dataFrame[(dataFrame['scan_code'] == scan_code_list[0])]
        scan_code_list.remove(scan_code_list[0])
        bar_width = 0.8/len(scan_code_list)
        plt.rcParams["figure.figsize"] = [5.8,3]#[4.8,2]
        fig, ax_ = plt.subplots(1, 2,sharey=True,gridspec_kw={'width_ratios': [9,1]})
        fig.tight_layout(w_pad=0.5)
        ax = ax_[0]
        i = 0
        for scan_code in scan_code_list:
            x_cordinates = np.arange(0.1+(bar_width*i)+bar_width/2, len(protocol_port_list)+0.1+(bar_width*i),1)
            i +=1
            scan_cur = dataFrame[(dataFrame['scan_code'] == scan_code)]
            only_ref_ip_count_list = []
            matched_ip_count_list = []
            only_cur_ip_list = []
            # print(scan_code)
            for protocol_port in protocol_port_list:
                scan_ref_ip_list = set(scan_ref[scan_ref['Protocol, Port']==protocol_port]['ip'].unique().tolist())
                scan_cur_ip_list = set(scan_cur[scan_cur['Protocol, Port']==protocol_port]['ip'].unique().tolist())
                only_ref_ip_count = len(scan_ref_ip_list - scan_cur_ip_list) 
                matched_ip_count = len(scan_ref_ip_list.intersection(scan_cur_ip_list))
                only_cur_ip = len(scan_cur_ip_list - scan_ref_ip_list)
                total_ip = only_ref_ip_count + matched_ip_count + only_cur_ip
                if total_ip:
                    only_ref_ip_count = (only_ref_ip_count*100)/total_ip
                    matched_ip_count = (matched_ip_count*100)/total_ip
                    only_cur_ip = (only_cur_ip*100)/total_ip
                
                only_ref_ip_count_list.append(only_ref_ip_count)
                only_cur_ip_list.append(only_cur_ip)
                matched_ip_count_list.append(matched_ip_count)
                # print(protocol_port)
                # # df = scan_ref[scan_ref['Protocol, Port']==protocol_port]
                # # df = df[df['ip'].isin(list(scan_ref_ip_list.intersection(scan_cur_ip_list)))].groupby(['AS'],sort=True)['ip'].count().reset_index()
                # # df.set_index('AS')
                # # df = df.sort_values(by='ip',ascending=False)
                # # tt = df['ip'].sum()
                # # df.loc[:, 'ip'] = df.ip.apply(lambda x: x*100/tt)
                # # print("matched",df.head())
                # df = scan_ref[scan_ref['Protocol, Port']==protocol_port]
                # df = df[df['ip'].isin(list(scan_ref_ip_list - scan_cur_ip_list))].groupby(['AS'],sort=True)['ip'].count().reset_index()
                # df.set_index('AS')
                # df = df.sort_values(by='ip',ascending=False)
                # tt = df['ip'].sum()
                # df.loc[:, 'ip'] = df.ip.apply(lambda x: x*100/tt)
                # print("ref",df.head(n=3))
                # df = scan_cur[scan_cur['Protocol, Port']==protocol_port]
                # df = df[df['ip'].isin(list(scan_cur_ip_list - scan_ref_ip_list))].groupby(['AS'],sort=True)['ip'].count().reset_index()
                # df.set_index('AS')
                # df = df.sort_values(by='ip',ascending=False)
                # tt = df['ip'].sum()
                # df.loc[:, 'ip'] = df.ip.apply(lambda x: x*100/tt)
                # print("cur",df.head(n=3))
                # df.to_csv(str(processed_data_dir.resolve())+"/"+"AS_ref"+str(scan_code)+protocol_port+".csv", sep='\t', index=False)
                print(protocol_port)
                # df = scan_ref[scan_ref['Protocol, Port']==protocol_port]
                # df = df[df['ip'].isin(list(scan_ref_ip_list.intersection(scan_cur_ip_list)))].groupby(['AS'],sort=True)['ip'].count().reset_index()
                # df.set_index('AS')
                # df = df.sort_values(by='ip',ascending=False)
                # tt = df['ip'].sum()
                # df.loc[:, 'ip'] = df.ip.apply(lambda x: x*100/tt)
                # print("matched",df.head())
                df = scan_ref[scan_ref['Protocol, Port']==protocol_port]
                df_val = set(df[(df['ip'].isin(list(scan_ref_ip_list - scan_cur_ip_list)))& (df['tls'].astype(str) == '1')]['fingerprint_sha256'].unique().tolist())
                df = scan_cur[scan_cur['Protocol, Port']==protocol_port]
                df_val1 = set(df[(df['ip'].isin(list(scan_cur_ip_list - scan_ref_ip_list)))& (df['tls'].astype(str) == '1')]['fingerprint_sha256'].unique().tolist())
                print(len(list(df_val.intersection(df_val1))))


            only_ref_ip_count_legend = ''
            matched_ip_count_legend = ''
            only_cur_ip_legend = ''
            if i == len(scan_code_list):
                only_ref_ip_count_legend = 'Reference Scan IP addresses only'
                matched_ip_count_legend = 'Current and Reference Scan IP addresses only'
                only_cur_ip_legend = 'Current Scan IP addresses only'
            ax.bar(x_cordinates, np.array(only_ref_ip_count_list), width=bar_width,edgecolor = '#595959',alpha=colour_alpha,
                                    color = '#ABABAB',linewidth=0.001,label=only_ref_ip_count_legend)
            ax.bar(x_cordinates, np.array(only_cur_ip_list), bottom=np.array(only_ref_ip_count_list), width=bar_width,edgecolor = '#595959',
                                    alpha=colour_alpha,color = '#FF800E',linewidth=0.001,label=only_cur_ip_legend)
            ax.bar(x_cordinates, np.array(matched_ip_count_list), bottom=np.add(np.array(only_cur_ip_list),np.array(only_ref_ip_count_list)), 
                                    width=bar_width,edgecolor = '#595959',alpha=colour_alpha,
                                    color = '#006BA4',linewidth=0.001,label=matched_ip_count_legend)

            for j in range(len(x_cordinates)):
                ax.text(x_cordinates[j], 110, scan_code, ha='center', va='center', color='black', alpha = 1,rotation=90,size=5)
        ax.set_axisbelow(True)
        ax.yaxis.grid(True, color='#EEEEEE')
        ax.xaxis.grid(False)
        ax.spines['top'].set_visible(False)
        ax.spines['right'].set_visible(False)
        ax.set_xticks(np.arange(0,len(protocol_port_list)+1,1))
        ax.set_xticklabels([])
        # ax.set_xticks(np.arange(0.5,len(protocol_port_list),1), minor=True)
        # ax.set_xticklabels(protocol_port_list,rotation=90, minor=True,fontsize = 6)
        xticks_loc_list = np.arange(0.5,len(protocol_port_list),1)
        for i in range(len(protocol_port_list)):
            ax.text(xticks_loc_list[i], -1.5, protocol_port_list[i], ha='center', va='top', color='black', alpha = 1,
                    size=6,rotation=90)

        # ax.legend(title = "",loc='upper left', bbox_to_anchor=(-0.15, -0.5),title_fontsize=7,fontsize = 6,ncol=4)
        ax.legend(title = "",loc='upper left', bbox_to_anchor=(-0.11, 1.42),title_fontsize=7,fontsize = 6,ncol=4,frameon=False)
        ax.set_xlabel('Protocol, Port', labelpad=45, fontsize = 7)
        ax.tick_params(axis = 'both', which = 'major', labelsize = 6)
        ax.set_ylabel("No. of IP Addresses(%)", labelpad=5, fontsize = 7)
        ax.set_title('Reference Scan\n(2022-01-14)',fontsize=7,pad=40)

        dataFrame = dataFrame[(dataFrame['port'].astype(str) == '5684') | (dataFrame['port'].astype(str) == '5683')]
        protocol_port_list = dataFrame.sort_values(by = ['Protocol, Port'],ascending=True)['Protocol, Port'].unique().tolist()
        scan_code_list = dataFrame.sort_values(by = ['scan_code'],ascending=True)['scan_code'].unique().tolist()
        dataFrame = dataFrame[dataFrame['status'] == 'success']
        scan_ref = dataFrame[(dataFrame['scan_code'] == scan_code_list[0])]
        scan_code_list.remove(scan_code_list[0])

        i = 0
        ax = ax_[1]
        for scan_code in scan_code_list:
            x_cordinates = np.arange(0.1+(bar_width*i)+bar_width/2, (0.2+(bar_width*len(scan_code_list)))*len(protocol_port_list),0.2+(bar_width*len(scan_code_list)))
            i +=1
            scan_cur = dataFrame[(dataFrame['scan_code'] == scan_code)]
            only_ref_ip_count_list = []
            matched_ip_count_list = []
            only_cur_ip_list = []
            # print(scan_code)
            for protocol_port in protocol_port_list:
                scan_ref_ip_list = set(scan_ref[scan_ref['Protocol, Port']==protocol_port]['ip'].unique().tolist())
                scan_cur_ip_list = set(scan_cur[scan_cur['Protocol, Port']==protocol_port]['ip'].unique().tolist())
                only_ref_ip_count = len(scan_ref_ip_list - scan_cur_ip_list) 
                matched_ip_count = len(scan_ref_ip_list.intersection(scan_cur_ip_list))
                only_cur_ip = len(scan_cur_ip_list - scan_ref_ip_list)
                total_ip = only_ref_ip_count + matched_ip_count + only_cur_ip
                if total_ip:
                    only_ref_ip_count = (only_ref_ip_count*100)/total_ip
                    matched_ip_count = (matched_ip_count*100)/total_ip
                    only_cur_ip = (only_cur_ip*100)/total_ip
                
                only_ref_ip_count_list.append(only_ref_ip_count)
                only_cur_ip_list.append(only_cur_ip)
                matched_ip_count_list.append(matched_ip_count)
                # print(protocol_port)
                # # df = scan_ref[scan_ref['Protocol, Port']==protocol_port]
                # # df = df[df['ip'].isin(list(scan_ref_ip_list.intersection(scan_cur_ip_list)))].groupby(['AS'],sort=True)['ip'].count().reset_index()
                # # df.set_index('AS')
                # # df = df.sort_values(by='ip',ascending=False)
                # # tt = df['ip'].sum()
                # # df.loc[:, 'ip'] = df.ip.apply(lambda x: x*100/tt)
                # # print("matched",df.head())
                # df = scan_ref[scan_ref['Protocol, Port']==protocol_port]
                # df = df[df['ip'].isin(list(scan_ref_ip_list - scan_cur_ip_list))].groupby(['AS'],sort=True)['ip'].count().reset_index()
                # df.set_index('AS')
                # df = df.sort_values(by='ip',ascending=False)
                # tt = df['ip'].sum()
                # df.loc[:, 'ip'] = df.ip.apply(lambda x: x*100/tt)
                # print("ref",df.head())
                # df = scan_cur[scan_cur['Protocol, Port']==protocol_port]
                # df = df[df['ip'].isin(list(scan_cur_ip_list - scan_ref_ip_list))].groupby(['AS'],sort=True)['ip'].count().reset_index()
                # df.set_index('AS')
                # df = df.sort_values(by='ip',ascending=False)
                # tt = df['ip'].sum()
                # df.loc[:, 'ip'] = df.ip.apply(lambda x: x*100/tt)
                # print("cur",df.head())
                # df.to_csv(str(processed_data_dir.resolve())+"/"+"AS_ref"+scan_code+protocol_port+".csv", sep='\t', index=False)
                
            only_ref_ip_count_legend = ''
            matched_ip_count_legend = ''
            only_cur_ip_legend = ''

            ax.bar(x_cordinates, np.array(only_ref_ip_count_list), width=bar_width,edgecolor = '#595959',alpha=colour_alpha,
                                    color = '#ABABAB',linewidth=0.001,label=only_ref_ip_count_legend)
            ax.bar(x_cordinates, np.array(only_cur_ip_list), bottom=np.array(only_ref_ip_count_list), width=bar_width,edgecolor = '#595959',
                                    alpha=colour_alpha,color = '#FF800E',linewidth=0.001,label=only_cur_ip_legend)
            ax.bar(x_cordinates, np.array(matched_ip_count_list), bottom=np.add(np.array(only_cur_ip_list),np.array(only_ref_ip_count_list)), 
                                    width=bar_width,edgecolor = '#595959',alpha=colour_alpha,
                                    color = '#006BA4',linewidth=0.001,label=matched_ip_count_legend)

            for j in range(len(x_cordinates)):
                ax.text(x_cordinates[j], 110, scan_code, ha='center', va='center', color='black', alpha = 1,rotation=90,size=5)

        ax.set_axisbelow(True)
        ax.yaxis.grid(True, color='#EEEEEE')
        ax.xaxis.grid(False)
        ax.spines['top'].set_visible(False)
        ax.spines['right'].set_visible(False)
        ax.spines['left'].set_visible(False)
        ax.set_xticks(np.arange(0,(bar_width*len(scan_code_list)+0.2)*len(protocol_port_list)+0.11,bar_width*len(scan_code_list)+0.2))
        ax.set_xticklabels([])
        # ax.set_xticks(np.arange(0.5,len(protocol_port_list),1), minor=True)
        # ax.set_xticklabels(protocol_port_list,rotation=90, minor=True,fontsize = 6)
        xticks_loc_list = np.arange((bar_width*len(scan_code_list)+0.2)/2,(bar_width*len(scan_code_list)+0.2)*len(protocol_port_list),bar_width*len(scan_code_list)+0.2)
        for i in range(len(protocol_port_list)):
            ax.text(xticks_loc_list[i], -1.5, protocol_port_list[i], ha='center', va='top', color='black', alpha = 1,
                    size=6,rotation=90)
        ax.set_xlabel('Protocol, Port', labelpad=45, fontsize = 7)
        ax.tick_params(axis = 'x', which = 'major', labelsize = 6)
        ax.tick_params(axis='y', which='both', length=0)
        ax.set_title('Reference Scan\n(2022-06-29)',fontsize=7,pad=40)

        plt.savefig(str(pathlib.Path(processed_data_dir,'IP_persistance_ProtocolPort_all.pdf')), bbox_inches = 'tight', pad_inches = 0.1,dpi = 600)
    except Exception as e: 
        print("Error", repr(e))



def plot_port_stability_all_scan(
                        dataFrame,
                        processed_data_dir):
    print("plot_port_stability_all_scan")
    try:
        scan_code_list = dataFrame.sort_values(by = ['scan_code'],ascending=True)['scan_code'].unique().tolist()
        dataFrame = dataFrame.sort_values(by = ['Protocol, Port'],ascending=True)
        port_list = dataFrame['port'].unique().tolist()
        scan_ref = dataFrame[(dataFrame['scan_code'] == scan_code_list[0])]
        scan_code_list.remove(scan_code_list[0])
        bar_width = 0.8/len(scan_code_list)
        plt.rcParams["figure.figsize"] = [5.8,3]#[4.8,2]
        fig, ax = plt.subplots(1, 1)
        i = 0
        for scan_code in scan_code_list:
            x_cordinates = np.arange(0.1+(bar_width*i)+bar_width/2, len(port_list)+0.1+(bar_width*i),1)
            i +=1
            scan_cur = dataFrame[(dataFrame['scan_code'] == scan_code)]
            only_ref_ip_count_list = []
            matched_ip_count_list = []
            only_cur_ip_list = []
            for port_ in port_list:
                scan_ref_ip_list = set(scan_ref[scan_ref['port']==port_]['ip'].unique().tolist())
                scan_cur_ip_list = set(scan_cur[scan_cur['port']==port_]['ip'].unique().tolist())
                only_ref_ip_count = len(scan_ref_ip_list - scan_cur_ip_list) 
                matched_ip_count = len(scan_ref_ip_list.intersection(scan_cur_ip_list))
                only_cur_ip = len(scan_cur_ip_list - scan_ref_ip_list)
                total_ip = only_ref_ip_count + matched_ip_count + only_cur_ip
                if total_ip:
                    only_ref_ip_count = (only_ref_ip_count*100)/total_ip
                    matched_ip_count = (matched_ip_count*100)/total_ip
                    only_cur_ip = (only_cur_ip*100)/total_ip
                
                only_ref_ip_count_list.append(only_ref_ip_count)
                only_cur_ip_list.append(only_cur_ip)
                matched_ip_count_list.append(matched_ip_count)
            only_ref_ip_count_legend = ''
            matched_ip_count_legend = ''
            only_cur_ip_legend = ''
            if i == len(scan_code_list):
                only_ref_ip_count_legend = 'Reference Scan (2022.01.14) IP only'
                matched_ip_count_legend = 'Current and Reference Scan IP only'
                only_cur_ip_legend = 'Current Scan IP only'
            plt.bar(x_cordinates, np.array(only_ref_ip_count_list), width=bar_width,edgecolor = '#595959',alpha=colour_alpha,
                                    color = '#ABABAB',linewidth=0.001,label=only_ref_ip_count_legend)
            plt.bar(x_cordinates, np.array(only_cur_ip_list), bottom=np.array(only_ref_ip_count_list), width=bar_width,edgecolor = '#595959',
                                    alpha=colour_alpha,color = '#FF800E',linewidth=0.001,label=only_cur_ip_legend)
            plt.bar(x_cordinates, np.array(matched_ip_count_list), bottom=np.add(np.array(only_cur_ip_list),np.array(only_ref_ip_count_list)), 
                                    width=bar_width,edgecolor = '#595959',alpha=colour_alpha,
                                    color = '#006BA4',linewidth=0.001,label=matched_ip_count_legend)

            for j in range(len(x_cordinates)):
                ax.text(x_cordinates[j], 110, scan_code, ha='center', va='center', color='black', alpha = 1,rotation=90,size=5)
        ax.set_axisbelow(True)
        ax.yaxis.grid(True, color='#EEEEEE')
        ax.xaxis.grid(False)
        ax.spines['top'].set_visible(False)
        ax.spines['right'].set_visible(False)
        ax.set_xticks(np.arange(0,len(port_list)+1,1))
        ax.set_xticklabels([])
        # ax.set_xticks(np.arange(0.5,len(protocol_port_list),1), minor=True)
        # ax.set_xticklabels(protocol_port_list,rotation=90, minor=True,fontsize = 6)
        xticks_loc_list = np.arange(0.5,len(port_list),1)
        for i in range(len(port_list)):
            ax.text(xticks_loc_list[i], -1.5, port_list[i], ha='center', va='top', color='black', alpha = 1,
                    size=6,rotation=90)
        # ax.set_xticks(np.arange(0.5,len(port_list),1))
        # ax.set_xticklabels(port_list,rotation=90)
        # ax.legend(title = "",loc='upper left', bbox_to_anchor=(-0.15, -0.5),title_fontsize=7,fontsize = 6,ncol=4)
        ax.legend(title = "",loc='upper left', bbox_to_anchor=(-0.15, 1.3),title_fontsize=7,fontsize = 6,ncol=4)
        ax.set_xlabel('Port', labelpad=15, fontsize = 7)
        ax.tick_params(axis = 'both', which = 'major', labelsize = 6)
        ax.set_ylabel("No. of IP Addresses(%)", labelpad=5, fontsize = 7)
        plt.savefig(str(pathlib.Path(processed_data_dir,'IP_persistance_Port_all.pdf')), bbox_inches = 'tight', pad_inches = 0.1,dpi = 600)
    except Exception as e: 
        print("Error", repr(e))


def is_present_64(row, scan_data):
    ip2 = row['ip']
    as2 = row['ipv6_64']
    if ip2 in scan_data['ip'].tolist():
        sel = scan_data.loc[scan_data['ip'] == ip2]
        if as2 == (sel['ipv6_64'].values)[0]:
            return "IP and IP subnet 64 match"
        else:
            return "IP match"
    return "No match"

def plot_protocol_stability_count_subnet64(
                        dataFrame,
                        processed_data_dir,
                        percentage_flag = True):
    print("plot_protocol_stability_count_subent64")
    try:
        scan_code_list = dataFrame.sort_values(by = ['scan_code'],ascending=True)['scan_code'].unique().tolist()
        scan1 = dataFrame[(dataFrame['scan_code'] == scan_code_list[0]) & (dataFrame['status'] == 'success')]
        scan2 = dataFrame[(dataFrame['scan_code'] == scan_code_list[1]) & (dataFrame['status'] == 'success')]
        protocol_list = dataFrame['protocol'].unique().tolist()
        stats = []
        for protocol in protocol_list:
            port_list = dataFrame[dataFrame['protocol']==protocol]['port'].unique().tolist()
            for port in port_list:
                scan1_subset = scan1[scan1['port']==port]
                scan2_subset = scan2[scan2['port']==port]
                old_ip_count = 0
                ip_match_count = 0
                ip_ip_subnet_64_match_count = 0
                new_ip = 0
                if not scan1_subset.empty:
                    scan1_subset['IP Persistance'] = scan1_subset.apply(is_present_64,scan_data = scan2_subset,axis=1)
                    old_ip_count = scan1_subset[scan1_subset['IP Persistance'] =='No match'].shape[0]
                if not scan2_subset.empty:
                    scan2_subset['IP Persistance'] = scan2_subset.apply(is_present_64,scan_data = scan1_subset,axis=1)
                    ip_match_count = scan2_subset[scan2_subset['IP Persistance'] == 'IP match'].shape[0]
                    ip_ip_subnet_64_match_count = scan2_subset[scan2_subset['IP Persistance'] == 'IP and IP subnet 64 match'].shape[0]
                    new_ip = scan2_subset[scan2_subset['IP Persistance'] == 'No match'].shape[0]
                stats.append({'Port, Protocol': "("+str(port) +", " +protocol+")",
                'Old IPs' : old_ip_count,
                'IP Match only' : ip_match_count,
                'IP and IP subnet 64 match': ip_ip_subnet_64_match_count,
                'New IPs' : new_ip,
                })
        # df = scan2['portocol','port','IP Persistance']
        # df = df.groupby(['portocol','port','IP Persistance'],sort = False)['IP Persistance'].count().unstack('IP Persistance').fillna(0)

        df = pd.DataFrame(stats)
        df = df.set_index('Port, Protocol')

        plot_stackBarPlot(df, str(pathlib.Path(processed_data_dir,'IP_persistance_Protocol_count_64.png')),
                        "IP Persistance","Port, Protocol", "Count of IPs",
                        legendPosition_outside = True,display_values=False)
        if percentage_flag:
            df = df.transform(lambda x: (x/x.sum())*100, axis=1)
        plot_stackBarPlot(df, str(pathlib.Path(processed_data_dir,'IP_persistance_Protocol_percentage_64.png')),
                        "IP Persistance","Port, Protocol", "Percentage of IPs",
                        legendPosition_outside = True,display_values=True)
    except Exception as e: 
        print("Error", repr(e))

def plot_port_stability_count_subnet64(
                        dataFrame,
                        processed_data_dir,
                        percentage_flag = True):
    print("plot_port_stability_count_subnet64")
    try:
        # scan_code_list = dataFrame['scan_code'].unique().tolist()
        scan_code_list = dataFrame.sort_values(by = ['scan_code'], ascending=True)['scan_code'].unique().tolist()
        print(scan_code_list)
        
        scan1 = dataFrame[dataFrame['scan_code'] == scan_code_list[0]]
        scan2 = dataFrame[dataFrame['scan_code'] == scan_code_list[1]]
        stats = []
        port_list = dataFrame['port'].unique().tolist()
        for port in port_list:
            scan1_subset = scan1[scan1['port']==port]
            scan2_subset = scan2[scan2['port']==port]
            old_ip_count = 0
            ip_match_count = 0
            ip_ip_subnet_64_match_count = 0
            new_ip = 0
            if not scan1_subset.empty:
                scan1_subset['IP Persistance'] = scan1_subset.apply(is_present_64,scan_data = scan2_subset,axis=1)
                old_ip_count = scan1_subset[scan1_subset['IP Persistance'] =='No match'].shape[0]
            if not scan2_subset.empty:
                scan2_subset['IP Persistance'] = scan2_subset.apply(is_present_64,scan_data = scan1_subset,axis=1)
                ip_match_count = scan2_subset[scan2_subset['IP Persistance'] == 'IP match'].shape[0]
                ip_ip_subnet_64_match_count = scan2_subset[scan2_subset['IP Persistance'] == 'IP and IP subnet 64 match'].shape[0]
                new_ip = scan2_subset[scan2_subset['IP Persistance'] == 'No match'].shape[0]
            stats.append({'Port': str(port),
            'Old IPs' : old_ip_count,
            'IP Match only' : ip_match_count,
            'IP and IP subnet 64 match': ip_ip_subnet_64_match_count,
            'New IPs' : new_ip,
            })
        # df = scan2['portocol','port','IP Persistance']
        # df = df.groupby(['portocol','port','IP Persistance'],sort = False)['IP Persistance'].count().unstack('IP Persistance').fillna(0)

        df = pd.DataFrame(stats)
        df = df.set_index('Port')

        plot_stackBarPlot(df, str(pathlib.Path(processed_data_dir,'IP_persistance_PORT_count_64.png')),
                        "IP Persistance","Port", "Count of IPs",
                        legendPosition_outside = True,display_values=False)
        if percentage_flag:
            df = df.transform(lambda x: (x/x.sum())*100, axis=1)
        plot_stackBarPlot(df, str(pathlib.Path(processed_data_dir,'IP_persistance_PORT_percentage_64.png')),
                        "IP Persistance","Port", "Percentage of IPs",
                        legendPosition_outside = True,display_values=True)
    except Exception as e: 
        print("Error", repr(e))

####
# Function to plot all the correlations
####
def plot_correlations(dataFrame,processed_data_dir):
    # plot_correlations_line_port_count(dataFrame,processed_data_dir)
    plot_correlations_stack_port_count(dataFrame,processed_data_dir)
    plot_correlations_stack_port_protocol_count(dataFrame,processed_data_dir)


def plot_unique_certificates(dataFrame,processed_data_dir,plot_font_size=8):
    scan_code_list = dataFrame.sort_values(by = ['scan_code'],ascending=True)['scan_code'].unique().tolist()

    # for scan_code in scan_code_list:
    #     print(scan_code)
    #     df = dataFrame[(dataFrame['scan_code'] == scan_code) & (dataFrame['tls'].astype(str) == '1') & (dataFrame['status'] == 'success')]
    #     print(df.shape[0])
    #     plotData = df.groupby(['certificate_raw','port','protocol'],sort=False)['certificate_raw'].count().unstack('certificate_raw').fillna(0)
    #     print(plotData)

    # df = dataFrame[(dataFrame['tls'].astype(str) == '1') & (dataFrame['status'] == 'success')]
    # print(df.shape[0])
    # plotData = df.groupby(['certificate_raw','port','protocol'],sort=False)['certificate_raw'].count().unstack('certificate_raw').fillna(0)
    # print(plotData)

    print("cert raw")
    raw_cert_dic = dict()
    total_value_list = []
    for scan_code in scan_code_list:
        df = dataFrame[(dataFrame['scan_code'] == scan_code) & (dataFrame['tls'].astype(str) == '1') & (dataFrame['status'] == 'success')]
        total_size = df.shape[0]
        unique_size = 100* df['certificate_raw'].unique().shape[0] / total_size
        duplicate_size = (100 - unique_size)
        total_value_list.append(total_size)
        raw_cert_dic[scan_code] = {"Unique certificates":unique_size,"Duplicate certificates":duplicate_size}
        # plotData = df.groupby(['certificate_raw','port','protocol'],sort=False)['certificate_raw'].unique().unstack('certificate_raw').fillna(0)
        # print(plotData)
    raw_cert_df =  pd.DataFrame.from_dict(raw_cert_dic,orient='index')
    plot_stackBarPlot(raw_cert_df,str(pathlib.Path(processed_data_dir,'raw_certificate_details.pdf')),
                    "","Scan Code","Count of certificates",
                    legendPosition_outside= True, yticks_in_Kilo=False,display_values=True, display_values_Float=True, 
                    figsize=[2,3], total_value_list=total_value_list,plot_font_size=plot_font_size)

    # print("total")
    # df = dataFrame[(dataFrame['tls'].astype(str) == '1') & (dataFrame['status'] == 'success')]
    # print(df.shape[0])
    # print(df['certificate_raw'].unique().shape[0])
    
    print("fingerprint")
    fingureprint_cert_dic = dict()
    total_value_list = []
    for scan_code in scan_code_list:
        df = dataFrame[(dataFrame['scan_code'] == scan_code) & (dataFrame['tls'].astype(str) == '1') & (dataFrame['status'] == 'success')]
        # print(df.shape[0])
        # print(df['fingerprint_sha256'].unique().shape[0])
        total_size = df.shape[0]
        unique_size = 100*df['fingerprint_sha256'].unique().shape[0] / total_size
        duplicate_size = 100 - unique_size
        total_value_list.append(total_size)

        fingureprint_cert_dic[scan_code] = {"Unique certificates fingureprints":unique_size,"Duplicate certificates fingureprints":duplicate_size}
    fingureprint_cert_df =  pd.DataFrame.from_dict(fingureprint_cert_dic,orient='index')
    plot_stackBarPlot(fingureprint_cert_df,str(pathlib.Path(processed_data_dir,'certificate_fingureprint_details.pdf')),
                "","Scan Code","Count of certificates",legend_title="Certificate Fingure Print",
                legendPosition_outside= True, yticks_in_Kilo=False,display_values=True, display_values_Float=True, 
                figsize=[2,3],total_value_list=total_value_list,plot_font_size=plot_font_size)

    # print("total")
    # df = dataFrame[(dataFrame['tls'].astype(str) == '1') & (dataFrame['status'] == 'success')]
    # print(df.shape[0])
    # print(df['fingerprint_sha256'].unique().shape[0])

def plot_unique_certificates1(dataFrame,processed_data_dir,plot_font_size=8):
    scan_code_list = dataFrame.sort_values(by = ['scan_code'],ascending=True)['scan_code'].unique().tolist()  
    fingureprint_cert_dic = dict()
    total_value_list = []
    for scan_code in scan_code_list:
        df = dataFrame[(dataFrame['scan_code'] == scan_code) & (dataFrame['tls'].astype(str) == '1') & (dataFrame['status'] == 'success')]
        # print(df.shape[0])
        # print(df['fingerprint_sha256'].unique().shape[0])
        total_size = 0
        protocol_port_list = df.sort_values(by = ['Protocol, Port'],ascending=True)['Protocol, Port'].unique().tolist()
        for protocol_port in protocol_port_list:
            total_size += len(set(df[df['Protocol, Port']==protocol_port]['fingerprint_sha256'].unique().tolist()))

        unique_size = 100*df['fingerprint_sha256'].unique().shape[0] / total_size
        duplicate_size = 100 - unique_size
        total_value_list.append(total_size)

        fingureprint_cert_dic[scan_code] = {"Unique certificates fingureprints":unique_size,"Duplicate certificates fingureprints":duplicate_size}
    fingureprint_cert_df =  pd.DataFrame.from_dict(fingureprint_cert_dic,orient='index')
    plot_stackBarPlot(fingureprint_cert_df,str(pathlib.Path(processed_data_dir,'certificate_fingureprint_details.pdf')),
                "","Scan Code","Count of certificates",legend_title="Certificate Fingure Print",
                legendPosition_outside= True, yticks_in_Kilo=False,display_values=True, display_values_Float=True, 
                figsize=[2,3],total_value_list=total_value_list,plot_font_size=plot_font_size)


def plot_certificate_stability_all_scan(
                        dataFrame,
                        processed_data_dir):
    print("plot_certificate_stability_all_scan")
    try:
        scan_code_list = dataFrame.sort_values(by = ['scan_code'],ascending=True)['scan_code'].unique().tolist()
        dataFrame = dataFrame[(dataFrame['status'] == 'success')& (dataFrame['tls'].astype(str) == '1')]
        protocol_port_list = dataFrame.sort_values(by = ['Protocol, Port'],ascending=True)['Protocol, Port'].unique().tolist()
        scan_ref = dataFrame[(dataFrame['scan_code'] == scan_code_list[0])]
        scan_code_list.remove(scan_code_list[0])
        bar_width = 0.8/len(scan_code_list)
        plt.rcParams["figure.figsize"] = [2.2,3]#[4.8,2]
        fig, ax = plt.subplots(1, 1)
        i = 0
        for scan_code in scan_code_list:
            x_cordinates = np.arange(0.1+(bar_width*i)+bar_width/2, len(protocol_port_list)+0.1+(bar_width*i),1)
            i +=1
            scan_cur = dataFrame[(dataFrame['scan_code'] == scan_code)]
            only_ref_cert_count_list = []
            matched_cert_count_list = []
            only_cur_cert_list = []
            # print(scan_code)
            for protocol_port in protocol_port_list:
                scan_ref_cert_list = set(scan_ref[scan_ref['Protocol, Port']==protocol_port]['fingerprint_sha256'].unique().tolist())
                scan_cur_cert_list = set(scan_cur[scan_cur['Protocol, Port']==protocol_port]['fingerprint_sha256'].unique().tolist())
                only_ref_cert_count = len(scan_ref_cert_list - scan_cur_cert_list) 
                matched_cert_count = len(scan_ref_cert_list.intersection(scan_cur_cert_list))
                only_cur_cert = len(scan_cur_cert_list - scan_ref_cert_list)
                total_cert = only_ref_cert_count + matched_cert_count + only_cur_cert
                if total_cert:
                    only_ref_cert_count = (only_ref_cert_count*100)/total_cert
                    matched_cert_count = (matched_cert_count*100)/total_cert
                    only_cur_cert = (only_cur_cert*100)/total_cert
                
                only_ref_cert_count_list.append(only_ref_cert_count)
                only_cur_cert_list.append(only_cur_cert)
                matched_cert_count_list.append(matched_cert_count)
               
            only_ref_cert_count_legend = ''
            matched_cert_count_legend = ''
            only_cur_cert_legend = ''
            if i == len(scan_code_list):
                only_ref_cert_count_legend = 'Reference Scan (2022.01.14) Certificate only'
                matched_cert_count_legend = 'Current and Reference Scan Certificate only'
                only_cur_cert_legend = 'Current Scan Certificate only'
            plt.bar(x_cordinates, np.array(only_ref_cert_count_list), width=bar_width,edgecolor = '#595959',alpha=colour_alpha,
                                    color = '#ABABAB',linewidth=0.001,label=only_ref_cert_count_legend)
            plt.bar(x_cordinates, np.array(only_cur_cert_list), bottom=np.array(only_ref_cert_count_list), width=bar_width,edgecolor = '#595959',
                                    alpha=colour_alpha,color = '#FF800E',linewidth=0.001,label=only_cur_cert_legend)
            plt.bar(x_cordinates, np.array(matched_cert_count_list), bottom=np.add(np.array(only_cur_cert_list),np.array(only_ref_cert_count_list)), 
                                    width=bar_width,edgecolor = '#595959',alpha=colour_alpha,
                                    color = '#006BA4',linewidth=0.001,label=matched_cert_count_legend)

            for j in range(len(x_cordinates)):
                ax.text(x_cordinates[j], 110, scan_code, ha='center', va='center', color='black', alpha = 1,rotation=90,size=5)
        ax.set_axisbelow(True)
        ax.yaxis.grid(True, color='#EEEEEE')
        ax.xaxis.grid(False)
        ax.spines['top'].set_visible(False)
        ax.spines['right'].set_visible(False)
        ax.set_xticks(np.arange(0.5,len(protocol_port_list),1))
        ax.set_xticklabels(protocol_port_list,rotation=90)
        # ax.legend(title = "",loc='upper left', bbox_to_anchor=(-0.15, -0.5),title_fontsize=7,fontsize = 6,ncol=4)
        ax.legend(title = "",loc='center left', bbox_to_anchor=(1, 0.5),title_fontsize=7,fontsize = 6)
        ax.set_xlabel('Protocol, Port', labelpad=5, fontsize = 7)
        ax.tick_params(axis = 'both', which = 'major', labelsize = 6)
        ax.set_ylabel("No. of IP Addresses(%)", labelpad=5, fontsize = 7)
        plt.savefig(str(pathlib.Path(processed_data_dir,'Certificate_persistance_ProtocolPort_all.pdf')), bbox_inches = 'tight', pad_inches = 0.1,dpi = 600)
    except Exception as e: 
        print("Error", repr(e))

def plot_certificate_validty_stability_all_scan(
                        dataFrame,
                        processed_data_dir):
    print("plot_certificate_stability_all_scan")
    try:
        dataFrame = dataFrame[dataFrame['port'].astype(str) != '4843']
        scan_code_list = dataFrame.sort_values(by = ['scan_code'],ascending=True)['scan_code'].unique().tolist()
        dataFrame = dataFrame[(dataFrame['status'] == 'success')& (dataFrame['tls'].astype(str) == '1')]
        protocol_port_list = dataFrame.sort_values(by = ['Protocol, Port'],ascending=True)['Protocol, Port'].unique().tolist()
        dataFrame['isValid'] = dataFrame.apply(check_certificate_validity,axis=1)
        # scan_code_list.remove(scan_code_list[0])
        bar_width = 0.8/len(scan_code_list)
        plt.rcParams["figure.figsize"] = [2.8,3]#[4.8,2]
        fig, ax = plt.subplots(1, 1)
        i = 0
        for scan_code in scan_code_list:
            x_cordinates = np.arange(0.1+(bar_width*i)+bar_width/2, len(protocol_port_list)+0.1+(bar_width*i),1)
            df = dataFrame[dataFrame['scan_code']==scan_code]
            i +=1
            valid_cert_count_list = []
            invalid_cert_count_list = []
            # print(scan_code)
            for protocol_port in protocol_port_list:
                valid_cert_count = df[(df['Protocol, Port']==protocol_port) & (df['isValid']=='Not Expired')].shape[0]
                invalid_cert_count = df[(df['Protocol, Port']==protocol_port) & (df['isValid']=='Expired')].shape[0]
                total_cert = valid_cert_count + invalid_cert_count
                if total_cert:
                    valid_cert_count = (valid_cert_count*100)/total_cert
                    invalid_cert_count = (invalid_cert_count*100)/total_cert
                
                valid_cert_count_list.append(valid_cert_count)
                invalid_cert_count_list.append(invalid_cert_count)
                
            valid_cert_legend = ''
            invalid_cert_legend = ''
            
            if i == len(scan_code_list):
                valid_cert_legend = 'Not expired'
                invalid_cert_legend = 'Expired'
            plt.bar(x_cordinates, np.array(valid_cert_count_list), width=bar_width,alpha=colour_alpha,color = '#006BA4',
                                    linewidth=0.001,edgecolor = '#595959',
                                    label=valid_cert_legend)
            plt.bar(x_cordinates, np.array(invalid_cert_count_list), bottom=np.array(valid_cert_count_list), 
                                    width=bar_width,alpha=colour_alpha,color = '#FF800E',
                                    linewidth=0.001,edgecolor = '#595959',
                                    label=invalid_cert_legend)

            for j in range(len(x_cordinates)):
                ax.text(x_cordinates[j], 114, scan_code, ha='center', va='center', color='black', alpha = 1,rotation=90,size=7)
        ax.set_axisbelow(True)
        ax.yaxis.grid(True, color='#EEEEEE')
        ax.xaxis.grid(False)
        ax.spines['top'].set_visible(False)
        ax.spines['right'].set_visible(False)
        # ax.set_xticks(np.arange(0.5,len(protocol_port_list),1))
        ax.set_xticks(np.arange(0,len(protocol_port_list)+1,1))
        # ax.set_xticklabels(protocol_port_list,rotation=90)
        # ax.legend(title = "",loc='upper left', bbox_to_anchor=(-0.15, -0.5),title_fontsize=7,fontsize = 6,ncol=4)
        ax.set_xticklabels([])
        xticks_loc_list = np.arange(0.5,len(protocol_port_list),1)
        for i in range(len(protocol_port_list)):
            ax.text(xticks_loc_list[i], -1.5, protocol_port_list[i], ha='center', va='top', color='black', alpha = 1,
                    size=6,rotation=90)
        ax.legend(title = "Certificate validity",loc='center left', bbox_to_anchor=(1, 0.5),title_fontsize=8,fontsize = 7,frameon=False)
        ax.set_xlabel('Protocol, Port', labelpad=45, fontsize = 8)
        ax.tick_params(axis = 'both', which = 'major', labelsize = 7)
        ax.set_ylabel("No. of Certificates (%)", labelpad=5, fontsize = 8)
        plt.savefig(str(pathlib.Path(processed_data_dir,'Certificate_validity_ProtocolPort_all.pdf')), bbox_inches = 'tight', pad_inches = 0.1,dpi = 600)
    except Exception as e: 
        print("Error", repr(e))

def plot_TLSversions_stability_all_scan(
                        dataFrame,
                        processed_data_dir):
    # print("plot_certificate_stability_all_scan")
    # try:
        dataFrame = dataFrame[dataFrame['port'].astype(str)!= '4843']
        scan_code_list = dataFrame.sort_values(by = ['scan_code'],ascending=True)['scan_code'].unique().tolist()
        dataFrame = dataFrame[(dataFrame['status'] == 'success')& (dataFrame['tls'].astype(str) == '1')]
        protocol_port_list = dataFrame.sort_values(by = ['Protocol, Port'],ascending=True)['Protocol, Port'].unique().tolist()
        dataFrame['isValid'] = dataFrame.apply(check_certificate_validity,axis=1)
        scan_code_list.remove(scan_code_list[0])
        scan_code_list.remove(scan_code_list[2])
        bar_width = 0.8/len(scan_code_list)
        plt.rcParams["figure.figsize"] = [1.5,3]#[4.8,2]
        fig, ax = plt.subplots(1, 1)
        i = 0
        TLSv_list = dataFrame.sort_values(by = ['serverTLS'],ascending=False)['serverTLS'].unique().tolist()
        for scan_code in scan_code_list:
            x_cordinates = np.arange(0.1+(bar_width*i)+bar_width/2, len(protocol_port_list)+0.1+(bar_width*i),1)
            df = dataFrame[dataFrame['scan_code']==scan_code]
            i +=1
            count_list = [[] for i in range(len(TLSv_list))]
            for protocol_port in protocol_port_list:
                # count = [[] for i in range(len(protocol_port_list))]
                # print(len(count))
                total = df[(df['Protocol, Port']==protocol_port)].shape[0]
                for v in range(len(TLSv_list)):
                    val = df[(df['Protocol, Port']==protocol_port) & (df['serverTLS']==TLSv_list[v])].shape[0]
                    if total != 0:
                        val = val*100/total
                    else:
                        val = 0
                    # count[p].append(val)
                    count_list[v].append(val)                
            legend_list = ['']*len(TLSv_list)
            if i == len(scan_code_list):
                legend_list = TLSv_list
            plt.bar(x_cordinates, np.array(count_list[0]), width=bar_width,edgecolor = '#595959',alpha=colour_alpha,
                                    color = '#006BA4',linewidth=0.001,label=legend_list[0])
            plt.bar(x_cordinates, np.array(count_list[1]), bottom=np.array(count_list[0]), 
                                    width=bar_width,edgecolor = '#595959',alpha=colour_alpha,
                                    color = '#FF800E',linewidth=0.001,label=legend_list[1])
            plt.bar(x_cordinates, np.array(count_list[2]), bottom=np.add(np.array(count_list[0]),np.array(count_list[1])), 
                                    width=bar_width,edgecolor = '#595959',alpha=colour_alpha,
                                    color = '#898989',linewidth=0.001,label=legend_list[2])
            plt.bar(x_cordinates, np.array(count_list[3]), bottom=np.add(np.add(np.array(count_list[0]),np.array(count_list[1])),np.array(count_list[2])), 
                                    width=bar_width,edgecolor = '#595959',alpha=colour_alpha,
                                    color = '#A2C8EC',linewidth=0.001,label=legend_list[3])

            for j in range(len(x_cordinates)):
                ax.text(x_cordinates[j], 114, scan_code, ha='center', va='center', color='black', alpha = 1,rotation=90,size=7)
        ax.set_axisbelow(True)
        ax.yaxis.grid(True, color='#EEEEEE')
        ax.xaxis.grid(False)
        ax.spines['top'].set_visible(False)
        ax.spines['right'].set_visible(False)
        # ax.set_xticks(np.arange(0.5,len(protocol_port_list),1))
        # ax.set_xticklabels(protocol_port_list,rotation=90)
        ax.set_xticks(np.arange(0,len(protocol_port_list)+1,1))
        ax.set_xticklabels([])
        xticks_loc_list = np.arange(0.5,len(protocol_port_list),1)
        for i in range(len(protocol_port_list)):
            ax.text(xticks_loc_list[i], -1.5, protocol_port_list[i], ha='center', va='top', color='black', alpha = 1,
                    size=6,rotation=90)
        # ax.legend(title = "",loc='upper left', bbox_to_anchor=(-0.15, -0.5),title_fontsize=7,fontsize = 6,ncol=4)
        ax.legend(title = "TLS version",loc='center left', bbox_to_anchor=(1, 0.5),title_fontsize=8,fontsize = 7,frameon=False)
        ax.set_xlabel('Protocol, Port', labelpad=45, fontsize = 8)
        ax.tick_params(axis = 'both', which = 'major', labelsize = 7)
        ax.set_ylabel("No. of IP addresses (%)", labelpad=5, fontsize = 8)
        plt.savefig(str(pathlib.Path(processed_data_dir,'TLSversion_ProtocolPort_all.pdf')), bbox_inches = 'tight', pad_inches = 0.1,dpi = 600)
    # except Exception as e: 
    #     print("Error", repr(e))

def get_TLSversions_upgrade(
                        dataFrame,
                        processed_data_dir):
    # print("plot_certificate_stability_all_scan")
    # try:
        scan_code_list = dataFrame.sort_values(by = ['scan_code'],ascending=True)['scan_code'].unique().tolist()
        dataFrame = dataFrame[(dataFrame['status'] == 'success')& (dataFrame['tls'].astype(str) == '1')]
        protocol_port_list = dataFrame.sort_values(by = ['Protocol, Port'],ascending=True)['Protocol, Port'].unique().tolist()
        dataFrame['isValid'] = dataFrame.apply(check_certificate_validity,axis=1)
        scan_code_list.remove(scan_code_list[0])
        scan_code_list.remove(scan_code_list[2])
        plt.rcParams["figure.figsize"] = [1.5,3]#[4.8,2]
        TLSv_list = dataFrame.sort_values(by = ['serverTLS'],ascending=False)['serverTLS'].unique().tolist()
        for protocol_port in protocol_port_list:
            ref = set(dataFrame[(dataFrame['Protocol, Port']==protocol_port) & (dataFrame['scan_code']==scan_code_list[0]) & (dataFrame['serverTLS']!='TLSv1.3')& (dataFrame['serverTLS']!='TLSv1.2')]['fingerprint_sha256'].unique().tolist())
            cur = set(dataFrame[(dataFrame['Protocol, Port']==protocol_port) & (dataFrame['scan_code']==scan_code_list[1]) & (dataFrame['serverTLS']!='TLSv1.3')& (dataFrame['serverTLS']=='TLSv1.2')]['fingerprint_sha256'].unique().tolist())
            print(ref.intersection(cur))
            df1 = dataFrame[(dataFrame['fingerprint_sha256'].isin(ref.intersection(cur)))&(dataFrame['scan_code']==scan_code_list[0]) & (dataFrame['serverTLS']!='TLSv1.3')& (dataFrame['serverTLS']!='TLSv1.2')]
            print(df1[['serverTLS']])
            # print(dataFrame[(dataFrame['fingerprint_sha256'].isin(ref.intersection(cur)))&(dataFrame['scan_code']==scan_code_list[1]) & (dataFrame['serverTLS']=='TLSv1.3')])
            # print(dataFrame[(dataFrame['fingerprint_sha256'].isin(ref.intersection(cur)))&(dataFrame['scan_code']==scan_code_list[0]) & (dataFrame['serverTLS']!='TLSv1.3')])
            print(protocol_port, len(ref.intersection(cur)))

def get_certificateUpgrade_info(
                        dataFrame,
                        processed_data_dir):
    print("get_certificateUpgrade_info")
    try:
        dataFrame = dataFrame[(dataFrame['status'] == 'success')& (dataFrame['tls'].astype(str) == '1')]
        scan_code_list = dataFrame.sort_values(by = ['scan_code'],ascending=True)['scan_code'].unique().tolist()
        dataFrame = dataFrame.sort_values(by = ['Protocol, Port'],ascending=True)
        dataFrame['validityEnd_YYYY'] = dataFrame.apply(extract_date_YYYY,axis=1)
        dataFrame['isValid'] = dataFrame.apply(check_certificate_validity,axis=1)
        protocol_port_list = dataFrame.sort_values(by = ['Protocol, Port'],ascending=True)['Protocol, Port'].unique().tolist()
        scan_ref = dataFrame[(dataFrame['scan_code'] == scan_code_list[0])]
        scan_code_list.remove(scan_code_list[0])
        bar_width = 0.8/len(scan_code_list)
        plt.rcParams["figure.figsize"] = [2.2,3]#[4.8,2]
        fig, ax = plt.subplots(1, 1)
        
        i = 0
        for scan_code in scan_code_list:
            x_cordinates = np.arange(0.1+(bar_width*i)+bar_width/2, len(protocol_port_list)+0.1+(bar_width*i),1)
            i +=1
            scan_cur = dataFrame[(dataFrame['scan_code'] == scan_code)]
            cert_count_list = []
            for protocol_port in protocol_port_list:
                scan_ref_ip_list = set(scan_ref[(scan_ref['Protocol, Port']==protocol_port) & (scan_ref['isValid'] =="Expired")]['ip'].unique().tolist())
                scan_cur_ip_list = set(scan_cur[(scan_cur['Protocol, Port']==protocol_port) &  (scan_cur['isValid'] =="Not Expired")]['ip'].unique().tolist())
                only_ref_ip_count = len(scan_ref_ip_list - scan_cur_ip_list) 
                matched_ip_count = len(scan_ref_ip_list.intersection(scan_cur_ip_list))
                only_cur_ip = len(scan_cur_ip_list - scan_ref_ip_list)
                # total_ip = only_ref_ip_count + matched_ip_count + only_cur_ip
                # if total_ip:
                #     only_ref_ip_count = (only_ref_ip_count*100)/total_ip
                #     matched_ip_count = (matched_ip_count*100)/total_ip
                #     only_cur_ip = (only_cur_ip*100)/total_ip
                print(scan_code,protocol_port,matched_ip_count)
                print(scan_ref_ip_list.intersection(scan_cur_ip_list))
                print(scan_ref[(scan_ref['Protocol, Port']==protocol_port) & (scan_ref['isValid'] =="Expired") & (scan_ref['ip'].isin(scan_ref_ip_list.intersection(scan_cur_ip_list)))][['fingerprint_sha256','validityStart','validityEnd','ip']])
                print(scan_cur[(scan_cur['Protocol, Port']==protocol_port) &  (scan_cur['isValid'] =="Not Expired")& (scan_cur['ip'].isin(scan_ref_ip_list.intersection(scan_cur_ip_list)))][['fingerprint_sha256','validityStart','validityEnd','ip']])
                print("\n\n")
                cert_count_list.append(matched_ip_count)
            plt.bar(x_cordinates, np.array(cert_count_list), width=bar_width,edgecolor = '#595959',alpha=colour_alpha,
                                    color = '#006BA4',linewidth=0.001)
            
            for j in range(len(x_cordinates)):
                ax.text(x_cordinates[j], 30, scan_code, ha='center', va='center', color='black', alpha = 1,rotation=90,size=5)
        ax.set_axisbelow(True)
        ax.yaxis.grid(True, color='#EEEEEE')
        ax.xaxis.grid(False)
        ax.spines['top'].set_visible(False)
        ax.spines['right'].set_visible(False)
        ax.set_xticks(np.arange(0.5,len(protocol_port_list),1))
        ax.set_xticklabels(protocol_port_list,rotation=90)
        # ax.legend(title = "",loc='upper left', bbox_to_anchor=(-0.15, -0.5),title_fontsize=7,fontsize = 6,ncol=4)
        ax.legend(title = "Certificate validity",loc='center left', bbox_to_anchor=(1, 0.5),title_fontsize=7,fontsize = 6)
        ax.set_xlabel('Protocol, Port', labelpad=5, fontsize = 7)
        ax.tick_params(axis = 'both', which = 'major', labelsize = 6)
        ax.set_ylabel("No. of Certificates (%)", labelpad=5, fontsize = 7)
        display_values_on_graph(ax,font_size=5,print_threshold=0,location='mid', round_position=0)
        plt.savefig(str(pathlib.Path(processed_data_dir,'Certificate_validityLost_IP_ProtocolPort_all.pdf')), bbox_inches = 'tight', pad_inches = 0.1,dpi = 600)
    except Exception as e: 
        print("Error", repr(e))


def create_dashboard_data(dataFrame,processed_data_dir): 
    protocol_list = dict()

    protocol_port_list = dataFrame.sort_values(by = ['Protocol, Port'],ascending=True)['Protocol, Port'].unique().tolist()
    dataFrame = dataFrame[dataFrame['status'] == 'success']
    protocol_list['scan code'] = dataFrame['scan_code'].unique().tolist()[0].strftime('%Y-%m-%d')
    for protocol_port in protocol_port_list:
        protocol_list[protocol_port] = dataFrame[dataFrame['Protocol, Port'] == protocol_port].shape[0]
    print(json.dumps(protocol_list))


def find_val(row):
    if row['error'].find('read: connection reset by peer') != -1:
        return 1 
    elif row['error'].find('i/o timeout') != -1:
        return 2
    elif row['error'].find('write: broken pipe') != -1:
        return 3
    elif row['error'].find('write: connection reset by peer') != -1:
        return 4
    elif row['error'].find('EOF') != -1:
        return 5
    else:
        return 6

####
# Main function
####
def main():
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
    # # plot
    if flag_O_arg:
        for arg in args.scan_output_dir_path:
            plot_hitrate(arg,processed_data_dir,arg.name)

    # port = 5683
    scan_code_list = dataFrame['scan_code'].unique().tolist()
    dataFrame = dataFrame[(dataFrame['country'] != 'RU')]
    dataFrame['Protocol, Port'] = '('+dataFrame['protocol'] + ', '+dataFrame['port'].astype(str) +')'
    dataFrame = dataFrame.sort_values(by = ['Protocol, Port'],ascending=True)
    dataFrame['asn_category1L1L2'] = dataFrame['asn_category1L1'] +':'+dataFrame['asn_category1L2']
    dataFrame['asn_category2L1L2'] = dataFrame['asn_category2L1'] +':'+dataFrame['asn_category2L2']


    scan_code_list = dataFrame.sort_values(by = ['scan_code'],ascending=True)['scan_code'].unique().tolist()
    print(scan_code_list)

    print(dataFrame.shape)
    df = dataFrame[dataFrame['status'] == 'io-timeout']
    print(df.shape)
   

    # create_dashboard_data(dataFrame,processed_data_dir)
    for scan_code in scan_code_list:
        df_temp = dataFrame[dataFrame['scan_code'] == scan_code]
        file_name_prefix = str(scan_code)+"_"
        plot_scan_summary_with_total(df_temp,processed_data_dir,file_name_prefix)
        plot_scan_summary(df_temp,processed_data_dir,file_name_prefix)
        plot_scan_country_info(df_temp,processed_data_dir,file_name_prefix)
        get_location_statistics(df_temp,processed_data_dir,file_name_prefix)
        get_cipher_info(df_temp,processed_data_dir,file_name_prefix)
        scan_location_inferences(df_temp,processed_data_dir,file_name_prefix)
        plot_tls_summary(df_temp,processed_data_dir,file_name_prefix)
        plot_tls_data(df_temp,processed_data_dir,file_name_prefix)
        plot_ip_port_count(df_temp,processed_data_dir,file_name_prefix,figsize=[8,10])
        plot_ip_port_upsetplot(df_temp,processed_data_dir,file_name_prefix,figsize=[8,10])
        plot_ip_port_upsetplot_two(df_temp,processed_data_dir,file_name_prefix,figsize=[8,10])
        plot_ip_port_count_ecdf(df_temp,processed_data_dir,file_name_prefix)

    plot_correlations(dataFrame,processed_data_dir)

    # plot_protocol_stability_count(dataFrame,processed_data_dir)
    # plot_port_stability_count(dataFrame,processed_data_dir)

    plot_protocol_stability_all_scan(dataFrame,processed_data_dir)
    plot_port_stability_all_scan(dataFrame,processed_data_dir)


    plot_protocol_stability_count_subnet64(dataFrame,processed_data_dir)
    plot_port_stability_count_subnet64(dataFrame,processed_data_dir)

    plot_unique_certificates1(dataFrame,processed_data_dir)
    plot_certificate_stability_all_scan(dataFrame,processed_data_dir)
    plot_certificate_validty_stability_all_scan(dataFrame,processed_data_dir)
    plot_TLSversions_stability_all_scan(dataFrame,processed_data_dir)
    get_TLSversions_upgrade(dataFrame,processed_data_dir)
    get_certificateUpgrade_info(dataFrame,processed_data_dir)

    # print(str(processed_data_dir))


if __name__ == "__main__":
    main()

