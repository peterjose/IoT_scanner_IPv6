"""@file pre_scan_multistage_ip_filter.py
    Multilevel Stage Fiteration with Shuffling
    Filter search IP list based on:
        Alias Prefix
        Blocked list
        Country based
    Filtering Order Alias Prefix > Blocked list > Country based

    @author Peter Jose

"""

from cmath import exp
from telnetlib import IP
import time
import subprocess
import pkg_resources
import argparse
import sys
import json
import pathlib
from multiprocessing import Process, Queue, freeze_support, cpu_count
import textwrap

import filter_package.apd_filteration as filter_apd
import filter_package.blocklist_filteration as filter_bl
import filter_package.country_filteration as filter_country

############################# CONFIG START #############################

# Flag to allow the installation of missing packages
ALLOW_INSTALLATION_OF_MISSING_PKGS_FLAG = False  # True / False

# List of countries which are blocklisted
# Add or remove the countries here
BLOCKLISTED_COUNTRY_CODES_LIST = [
    "RU", # Russia
]

# Name of the directory in which the output to be stored
DEFAULT_OUTPUT_DIRECTORY_NAME = "hitlist_processed"

# postfix of file in which the filtering summary should be kept 
DEFAULT_FILE_POSTPIX_FILTERING_SUMMARY = "_filtering_info.txt"

DEFAULT_PROCESSED_FILE_SUFFIX = "_processed"

############################## CONFIG END ##############################

# Internal (Don't edit, change to tweak the performance) 

# For spliting the input ip file
DEFAULT_NUMBER_OF_IP_PER_SPLIT = 1000000

# Number of multi processing depends on this variable
# NUMBER_OF_PROCESSES = cpu_count() - 2
NUMBER_OF_PROCESSES = 2

# Temporary files that will be created
DEFAULT_APD_FILTERED_FILE_NAME_PREFIX = "temp_apd_filtered_"
DEFAULT_SPLIT_FILE_NAME_EXTENTION = "_temp_file"
DEFAULT_TEMP_OUTPUT_FILE_NAME_EXTENTION = "_out_filtered"
DEFAULT_TEMP_OUTPUT_UNSORTED_FILE_NAME_EXTENTION = "_unsorted_out.txt"
DEFAULT_TEMP_OUTPUT_DIR_NAME="output"

# config file json keys
RAW_HITLIST_KEY             = "hitlist"
APD_KEY                     = "apd"
NON_APD_KEY                 = "non_apd"
MAXMIND_GEOIP_COUNTRY_KEY   = "geoip_country"
MAXMIND_GEOIP_ASN_KEY       = "geoip_asn"
ASN_CATEGORY_KEY            = "asn_cat"
PROCESSED_HITLIST_KEY       = "final_hitlist"
ASN_ORG_KEY                 = "asn_org"
ASN_IP_KEY                  = "asn"

############## Check if required libraries are present #################

# Check if the required modules for the script are installed
required = {'pysubnettree','pandas','ipaddress'}
installed = {pkg.key for pkg in pkg_resources.working_set}
missing = required - installed
if len(missing) != 0:
    print("Following packages are missing : ", missing)
    if ALLOW_INSTALLATION_OF_MISSING_PKGS_FLAG:
        print ("Installing the missing packages")
        python = sys.executable
        subprocess.check_call([python, '-m', 'pip', 'install', *missing], 
                                stdout=subprocess.DEVNULL)
    else:
        print("Enable 'ALLOW_INSTALLATION_OF_MISSING_PKGS_FLAG' in the script to install it automatically")
        exit()

########################################################################

import pandas as pd
import ipaddress

######################### Class definitions ############################

class Filtering_option:
    def __init__(self, ip_list_file , apd_filtering, blocklist_filtering, country_filtering,database_location_json=''):
        self.ip_list_file = ip_list_file
        self.apd_filtering_group = apd_filtering
        self.blocklist_filtering_group = blocklist_filtering
        self.country_filtering_group = country_filtering
        self.database_location_json = database_location_json

    def validator(self):
        """Validates all the filters
        
        """
        # Check if the file is present
        # This option is a required field so can check the file path directly
        if not pathlib.Path.is_file(pathlib.Path(self.ip_list_file)):
            exit("File not present : "+ self.ip_list_file)
        print("\nInput IP list:", pathlib.Path(self.ip_list_file).resolve())
        try:
            # print("APD filter validator")
            self.apd_filtering_group.validator()
        except Exception as e:
            print(e)
            pass
        try:
            # print("Blocklist filter validator")            
            self.blocklist_filtering_group.validator()
        except Exception as e:
            print(e)
            pass
        try:
            # print("country filter validator")            
            self.country_filtering_group.validator()
        except Exception as e:
            print(e)
            pass

class Filter_statistics:
    def __init__(self, country_code_count):
        self.processed_ip_address_count = 0
        self.improper_ip_address_count = 0
        self.aliased_ip_removed_count = 0
        self.blocklisted_ip_removed_count = 0
        self.country_code_count = country_code_count
        if self.country_code_count < 1:
            self.country_code_count = 1
        self.country_list_ip_removed_count = [ 0 for i in range(self.country_code_count)]
        self.ip_address_retained = 0
    
    def add(self,filter_stats):
        """Add two filter statistics
        
        """
        
        self.processed_ip_address_count += filter_stats.processed_ip_address_count
        self.improper_ip_address_count += filter_stats.improper_ip_address_count
        self.aliased_ip_removed_count += filter_stats.aliased_ip_removed_count
        self.blocklisted_ip_removed_count += filter_stats.blocklisted_ip_removed_count
        for index in range(self.country_code_count):
            self.country_list_ip_removed_count[index] += filter_stats.country_list_ip_removed_count[index]
        self.ip_address_retained += filter_stats.ip_address_retained

######################### Function definitions #########################

def argument_parser_fn():
    """Function to to parse the arguments received by the program
    
    """
    global BLOCKLISTED_COUNTRY_CODES_LIST
    parser = argparse.ArgumentParser(prog='pre_scan_multistage_ip_filter.py',
             formatter_class=argparse.RawDescriptionHelpFormatter,
            description=textwrap.dedent('''\
            \rProgram to do multistage filteration of IP list
            \r--------------------------------
            \rHitlist, apd, non apd is obtained from https://ipv6hitlist.github.io/
            \nAS file (pfx2as file) can be obtained from: \n\thttps://publicdata.caida.org/datasets/routing/routeviews6-prefix2as/'
            \nAS Organisations file (JSONL file) can be obtained from:\n\thttps://publicdata.caida.org/datasets/as-organizations/',
            '''))

    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("-i", "--ip-list-file", type=str, 
                            help="Raw IP list that has to be filtered")

    group.add_argument("-d", "--db-locations-file", type=str, 
                            help="Json file that contains the information about all the filtering files, used in pipeline")

    parser_group_CN = parser.add_argument_group('Country based filtering')
    parser_group_CN.add_argument("-a", "--as-file", type=str, default='',
                                    help="File containing AS info OR MaxMind geoiplite country database binary file")
    parser_group_CN.add_argument("-o", "--as-org-file", type=str, default='',
                                    help="JSON file containing AS-organization-country info")
    parser_group_CN.add_argument("-c", "--country-code", nargs='*', 
                                    default = BLOCKLISTED_COUNTRY_CODES_LIST,
                                    help="country code or codes to be blocklisted")

    parser_group_BL = parser.add_argument_group('Blocklist based filtering')
    parser_group_BL.add_argument("-b", "--blocklist-file", type=str, 
                                    help="File containing Blocklist ips")

    parser_group_APD = parser.add_argument_group('Alias prefix detection based filtering')
    parser_group_APD.add_argument("-A", "--apd-file", type=str, 
                                    help="File containing apd prefixes")
    parser_group_APD.add_argument("-N", "--non-apd-file", type=str, 
                                    help="File containing non-apd prefixes")

    args = parser.parse_args()

    apd_filtering = filter_apd.apd_filtering_option()
    blocklist_filtering = filter_bl.blocklist_filtering_option()
    country_filtering = filter_country.country_filtering_option()
    filtering_info = None
    ## Not using library based types to do automated checking, 
    ## because of further anticipated architecture of the code
    ## The following section is added to manually check if the options provided are correct or not
    if (getattr(args,'db_locations_file') is None):
        # Verify country based filtering 
        if (getattr(args,'as_file') is not None) or (getattr(args,'as_org_file') is not None):
            if (getattr(args,'as_file') is None) or (getattr(args,'as_org_file') is None):
                parser.error("missing argument in group 'Country based filtering'")
            else:
                country_filtering = filter_country.country_filtering_option(args.as_file,args.as_org_file,args.country_code)

        # Verify Alias Prefix Detection based filtering 
        if (getattr(args,'apd_file') is not None) or (getattr(args,'non_apd_file') is not None):
            if (getattr(args,'apd_file') is None) or (getattr(args,'non_apd_file') is None):
                parser.error("missing argument in group 'Alias prefix detection based filtering'")
            else:
                apd_filtering = filter_apd.apd_filtering_option(args.apd_file, args.non_apd_file)

        # Check if the blocklist file option is given
        if getattr(args,'blocklist_file') is not None:
            blocklist_filtering = filter_bl.blocklist_filtering_option(args.blocklist_file)

        filtering_info = Filtering_option(args.ip_list_file,apd_filtering,blocklist_filtering,country_filtering)
        db_locations_file = None
    else:
        db_locations_file = args.db_locations_file
        database_location_json = {}
        # read the file with the list of the database location
        if pathlib.Path(args.db_locations_file).is_file() :
            with open(args.db_locations_file, 'r') as file:
                database_location_json = json.load(file)
        if RAW_HITLIST_KEY not in database_location_json.keys():
            raise Exception("Error: hitlist missing in the db locator json")

        country_filtering = filter_country.country_filtering_option(database_location_json[MAXMIND_GEOIP_COUNTRY_KEY],args.as_org_file,args.country_code)

        apd_filtering = filter_apd.apd_filtering_option(database_location_json[APD_KEY], database_location_json[NON_APD_KEY])

        # Check if the blocklist file option is given
        if getattr(args,'blocklist_file') is not None:
            blocklist_filtering = filter_bl.blocklist_filtering_option(args.blocklist_file)

        filtering_info = Filtering_option(database_location_json[RAW_HITLIST_KEY],apd_filtering,
                            blocklist_filtering,country_filtering,database_location_json)
    filtering_info.validator()
    print("\nFile validation completed")
    return filtering_info, db_locations_file

def filter_single_file_fn(IPlist_file_path, output_file_path, filtering_options, ipCheckFlag):
    """Filter single IP file

    Args:
        IPlist_file_path: IP address file
        output_file_path: output file path
        filtering_options: filtering options

    Returns:
        filtering_statistics: filtering statistics object

    Raises:
        Nil
    """

    filtering_statistics = Filter_statistics(len(filtering_options.country_filtering_group.country_code_list))


    try:
        filtering_options.blocklist_filtering_group.initialise()
        if ipCheckFlag:
            filtering_options.blocklist_filtering_group.disable_verify_IP()
    except Exception as e:
        pass

    try:
        filtering_options.country_filtering_group.initialise()
        if ipCheckFlag:
            filtering_options.country_filtering_group.disable_verify_IP()
    except Exception as e:
        # print(e)
        pass

    with open(output_file_path,'w') as output_file:
        with open(IPlist_file_path,'r') as searchfile_reader:
            try:
                for line in searchfile_reader:
                    try:
                        ip_address = line.strip().split("\t")[0]
                        ip_address_removed_flag = False

                        if not ipCheckFlag:
                            filtering_statistics.processed_ip_address_count += 1
                            # Check if the ip is proper or not
                            # currently not checking if IPv6 or IPv4
                            try:
                                ipaddress.ip_address(ip_address)
                            except Exception as e:
                                # print("Ignoring failed to process entry", ip_address, "Error :",repr(e))
                                filtering_statistics.improper_ip_address_count += 1                    
                                ip_address_removed_flag = True

                        # Blocklist based filtering
                        if (filtering_options.blocklist_filtering_group.stateFlag == filter_bl.state.INITIALISED
                                        and not ip_address_removed_flag):
                            try:
                                if filtering_options.blocklist_filtering_group.check_blocklisted_IP_fn(ip_address):
                                    filtering_statistics.blocklisted_ip_removed_count += 1
                                    ip_address_removed_flag = True
                            except Exception as e:
                                # print("Error", repr(e))
                                pass

                        # Country based filtering
                        if (filtering_options.country_filtering_group.stateFlag == filter_country.state.INITIALISED 
                                        and not ip_address_removed_flag):
                            try:
                                if filtering_options.country_filtering_group.check_country_IP_fn(ip_address):
                                    filtering_statistics.country_list_ip_removed_count[0] += 1
                                    ip_address_removed_flag = True
                            except Exception as e:
                                # print("Skipped line '" + line +":",repr(e))
                                pass

                        if not ip_address_removed_flag:
                            output_file.write(ip_address)
                            output_file.write("\n")
                            filtering_statistics.ip_address_retained += 1
                    except Exception as e:
                        # print("Error", repr(e))
                        pass
            except Exception as e:
                # print("Error", repr(e))
                pass
    return filtering_statistics
    
def filter_based_apd_fn(filtering_options,filtering_statistics,temp_dir_path):
    """Function to filter aliased prefixed IP addresses

    Args:
        filtering_options: filtering option object
        filtering_statistics: filtering statistics object
        temp_dir_path: temporary directory path

    Returns:
        filtering_options: filtering option object
        filtering_statistics: filtering statistics object

    Raises:
        Nil
    """
    try:
        filtering_options.apd_filtering_group.initialise()
    except Exception as e:
        pass

    ipCheckFlag = False

    if filtering_options.apd_filtering_group.stateFlag == filter_apd.state.INITIALISED:
        ipCheckFlag = True
        searchlist_file_list = ([x for x in temp_dir_path.iterdir() if (x.is_file() and x.name.find(DEFAULT_SPLIT_FILE_NAME_EXTENTION) != -1)])

        for file in searchlist_file_list:
            apd_output_file = pathlib.Path(file.parents[0],DEFAULT_APD_FILTERED_FILE_NAME_PREFIX+file.name)
            with open(apd_output_file,'w') as output_file:
                with open(file ,'r') as searchfile_reader:
                    try:
                        for line in searchfile_reader:
                            filtering_statistics.processed_ip_address_count += 1 
                            try:
                                ip_address = line.strip().split("\t")[0]
                                ip_address_removed_flag = False

                                # Check if the ip is proper or not
                                # currently not checking if IPv6 or IPv4
                                try:
                                    ipaddress.ip_address(ip_address)
                                except Exception as e:
                                    # print("Ignoring failed to process entry", ip_address, "Error :",repr(e))
                                    filtering_statistics.improper_ip_address_count += 1                    
                                    ip_address_removed_flag = True

                                # Alias prefix based filtering
                                if not ip_address_removed_flag:
                                    try:
                                        # if ip_address in tree_apd:
                                        if filtering_options.apd_filtering_group.check_apd_IP_fn(ip_address):
                                            filtering_statistics.aliased_ip_removed_count += 1
                                            ip_address_removed_flag = True
                                    except Exception as e:
                                        # print("Error", repr(e))
                                        pass
                                if not ip_address_removed_flag:
                                    output_file.write(ip_address)
                                    output_file.write("\n")
                            except Exception as e:
                                # print("Error", repr(e))
                                pass
                    except Exception as e:
                        # print("Error", repr(e))
                        pass
                pathlib.Path.unlink(file)
    try:
        filtering_options.apd_filtering_group.deinitialise()
    except Exception as e:
        pass
    return filtering_options, filtering_statistics, ipCheckFlag


def create_temporary_Directories_fn(filtering_options, timeStamp):
    """Function to create temporary directories
    
    Args:
        filtering_options: filtering option object
        timeStamp: timeStamp

    Returns:
        temp_dir_path: temp directory
        temp_output_dir_path: temp output directory

    Raises:
        Nil
    """
    # create temp directory to store the split files and the output
    temp_dir_path = pathlib.Path(pathlib.Path(filtering_options.ip_list_file).parents[0],"temp_dir_"+str(timeStamp))
    if not pathlib.Path.is_dir(temp_dir_path):
        pathlib.Path.mkdir(temp_dir_path)

    # create output directory
    temp_output_dir_path = pathlib.Path(temp_dir_path, DEFAULT_TEMP_OUTPUT_DIR_NAME)
    if not pathlib.Path.is_dir(temp_output_dir_path):
        pathlib.Path.mkdir(temp_output_dir_path)

    return temp_dir_path, temp_output_dir_path


def print_filter_statistics_fn(filtering_options,filter_statistics):
    """Function to print the filter statistics

    Args:
        filtering_options: filtering option object
        filtering_statistics: filtering statistics object

    Returns:
        Nil

    Raises:
        Nil
    """
    # print the count of IPs filtered out
    print("\nFilter Statistics")
    print("Total IP address processed                   : "+ str(filter_statistics.processed_ip_address_count))
    print("Improper IPs skipped                         : " + str(filter_statistics.improper_ip_address_count))
    if filtering_options.apd_filtering_group.stateFlag == filter_apd.state.INITIALISED or 1:
        print("IPs filtered based on Alias Prefix Detection : " + str(filter_statistics.aliased_ip_removed_count))

    if filtering_options.blocklist_filtering_group.stateFlag == filter_bl.state.INITIALISED or 1:
        print("IPs filtered based on Blocklist              : " + str(filter_statistics.blocklisted_ip_removed_count))

    if filtering_options.country_filtering_group.stateFlag == filter_country.state.INITIALISED or 1:
        print("IPs filtered based on country Code")
        for index in range(0,len(filtering_options.country_filtering_group.country_code_list)):
            print("\tCountry Code",filtering_options.country_filtering_group.country_code_list[index], ", IPs removed        :", 
                    str(filter_statistics.country_list_ip_removed_count[index]))
    
    print("Total IPs retained after filtering           : " + str(filter_statistics.ip_address_retained))


def log_filter_statistics(filtering_options,filter_statistics,output_dir_path,filtered_output_file,timeStamp):
    """Function to log filter statistics
       
    Args:
        filtering_options: filtering option object
        filtering_statistics: filtering statistics object
        output_dir_path: temporary directory path
        filtered_output_file: filtered output file
        timeStamp: timestamp

    Returns:
        Nil

    Raises:
        Nil
    """
    
    with open(str(output_dir_path.resolve())+"/"+str(timeStamp)+DEFAULT_FILE_POSTPIX_FILTERING_SUMMARY, 'a') as ofile:
        print("\nInput IP list:", pathlib.Path(filtering_options.ip_list_file).resolve(), file= ofile)
        if filtering_options.apd_filtering_group.stateFlag  != filter_apd.state.NOT_INITIALISED:
            print("Files used for APD filtering: ", file= ofile)
            print("    Aliased Prefixed IP list:", pathlib.Path(filtering_options.apd_filtering_group.apd_file).resolve(), file= ofile)
            print("    Non-Aliased Prefixed IP list:", pathlib.Path(filtering_options.apd_filtering_group.non_apd_file).resolve(), file= ofile)
        if filtering_options.blocklist_filtering_group.stateFlag  != filter_bl.state.NOT_INITIALISED:
            print("Files used for blocklist filtering: ", file= ofile)
            print("    Blocklisted IP list:", pathlib.Path(filtering_options.blocklist_filtering_group.blocklist_file).resolve(), file= ofile)
        if filtering_options.country_filtering_group.stateFlag  != filter_country.state.NOT_INITIALISED:
            print("Files used for Country based filtering: ", file= ofile)
            print("  of countries:",filtering_options.country_filtering_group.country_code_list, file= ofile)
            print("    AS-info list:", pathlib.Path(filtering_options.country_filtering_group.as_file).resolve(), file= ofile)
            print("    AS-org list:", pathlib.Path(filtering_options.country_filtering_group.as_org_file).resolve(), file= ofile)

        # print the count of IPs filtered out
        print("\nFilter Statistics", file= ofile)
        print("Total IP address processed                   : "+ str(filter_statistics.processed_ip_address_count), file= ofile)
        print("Improper IPs skipped                         : " + str(filter_statistics.improper_ip_address_count), file= ofile)
        if filtering_options.apd_filtering_group.stateFlag  != filter_apd.state.NOT_INITIALISED:
            print("IPs filtered based on Alias Prefix Detection : " + str(filter_statistics.aliased_ip_removed_count), file= ofile)

        if filtering_options.blocklist_filtering_group.stateFlag  != filter_bl.state.NOT_INITIALISED:
            print("IPs filtered based on Blocklist              : " + str(filter_statistics.blocklisted_ip_removed_count), file= ofile)

        if filtering_options.country_filtering_group.stateFlag  != filter_country.state.NOT_INITIALISED:
            print("IPs filtered based on country Code", file= ofile)
            for index in range(0,len(filtering_options.country_filtering_group.country_code_list)):
                print("\tCountry Code",filtering_options.country_filtering_group.country_code_list[index], ", IPs removed               :", 
                        str(filter_statistics.country_list_ip_removed_count[index]), file= ofile)
        
        print("Total IPs retained after filtering           : " + str(filter_statistics.ip_address_retained), file= ofile)
        print("\nSearch list generated is : "+str(output_dir_path.resolve())+"/"+filtered_output_file,file= ofile)


def get_output_filename_fn(filtering_options,timeStamp,apd_done_flag=False):
    """Function to get the output file name
       
    Args:
        filtering_options: filtering option object
        timeStamp: timestamp
        apd_done_flag: flag should be done if apd object has be deinitailiased after use for memory issues

    Returns:
        Nil

    Raises:
        Nil
    """
    # Output file name creation 
    output_filename = pathlib.Path(filtering_options.ip_list_file).name.split('.')[0]

    if (filtering_options.apd_filtering_group.stateFlag != filter_apd.state.NOT_INITIALISED) or apd_done_flag:
        output_filename += '_apd'

    if filtering_options.blocklist_filtering_group.stateFlag != filter_bl.state.NOT_INITIALISED:
        output_filename += '_bl'

    if filtering_options.country_filtering_group.stateFlag != filter_country.state.NOT_INITIALISED:
        output_filename += '_cn'

    output_filename += "_" + str(timeStamp)+DEFAULT_PROCESSED_FILE_SUFFIX+".txt"

    return output_filename

# ####
# # Worker function to manage the multiprocessing
# ####
def worker(input, output):
    for func, args in iter(input.get, 'STOP'):
        filter_stats = func(*args)
        output.put((filter_stats))


def filter_ip_list_fn(filtering_options):
    """Filter the IP addresses

    Args:
        filtering_options: Filtering option class object
    
    Returns:
        Nil
    
    Raises:
        Nil
    
    """
    
    timeStamp = int(time.time())    

    # create the temporary directories
    temp_dir_path, temp_output_dir_path = create_temporary_Directories_fn(filtering_options,timeStamp)

    print("Splitting input file for further processing")
    # Split the input file for faster processing 
    subprocess.call(["cd "+str(temp_dir_path.resolve())+";"+
                    " split -l "+str(DEFAULT_NUMBER_OF_IP_PER_SPLIT)+" "+ str(pathlib.Path(filtering_options.ip_list_file).resolve()) +
                            " --additional-suffix="+DEFAULT_SPLIT_FILE_NAME_EXTENTION],
                    shell=True)

    print("Processing of data started, please wait ..")

    filter_statistics = Filter_statistics(len(filtering_options.country_filtering_group.country_code_list))
    filtering_options, filter_statistics, apd_ipCheckFlag = filter_based_apd_fn(filtering_options,filter_statistics,temp_dir_path)

    # get all the search list files
    searchlist_file_list = ([x for x in temp_dir_path.iterdir() if (x.is_file() and x.name.find(DEFAULT_SPLIT_FILE_NAME_EXTENTION) != -1)])

    TASKS1 = []
    for file in searchlist_file_list:
        TASKS1.append((filter_single_file_fn, 
                        (file,pathlib.Path(temp_output_dir_path,file.name+DEFAULT_TEMP_OUTPUT_FILE_NAME_EXTENTION)
                            ,filtering_options, apd_ipCheckFlag)))

    # Create queues
    task_queue = Queue()
    done_queue = Queue()

    # Submit tasks
    for task in TASKS1:
        task_queue.put(task,timeout=2000)

    # Start worker processes
    for i in range(NUMBER_OF_PROCESSES):
        Process(target=worker, args=(task_queue, done_queue)).start()

    # Get the count
    for i in range(len(TASKS1)):
        filter_statistics.add(done_queue.get())

    # Tell child processes to stop
    for i in range(NUMBER_OF_PROCESSES):
        task_queue.put('STOP')

    # print filter statistics
    print_filter_statistics_fn(filtering_options,filter_statistics)
    print("\nFiltering completed. Merging in progress")    

    # get the output file name
    final_output_filename = get_output_filename_fn(filtering_options,timeStamp,apd_ipCheckFlag)

    # output directory path
    output_dir_path = pathlib.Path(pathlib.Path(filtering_options.ip_list_file).parents[1],DEFAULT_OUTPUT_DIRECTORY_NAME)
    if not pathlib.Path.is_dir(output_dir_path):
        pathlib.Path.mkdir(output_dir_path)

    # get all the temp filtered files
    temp_filtered_file_list = ([x for x in temp_output_dir_path.iterdir() if (x.is_file() and x.name.find(DEFAULT_TEMP_OUTPUT_FILE_NAME_EXTENTION) != -1)])

    for temp_file in temp_filtered_file_list:
        subprocess.call(["cd "+str(temp_output_dir_path.resolve())+";"+
                " sort -u -R -b "+str(temp_file.resolve())+" -o "+str(temp_file.resolve())+DEFAULT_TEMP_OUTPUT_UNSORTED_FILE_NAME_EXTENTION], 
                stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)        

    # Split the input file for faster processing 
    subprocess.call(["cd "+str(temp_output_dir_path.resolve())+";"+
                    "cat *"+ DEFAULT_TEMP_OUTPUT_FILE_NAME_EXTENTION+DEFAULT_TEMP_OUTPUT_UNSORTED_FILE_NAME_EXTENTION +" >> "+str(output_dir_path.resolve())+"/"+final_output_filename+ ";" +
                    "rm -rf "+str(temp_dir_path.resolve())], stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                    shell=True) 
 
    print("Merging completed")
    print("\nSearch list generated is : "+str(output_dir_path.resolve())+"/"+final_output_filename)

    log_filter_statistics(filtering_options,filter_statistics,output_dir_path.resolve(),final_output_filename,timeStamp)
    filtering_options.database_location_json.update({PROCESSED_HITLIST_KEY:str(output_dir_path.resolve())+"/"+final_output_filename})

def main():
    """Main function
    
    """
    start_ = time.time()
    filtering_options,db_locations_file = argument_parser_fn()
    
    if ((filtering_options.country_filtering_group.stateFlag == filter_country.state.VALIDATED) | 
            (filtering_options.blocklist_filtering_group.stateFlag == filter_bl.state.VALIDATED)| 
            (filtering_options.apd_filtering_group.stateFlag == filter_apd.state.VALIDATED)) : 
        filter_ip_list_fn(filtering_options)
        if db_locations_file != None:
            with open(db_locations_file, 'w') as f:
                json.dump(filtering_options.database_location_json, f)
        print("Total time elapsed:", str(int((time.time() - start_)/60)) + " minutes" 
                                    if (time.time() - start_) > 60 
                                    else str(int(time.time()- start_)) + " seconds")
    else:
        print("Filtering not done as none of the filtering groups were provided")

if __name__ == "__main__":
    freeze_support()
    main()

# EOF 