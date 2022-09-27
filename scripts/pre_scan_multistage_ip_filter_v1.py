###
#
# Filter search IP list based on:
    # Alias Prefix
    # Blocked list
    # Country based
# Filtering Order Alias Prefix > Blocked list > Country based
#
###

import time
import subprocess
import pkg_resources
import argparse
import sys
import json
import pathlib
from multiprocessing import Process, Queue, freeze_support
import multiprocessing

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

############################## CONFIG END ##############################

# Internal (Don't edit, change to tweak the performance) 

# For spliting the input ip file
DEFAULT_NUMBER_OF_IP_PER_SPLIT = 1000000

# Number of multi processing depends on this variable
NUMBER_OF_PROCESSES = multiprocessing.cpu_count() - 2

# Temporary files that will be created
DEFAULT_APD_FILTERED_FILE_NAME_PREFIX = "temp_apd_filtered_"
DEFAULT_SPLIT_FILE_NAME_EXTENTION = "_temp_file"
DEFAULT_TEMP_OUTPUT_FILE_NAME_EXTENTION = "_out_filtered"
DEFAULT_TEMP_OUTPUT_UNSORTED_FILE_NAME_EXTENTION = "_unsorted_out.txt"
DEFAULT_TEMP_OUTPUT_DIR_NAME="output"

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

import SubnetTree
import pandas as pd
import ipaddress

######################### Class definitions ############################
class APD_filtering_option:
    def __init__(self, apd_file, non_apd_file):
        self.apd_file=apd_file
        self.non_apd_file = non_apd_file
        self.enabledFlag = False
    
    def validator(self):
        # Check if the files are present
        if self.apd_file != '':
            if not pathlib.Path.is_file(pathlib.Path(self.apd_file)):
                exit("File not present: " + self.apd_file)
        if self.non_apd_file != '':
            if not pathlib.Path.is_file(pathlib.Path(self.non_apd_file)):
                exit("File not present: " + self.non_apd_file)
        if self.apd_file != '' and self.non_apd_file != '':
            self.enabledFlag = True
            print("Files used for APD filtering: ")
            print("    Aliased Prefixed IP list:", pathlib.Path(self.apd_file).resolve())
            print("    Non-Aliased Prefixed IP list:", pathlib.Path(self.non_apd_file).resolve())

class Blocklist_filtering_option:
    def __init__(self, blocklist_file):
        self.blocklist_file=blocklist_file
        self.enabledFlag = False
    
    def validator(self):
        # Check if the files are present
        if self.blocklist_file != '':
            if not pathlib.Path.is_file(pathlib.Path(self.blocklist_file)):
                exit("File not present: " + self.blocklist_file)
            self.enabledFlag = True
            print("Files used for blocklist filtering: ")
            print("    Blocklisted IP list:", pathlib.Path(self.blocklist_file).resolve())

class Country_filtering_option:
    def __init__(self, as_file, as_org_file, country_code_list):
        self.as_file=as_file
        self.as_org_file = as_org_file
        self.country_code_list=country_code_list
        self.enabledFlag = False
    
    def validator(self):
        if self.as_file != '':
            if not pathlib.Path.is_file(pathlib.Path(self.as_file)):
                exit("File not present: "+self.as_file)
        if self.as_org_file != '':
            if not pathlib.Path.is_file(pathlib.Path(self.as_org_file)):
                exit("File not present: "+self.as_org_file)
        if self.as_file != '' and self.as_org_file != '' and len(self.country_code_list) != 0:
            self.enabledFlag = True
            print("Files used for Country based filtering: ")
            print("  of countries:",self.country_code_list )
            print("    AS-info list:", pathlib.Path(self.as_file).resolve())
            print("    AS-org list:", pathlib.Path(self.as_org_file).resolve())

class Filtering_option:
    def __init__(self, ip_list_file , apd_filtering, blocklist_filtering, country_filtering):
        self.ip_list_file = ip_list_file
        self.apd_filtering_group = apd_filtering
        self.blocklist_filtering_group = blocklist_filtering
        self.country_filtering_group = country_filtering

    def validator(self):
        # Check if the file is present
        # This option is a required field so can check the file path directly
        if not pathlib.Path.is_file(pathlib.Path(self.ip_list_file)):
            exit("File not present : "+ self.ip_list_file)
        print("\nInput IP list:", pathlib.Path(self.ip_list_file).resolve())
        self.apd_filtering_group.validator()
        self.blocklist_filtering_group.validator()
        self.country_filtering_group.validator()

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
        self.processed_ip_address_count += filter_stats.processed_ip_address_count
        self.improper_ip_address_count += filter_stats.improper_ip_address_count
        self.aliased_ip_removed_count += filter_stats.aliased_ip_removed_count
        self.blocklisted_ip_removed_count += filter_stats.blocklisted_ip_removed_count
        for index in range(self.country_code_count):
            self.country_list_ip_removed_count[index] += filter_stats.country_list_ip_removed_count[index]
        self.ip_address_retained += filter_stats.ip_address_retained

######################### Function definitions #########################

# ####
# # Function to to parse the arguments received by the program
# ####
def argument_parser_fn():
    global BLOCKLISTED_COUNTRY_CODES_LIST
    parser = argparse.ArgumentParser()

    parser.add_argument("-i", "--ip-list-file", required=True, type=str, 
                            help="Raw IP list that has to be filtered")

    parser_group_CN = parser.add_argument_group('Country based filtering')
    parser_group_CN.add_argument("-a", "--as-file", type=str, 
                                    help="File containing AS info")
    parser_group_CN.add_argument("-o", "--as-org-file", type=str, 
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

    apd_filtering = APD_filtering_option('','')
    blocklist_filtering = Blocklist_filtering_option('')
    country_filtering = Country_filtering_option('','','')
    
    ## Not using library based types to do automated checking, 
    ## because of further anticipated architecture of the code
    ## The following section is added to manually check if the options provided are correct or not

    # Verify country based filtering 
    if (getattr(args,'as_file') is not None) or (getattr(args,'as_org_file') is not None):
        if (getattr(args,'as_file') is None) or (getattr(args,'as_org_file') is None):
            parser.error("missing argument in group 'Country based filtering'")
        else:
            country_filtering = Country_filtering_option(args.as_file,args.as_org_file,args.country_code)

    # Verify Alias Prefix Detection based filtering 
    if (getattr(args,'apd_file') is not None) or (getattr(args,'non_apd_file') is not None):
        if (getattr(args,'apd_file') is None) or (getattr(args,'non_apd_file') is None):
            parser.error("missing argument in group 'Alias prefix detection based filtering'")
        else:
            apd_filtering = APD_filtering_option(args.apd_file, args.non_apd_file)



    # Check if the blocklist file option is given
    if getattr(args,'blocklist_file') is not None:
        blocklist_filtering = Blocklist_filtering_option(args.blocklist_file)

    filtering_info = Filtering_option(args.ip_list_file,apd_filtering,blocklist_filtering,country_filtering)
    filtering_info.validator()
    print("\nFile validation completed")
    return filtering_info

####
# Create blocklist info tree 
####
def create_blocklist_info_tree_fn(blocklist_filtering_group):
    tree_bl = SubnetTree.SubnetTree()

    with open(blocklist_filtering_group.blocklist_file,'r') as file_ptr:
        for ip_prefix in file_ptr:
            ip_prefix=ip_prefix.split()
            if ip_prefix:
                try:
                    tree_bl.insert(ip_prefix[0])
                except Exception as e:
                    pass 
                    # print("Ignoring failed to process entry", ip_prefix[0], "Error :", repr(e))
    
    return tree_bl

####
# Create AS tree function
#### 
def create_AS_tree_fn(AS_file_name):
    # Subnet tree with AS info
    tree = SubnetTree.SubnetTree()
    with open(AS_file_name,'r') as AS_file:
        try:
            for line in AS_file:
                AS_data = line.strip().split("\t")
                try:
                    tree[AS_data[0]+"/"+AS_data[1]] = AS_data[2]
                except Exception as e:
                    pass
                    #print("Skipped line '" + line)
        except Exception as e:
            pass
            #print("Error", repr(e))
    return tree

####
# Create AS filter list function
####
def get_AS_org_country_list_fn(as_org_file):
    with open(as_org_file,'r') as as_country_file:
        AS_country_map = dict()
        try:
            for line in as_country_file:
                line_json=json.loads(line)
                orgID = ''
                country = ''
                asn = ''
                if "organizationId" in line_json.keys():
                    orgID = line_json["organizationId"]
                if "country" in line_json.keys():
                    country = line_json["country"]
                if "asn" in line_json.keys():
                    asn = line_json["asn"]
                if orgID != '':
                    elem = {"country":country,"asn":asn}
                    if orgID in AS_country_map.keys():
                        elem = AS_country_map[orgID]
                        if country != '':
                            elem["country"] = country
                        if asn != '':
                            elem["asn"] = asn
                    AS_country_map[orgID] = elem
        except Exception as e:
            pass
            #print("Error", repr(e))

    return pd.DataFrame.from_dict(AS_country_map,orient='index').reset_index()

# ####
# # Function to filter the IP from a single file
# ####
def filter_searchlist_single_file_fn(searchlist_file_path, output_file_path, filtering_options):
    
    filtering_statistics = Filter_statistics(len(filtering_options.country_filtering_group.country_code_list))

    if filtering_options.blocklist_filtering_group.enabledFlag:
        tree_bl = create_blocklist_info_tree_fn(filtering_options.blocklist_filtering_group)

    if filtering_options.country_filtering_group.enabledFlag:
        df = get_AS_org_country_list_fn(filtering_options.country_filtering_group.as_org_file)
        blocked_country_list_len = len(filtering_options.country_filtering_group.country_code_list)
        asn_filter_list = []
        for country_code in filtering_options.country_filtering_group.country_code_list:
            data = df[(df["country"]==country_code)]["asn"].to_list()
            if len(data):
                asn_filter_list.append(data)
            
        tree_as = create_AS_tree_fn(filtering_options.country_filtering_group.as_file)

    with open(output_file_path,'w') as output_file:
        with open(searchlist_file_path,'r') as searchfile_reader:
            try:
                for line in searchfile_reader:
                    try:
                        ip_address = line.strip().split("\t")[0]
                        ip_address_removed_flag = False

                        if not filtering_options.apd_filtering_group.enabledFlag:
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
                        if filtering_options.blocklist_filtering_group.enabledFlag and not ip_address_removed_flag:
                            try:
                                if ip_address in tree_bl:
                                    filtering_statistics.blocklisted_ip_removed_count += 1
                                    ip_address_removed_flag = True
                            except Exception as e:
                                # print("Error", repr(e))
                                pass

                        # Country based filtering
                        if filtering_options.country_filtering_group.enabledFlag and not ip_address_removed_flag:
                            try:
                                AS_info = tree_as[ip_address].split(",")
                                for asn in AS_info:
                                    for asn_filter_list_individual, index in zip(asn_filter_list,range(0,blocked_country_list_len)):
                                        if asn in asn_filter_list_individual:
                                            filtering_statistics.country_list_ip_removed_count[index] += 1
                                            ip_address_removed_flag = True
                                            break
                                    if ip_address_removed_flag:
                                        break

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

####
# fill tree function
#### 
def fill_tree(tree, fh, suffix):
    for line in fh:
        line = line.strip()
        try:
            tree[line] = line + suffix
        except ValueError as e:
            pass
            # print("Skipped line '" + line + "'", file=sys.stderr)
    return tree

def read_aliased(tree, fh):
    with open(fh,'r') as file_ptr:
        return fill_tree(tree, file_ptr, ",1")

def read_non_aliased(tree, fh):
    with open(fh,'r') as file_ptr:
        return fill_tree(tree, file_ptr, ",0")

####
# get tree function
####
def create_apd_info_tree_fn(apd_filtering_group):
    # Store aliased and non-aliased prefixes in a single subnet tree
    tree_apd = SubnetTree.SubnetTree()

    # Read aliased and non-aliased prefixes
    tree_apd = read_aliased(tree_apd, apd_filtering_group.apd_file)
    tree_apd = read_non_aliased(tree_apd, apd_filtering_group.non_apd_file)

    return tree_apd
# ####
# # Function to filter based on apd
# ####
def filter_based_apd_fn(filtering_options,filtering_statistics,temp_dir_path):
    
    if filtering_options.apd_filtering_group.enabledFlag:
        tree_apd = create_apd_info_tree_fn(filtering_options.apd_filtering_group)
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
                                        if tree_apd[ip_address][-1] == '1':
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
    return filtering_options, filtering_statistics

# ####
# # Function to create temporary directories
# ####
def create_temporary_Directories_fn(filtering_options, timeStamp):
    # create temp directory to store the split files and the output
    temp_dir_path = pathlib.Path(pathlib.Path(filtering_options.ip_list_file).parents[0],"temp_dir_"+str(timeStamp))
    if not pathlib.Path.is_dir(temp_dir_path):
        pathlib.Path.mkdir(temp_dir_path)

    # create output directory
    temp_output_dir_path = pathlib.Path(temp_dir_path, DEFAULT_TEMP_OUTPUT_DIR_NAME)
    if not pathlib.Path.is_dir(temp_output_dir_path):
        pathlib.Path.mkdir(temp_output_dir_path)

    return temp_dir_path, temp_output_dir_path

# ####
# # Function to print filter statistics
# ####
def print_filter_statistics_fn(filtering_options,filter_statistics):
    # print the count of IPs filtered out
    print("\nFilter Statistics")
    print("Total IP address processed                   : "+ str(filter_statistics.processed_ip_address_count))
    print("Improper IPs skipped                         : " + str(filter_statistics.improper_ip_address_count))
    if filtering_options.apd_filtering_group.enabledFlag:
        print("IPs filtered based on Alias Prefix Detection : " + str(filter_statistics.aliased_ip_removed_count))

    if filtering_options.blocklist_filtering_group.enabledFlag:
        print("IPs filtered based on Blocklist              : " + str(filter_statistics.blocklisted_ip_removed_count))

    if filtering_options.country_filtering_group.enabledFlag:
        print("IPs filtered based on country Code")
        for index in range(0,len(filtering_options.country_filtering_group.country_code_list)):
            print("\tCountry Code",filtering_options.country_filtering_group.country_code_list[index], ", IPs removed        :", 
                    str(filter_statistics.country_list_ip_removed_count[index]))
    
    print("Total IPs retained after filtering           : " + str(filter_statistics.ip_address_retained))

# ####
# # Function to Log filter statistics
# ####
def log_filter_statistics(filtering_options,filter_statistics,output_dir_path,filtered_output_file,timeStamp):
    
    with open(str(output_dir_path.resolve())+"/"+str(timeStamp)+DEFAULT_FILE_POSTPIX_FILTERING_SUMMARY, 'a') as ofile:
        print("\nInput IP list:", pathlib.Path(filtering_options.ip_list_file).resolve(), file= ofile)
        if filtering_options.apd_filtering_group.enabledFlag:
            print("Files used for APD filtering: ", file= ofile)
            print("    Aliased Prefixed IP list:", pathlib.Path(filtering_options.apd_filtering_group.apd_file).resolve(), file= ofile)
            print("    Non-Aliased Prefixed IP list:", pathlib.Path(filtering_options.apd_filtering_group.non_apd_file).resolve(), file= ofile)
        if filtering_options.blocklist_filtering_group.enabledFlag:
            print("Files used for blocklist filtering: ", file= ofile)
            print("    Blocklisted IP list:", pathlib.Path(filtering_options.blocklist_filtering_group.blocklist_file).resolve(), file= ofile)
        if filtering_options.country_filtering_group.enabledFlag:
            print("Files used for Country based filtering: ", file= ofile)
            print("  of countries:",filtering_options.country_filtering_group.country_code_list, file= ofile)
            print("    AS-info list:", pathlib.Path(filtering_options.country_filtering_group.as_file).resolve(), file= ofile)
            print("    AS-org list:", pathlib.Path(filtering_options.country_filtering_group.as_org_file).resolve(), file= ofile)

        # print the count of IPs filtered out
        print("\nFilter Statistics", file= ofile)
        print("Total IP address processed                   : "+ str(filter_statistics.processed_ip_address_count), file= ofile)
        print("Improper IPs skipped                         : " + str(filter_statistics.improper_ip_address_count), file= ofile)
        if filtering_options.apd_filtering_group.enabledFlag:
            print("IPs filtered based on Alias Prefix Detection : " + str(filter_statistics.aliased_ip_removed_count), file= ofile)

        if filtering_options.blocklist_filtering_group.enabledFlag:
            print("IPs filtered based on Blocklist              : " + str(filter_statistics.blocklisted_ip_removed_count), file= ofile)

        if filtering_options.country_filtering_group.enabledFlag:
            print("IPs filtered based on country Code", file= ofile)
            for index in range(0,len(filtering_options.country_filtering_group.country_code_list)):
                print("\tCountry Code",filtering_options.country_filtering_group.country_code_list[index], ", IPs removed               :", 
                        str(filter_statistics.country_list_ip_removed_count[index]), file= ofile)
        
        print("Total IPs retained after filtering           : " + str(filter_statistics.ip_address_retained), file= ofile)
        print("\nSearch list generated is : "+str(output_dir_path.resolve())+"/"+filtered_output_file,file= ofile)

# ####
# # Function to get the output file name
# ####
def get_output_filename_fn(filtering_options,timeStamp):
    # Output file name creation 
    output_filename = pathlib.Path(filtering_options.ip_list_file).name.split('.')[0]

    if filtering_options.apd_filtering_group.enabledFlag:
        output_filename += '_apd'

    if filtering_options.blocklist_filtering_group.enabledFlag:
        output_filename += '_bl'

    if filtering_options.country_filtering_group.enabledFlag:
        output_filename += '_cn'

    output_filename += "_" + str(timeStamp)+".txt"

    return output_filename

# ####
# # Worker function to manage the multiprocessing
# ####
def worker(input, output):
    for func, args in iter(input.get, 'STOP'):
        filter_stats = func(*args)
        output.put((filter_stats))

# ####
# # Function to filter the IP
# ####
def filter_ip_list_fn(filtering_options):
    
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
    filtering_options, filter_statistics = filter_based_apd_fn(filtering_options,filter_statistics,temp_dir_path)

    # get all the search list files
    searchlist_file_list = ([x for x in temp_dir_path.iterdir() if (x.is_file() and x.name.find(DEFAULT_SPLIT_FILE_NAME_EXTENTION) != -1)])

    TASKS1 = []
    for file in searchlist_file_list:
        TASKS1.append((filter_searchlist_single_file_fn, 
                        (file,pathlib.Path(temp_output_dir_path,file.name+DEFAULT_TEMP_OUTPUT_FILE_NAME_EXTENTION)
                            ,filtering_options)))

    # Create queues
    task_queue = Queue()
    done_queue = Queue()

    # Submit tasks
    for task in TASKS1:
        task_queue.put(task,timeout=200)

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
    final_output_filename = get_output_filename_fn(filtering_options,timeStamp)

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

####
# Main function
####
def main():
    start_ = time.time()
    filtering_options = argument_parser_fn()
    
    if (filtering_options.country_filtering_group.enabledFlag | 
            filtering_options.blocklist_filtering_group.enabledFlag | 
            filtering_options.apd_filtering_group.enabledFlag) : 
        filter_ip_list_fn(filtering_options)
        print("Total time elapsed:", str(int((time.time() - start_)/60)) + " minutes" 
                                    if (time.time() - start_) > 60 
                                    else str(int(time.time()- start_)) + " seconds")
    else:
        print("Filtering not done as none of the filtering groups were provided")

if __name__ == "__main__":
    freeze_support()
    main()

# EOF 