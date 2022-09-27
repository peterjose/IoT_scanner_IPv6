"""
    Program to filter a set of files or a file based on country

        AS file (pfx2as file) can be obtained from: 
            https://publicdata.caida.org/datasets/routing/routeviews6-prefix2as/
        AS Organisations file (JSONL file) can be obtained from:
            https://publicdata.caida.org/datasets/as-organizations/

"""

import subprocess   
import pkg_resources
import argparse
import sys
import filter_package
import pathlib
from multiprocessing import Process, Queue, freeze_support
import textwrap

############################# CONFIG START #############################

# Flag to allow the installation of missing packages
ALLOW_INSTALLATION_OF_MISSING_PKGS_FLAG = False  # True / False

# depending on the process capabilities use the number of processes
# NUMBER_OF_PROCESSES = multiprocessing.cpu_count() - 2
NUMBER_OF_PROCESSES = 2

# List of countries which are blocklisted
# Add or remove the countries here
BLOCKLISTED_COUNTRY_CODES_LIST = [
    "RU", # Russia
]

############################## CONFIG END ##############################

# Check if the required modules for the script are installed
required = {'pysubnettree','pandas'}
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

import pandas as pd

Total_IP_Removed = []


def filter_searchlist_single_file_fn(
        input_file_path, 
        output_file_path, 
        as_organisations_file_path, 
        as_file_path, 
        blocked_country_list):
    """Function to filter the IP from a single file

    Args:
        input_file_path: input file path on which filteration to be done
        output_file_path: output file path to which filtered data to be stored
        as_organisations_file_path: path to AS Organistaion file
        as_file_path: path to AS information file
        blocked_country_list: list of country codes that has to be filtered
    
    Returns:
        List containing the number of removed IP addresses 
    
    Raises:
        Nil
    """

    # Get the AS country list
    AS_Country_df = pd.DataFrame.from_dict(filter_package.create_AS_country_dict_fn(as_organisations_file_path),orient='index').reset_index()
    blocked_country_list_len = len(blocked_country_list)
    asn_filter_list = []
    for country_code in blocked_country_list:
        data = AS_Country_df[(AS_Country_df["country"]==country_code)]["asn"].to_list()
        if len(data):
            asn_filter_list.append(data)
    
    # create IP-AS tree
    IP_AS_tree = filter_package.create_AS_tree_fn(as_file_path)
    # list to keep the count of IP addresses removed
    IP_removed = [ 0 for i in range(blocked_country_list_len)]
    with open(output_file_path,'w') as output_file:
        with open(input_file_path,'r') as searchfile_reader:
            try:
                for line in searchfile_reader:
                    ip_address = line.strip().split("\t")
                    try:
                        AS_info = IP_AS_tree[ip_address[0]].split(",")
                        rm_flag = False
                        for asn in AS_info:
                            for asn_filter_list_individual, index in zip(asn_filter_list,range(0,blocked_country_list_len)):
                                if asn in asn_filter_list_individual:
                                    IP_removed[index] += 1
                                    rm_flag = True
                                    break
                            if rm_flag:
                                break
                        if not rm_flag:
                            output_file.write(ip_address[0])
                            output_file.write("\n")
                    except Exception as e:
                        # print("Skipped line '" + line +":",repr(e))
                        output_file.write(ip_address[0])
                        output_file.write("\n")
            except Exception as e:
                # print("Error", repr(e))
                pass
    return IP_removed

def worker(input, output):
    for func, args in iter(input.get, 'STOP'):
        IP_removed = func(*args)
        output.put((IP_removed))

def schedule_filter_searchlist_files_fn(
        iplist_dir_path, 
        as_organisations_file_path, 
        AS_file_path, 
        file_name_pattern):
    """Function to schedule the filteration of files 
    
    Args:
        iplist_dir_path: input directory path in which files that contain IP list
        as_organisations_file_path: path to AS Organistaion file
        as_file_path: path to AS information file
        file_name_pattern: File name or file name pattern
    
    Returns:
        List containing the number of removed IP addresses 
    
    Raises:
        Nil
    """    
    
    global Total_IP_removed
    # create output directory
    output_path = pathlib.Path(iplist_dir_path,"output")
    if not pathlib.Path.is_dir(output_path):
        pathlib.Path.mkdir(output_path)
    
    # get all the search list files
    searchlist_file_list = ([x for x in iplist_dir_path.iterdir() if (x.is_file() and x.name.find(file_name_pattern) != -1)])

    TASKS1 = []
    for file in searchlist_file_list:
        TASKS1.append((filter_searchlist_single_file_fn, 
                        (file,pathlib.Path(output_path,file.name+"_out.txt"),as_organisations_file_path, AS_file_path,BLOCKLISTED_COUNTRY_CODES_LIST)))

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
        removed_count = done_queue.get()
        for count,index in zip(removed_count,range(0,len(BLOCKLISTED_COUNTRY_CODES_LIST))):
            Total_IP_Removed[index] += count
        

    # Tell child processes to stop
    for i in range(NUMBER_OF_PROCESSES):
        task_queue.put('STOP')

    # print the count of IPs filtered out
    for index in range(0,len(BLOCKLISTED_COUNTRY_CODES_LIST)):
        print("Country Code",BLOCKLISTED_COUNTRY_CODES_LIST[index], ",IPs removed ", Total_IP_Removed[index])


def main():
    """ 
        Function Main
    """
    global BLOCKLISTED_COUNTRY_CODES_LIST
    global Total_IP_Removed
    parser = argparse.ArgumentParser()
    parser = argparse.ArgumentParser(prog='country_based_ip_filter.py',
             formatter_class=argparse.RawDescriptionHelpFormatter,
            description=textwrap.dedent('''\
            \rProgram to filter a set of files or a file based on country
            \r--------------------------------
            \nAS file (pfx2as file) can be obtained from: \n\thttps://publicdata.caida.org/datasets/routing/routeviews6-prefix2as/'
            \nAS Organisations file (JSONL file) can be obtained from:\n\thttps://publicdata.caida.org/datasets/as-organizations/',
            '''))
    parser.add_argument("-d", "--iplist-dir-path", required=True, type=pathlib.Path, help="Directory path where the IP list to be filtered are stored")
    parser.add_argument("-a", "--as-file", required=True, type=str, help='File containing AS info')
    parser.add_argument("-c", "--as-organisations-file", required=True, type=str,help='File containing AS Organization info')
    parser.add_argument("-p", "--input-filename-pattern", default="_input", type=str, help="input file name or pattern, default pattern is '_input'")
    parser.add_argument("-l", "--country-code", nargs='*', default = BLOCKLISTED_COUNTRY_CODES_LIST,help="country code or codes to be blocklisted")

    args = parser.parse_args()

    BLOCKLISTED_COUNTRY_CODES_LIST = args.country_code
    Total_IP_Removed = [ 0 for i in range(len(BLOCKLISTED_COUNTRY_CODES_LIST))]

    schedule_filter_searchlist_files_fn(args.iplist_dir_path, args.as_organisations_file, args.as_file, args.input_filename_pattern)


if __name__ == "__main__":
    freeze_support()
    main()
