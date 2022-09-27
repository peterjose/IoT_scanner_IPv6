#!/usr/bin/env python
"""
    Script to filter the hitlist, using the block list
    Script is designed to work with python 3

"""

import os
import sys
import subprocess
import pkg_resources
import random
from csv import DictReader

############################# CONFIG START #############################

# Flag set to stop the filtering when it has found the MAX_NUMBER_OF_IP for the search list
LIMIT_THE_IP_FILTERED_FLAG = False               # True / False

# Enable the flag to print the debug statments
ENABLE_DEBUG_PRINT_FLAG = True                  # True / False

# Flag to allow the installation of missing packages
ALLOW_INSTALLATION_OF_MISSING_PKGS_FLAG = False # True / False

# Flag to find unique and randomise the ip list
ENABLE_UNIQUE_RANDOMISE_FLAG = False

# Number of IPs to be there in the search list
MAX_NUMBER_OF_IP = 100000

# Paths
BLOCKLIST_FILE_PATH  = "../blocklist/release/ipv6-bl-merged.txt"
HITLIST_FILE_PATH    = "../searchlist/ipv6/hitlist/nonalias_output_ip.txt"
SEARCHLIST_FILE_PATH = "../searchlist/ipv6/searchlist.txt"
SEARCHLIST_FILE_PATH_TEMP = SEARCHLIST_FILE_PATH + "_tmp" 

############################# CONFIG  STOP #############################

# Check if the required modules for the script are installed
required = {'pysubnettree','ipaddress'}
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

# Import the pysubnettree
import SubnetTree
import ipaddress

if ENABLE_DEBUG_PRINT_FLAG:
    print("Script to filter the hitlist, using the block list to create a search list")
    print("Hitlist is       " , HITLIST_FILE_PATH)
    print("Blocklist is     " , BLOCKLIST_FILE_PATH)
    print("Searchlist is    " , SEARCHLIST_FILE_PATH)
    print("Please wait\n")

# Open files
try :
    blocklistFile = open(BLOCKLIST_FILE_PATH, 'r')
except Exception as e: 
    print(BLOCKLIST_FILE_PATH, "Error ", repr(e))
    print("Script Failed, exiting..")
    exit()
try :
    hitlistFile = open(HITLIST_FILE_PATH, 'r')
except Exception as e: 
    print(HITLIST_FILE_PATH, "Error", repr(e))
    print("Script Failed, exiting..")
    exit()
try :
    searchlistFile_temp=open(SEARCHLIST_FILE_PATH_TEMP,"w+")
except Exception as e: 
    print(SEARCHLIST_FILE_PATH, "Error", repr(e))
    print("Script Failed, exiting..")
    exit()

# read the lines from the blocklist
ip_prefixs = blocklistFile.readlines()

# creating the prefix tree
t_list = SubnetTree.SubnetTree()
for ip_prefix in ip_prefixs:
    ip_prefix=ip_prefix.split()
    if ip_prefix:
        try:
            t_list.insert(ip_prefix[0])
        except Exception as e: 
            print("Ignoring failed to process entry", ip_prefix[0], "Error :", repr(e))

del ip_prefixs
del ip_prefix
# close block list file
blocklistFile.close()

# count of the IPs
ip_processed_count=0
removed_basedon_blocklist=0
ip_filtered_count=0
saved_ip_count=0
fileType_csv = True

# to find the file name extension
filename_extension = HITLIST_FILE_PATH.split('.')
if filename_extension[-1] == "csv":
    # get the hitlist
    hitlist_reader = DictReader(hitlistFile)
else :
    fileType_csv = False
    # read the lines from the blocklist
    hitlist_reader = hitlistFile

# iterate through the hitlist
for row in hitlist_reader:
    ip_processed_count+=1
    if fileType_csv:
        hitlist_ip = row['saddr'].split()[0]
    else:
        hitlist_ip = row.split()[0]
    try :
        if hitlist_ip:
            ipaddress.ip_address(hitlist_ip)
            # check if the ip is present in the block list
            if not ( hitlist_ip in t_list):
                searchlistFile_temp.write(hitlist_ip)
                searchlistFile_temp.write("\n")
            else :
                removed_basedon_blocklist += 1          
    except Exception as e: 
        print("Ignoring failed to process entry", hitlist_ip, "Error :",repr(e))

# del hitlist_reader
del t_list
hitlistFile.close()

if ENABLE_UNIQUE_RANDOMISE_FLAG:
    if ENABLE_DEBUG_PRINT_FLAG:
        print("Randomising the search list")

    try :
        searchlistFile = open(SEARCHLIST_FILE_PATH,"w")
    except Exception as e: 
        print(SEARCHLIST_FILE_PATH, "Error", repr(e))
        print("Script Failed, exiting..")
        exit()
    
    # Randomise the list
    searchlist_ip_set = set()
    searchlistFile_temp.seek(0)
    # searchlist_ip_set = {line.strip() for line in searchlistFile_temp}
    for line in searchlistFile_temp:
        searchlist_ip_set.add(line.strip())
        ip_filtered_count += 1
    searchlist_ip_list = list(searchlist_ip_set)
    random.shuffle(searchlist_ip_list)
    for searchlist_ip in searchlist_ip_list:
        # write the ip into a new file
        if saved_ip_count:
            searchlistFile.write("\n")
        searchlistFile.write(searchlist_ip)
        saved_ip_count += 1
        if (saved_ip_count == MAX_NUMBER_OF_IP) and (LIMIT_THE_IP_FILTERED_FLAG == True):
            break
        searchlistFile.close()

# close the files
searchlistFile_temp.close()

# Removing the temporary file created
if os.path.exists(SEARCHLIST_FILE_PATH_TEMP) and ENABLE_UNIQUE_RANDOMISE_FLAG:
    os.remove(SEARCHLIST_FILE_PATH_TEMP)
else:
    print("output is stored in", SEARCHLIST_FILE_PATH_TEMP)    

# Print the status
if ENABLE_DEBUG_PRINT_FLAG:
    print("\nIP addresses processed", ip_processed_count)
    print("IP address removed based on blocklist",removed_basedon_blocklist)
    if ENABLE_UNIQUE_RANDOMISE_FLAG:
        print("IP addresses filtered ", ip_filtered_count)
        print("Unique IP addresses saved to search list", saved_ip_count)

# EOF
