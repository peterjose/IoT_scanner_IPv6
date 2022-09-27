#######################################################################
#
# Script to extract saddr from csv output
#
#######################################################################

import sys, getopt
from csv import DictReader

input_file_name = ""
output_file_name = ""
try:
    opts, args = getopt.getopt(sys.argv[1:],"hi:o:",["ifile=","ofile="])
except getopt.GetoptError:
    print ("extract_saddr.py -i <inputfile> -o <outputfile>")
    exit(1)

inputFileFlag  = False
outputFileFlag = False
for opt, arg in opts:
    if opt == '-h':
        print ("extract_saddr.py -i <inputfile> -o <outputfile>")
        exit(1)
    elif opt in ("-i", "--ifile"):
        input_file_name = arg
        inputFileFlag = True
    elif opt in ("-o", "--ofile"):
        output_file_name = arg
        outputFileFlag = True

# check if mandatory the mandatory fields are there 
if (inputFileFlag == False) or (outputFileFlag == False):
    print ("extract_saddr.py -i <inputfile> -o <outputfile>")
    exit(1)

try :
    inputFile = open(input_file_name, 'r')
except Exception as e: 
    print(input_file_name, "Error ", repr(e))
    print("Script Failed, exiting..")
    exit()

try :
    outputfile = open(output_file_name, 'w')
except Exception as e: 
    print(output_file_name, "Error", repr(e))
    print("Script Failed, exiting..")
    exit()

# get the hitlist
csv_dict_reader = DictReader(inputFile)

# count of the IPs
ip_processed_count=0
# list matianed to remove the duplicates
ip_processed_list=[]

# iterate through the hitlist
for row in csv_dict_reader:
    ip = row['saddr'].split()[0]
    if ip:
        if not ip in ip_processed_list:
            ip_processed_list.append(ip)
            if(ip_processed_count):
                outputfile.write("\n")
            outputfile.write(ip)
            ip_processed_count+=1

inputFile.close()
outputfile.close()

# EOF