# #######################################
# #
# # Scan for the coaps using the libcoap
# #
# # NOTE: this design is not followed anymore
# #
# #######################################

import subprocess
import os, sys, getopt, time
from multiprocessing import Process, Queue, freeze_support


# When enabled the scan stops after scanning 'MAX_NUMBER_OF_IP_TO_SCAN' numbers
LIMIT_MAX_NUMBER_OF_IP_SCAN_FLAG = False

# Number of IPs to be there in the search list
MAX_NUMBER_OF_IP_TO_SCAN = 100000

# Batch size
PROCESS_BATCHSIZE = 5000

# For multiprocessing 
NUMBER_OF_PROCESSES = 500

input_file_path = ""
output_dir_path = ""

try:
    opts, args = getopt.getopt(sys.argv[1:],"hi:o:",["ifile=","ofile="])
except getopt.GetoptError:
    print ("scan_coaps.py -i <input file path> -o <output Directory path>")
    exit()

inputFileFlag  = False
outputDirFlag = False
for opt, arg in opts:
    if opt == '-h':
        print ("scan_coaps.py -i <input file path> -o <output Directory path>")
        exit()
    elif opt in ("-i", "--ifile"):
        input_file_path = arg
        inputFileFlag = True
    elif opt in ("-o", "--ofile"):
        output_dir_path = arg
        outputDirFlag = True

# check if mandatory the mandatory fields are there 
if (inputFileFlag == False) or (outputDirFlag == False):
    print ("scan_coaps.py -i <input file path> -o <output Directory path>")
    exit()

# Paths
SEARCHLIST_FILE_PATH =input_file_path
OUTPUT_IP_FILE_PATH=output_dir_path+"/coaps_5684_out.json"
OUTPUT_FILE_PATH=output_dir_path+"/coaps_5684.txt"
TEMP_FOLDER=output_dir_path+"/temp"



# create temporary folder
if not os.path.exists(TEMP_FOLDER):
    os.makedirs(TEMP_FOLDER)

# Function for scaning coap
def fn_coap(ip,fileName):
    try:
        file=TEMP_FOLDER+"/"+fileName+".txt"
        outfile= open(file,"w")
        # t = subprocess.call(["./coap-client","-m get", "coaps://"+ip+"/.well-known/core", "-B", "5" ,"-v", "9", "-o", "outcoap.txt"],stdout=outfile) 
        t = subprocess.call(["./coap-client -m get coaps://["+ip+"]/.well-known/core -B 10 -v 9 -o outcoap.txt"],stdout=outfile,shell=True) 
        outfile.write("\n\n**********END**********\n\n")
        outfile.close()
        if(t == 0):
            # For debugging purpose not deleting the output files of unsuccessful coap communication
            # os.remove(file)
            return ip, False
        return ip, True
    except Exception as e: 
        print("Error :",repr(e))


def worker(input, output):
    for func, args in iter(input.get, 'STOP'):
        ip,flag = func(*args)
        output.put((ip,flag))

def scan_coap():
    print("CoAPs scan begins ")
    try :
        searchFile=open(SEARCHLIST_FILE_PATH,"r")
    except Exception as e: 
        print(input_file_path, "Error ", repr(e))
        print("Script Failed, exiting..")
        return
    index = 0
    total_ip_scanned=0
    while True:
        start = time.time()
        TASKS1 = []
        if MAX_NUMBER_OF_IP_TO_SCAN == 0:
            break
        for ip in searchFile:
            index += 1
            total_ip_scanned += 1
            TASKS1.append((fn_coap, (ip.strip(), str(index))))
            if index == PROCESS_BATCHSIZE:
                break
            if (total_ip_scanned == MAX_NUMBER_OF_IP_TO_SCAN) and LIMIT_MAX_NUMBER_OF_IP_SCAN_FLAG :
                break
        # Create queues
        task_queue = Queue()
        done_queue = Queue()

        # Submit tasks
        for task in TASKS1:
            task_queue.put(task,timeout=15)

        # Start worker processes
        for i in range(NUMBER_OF_PROCESSES):
            Process(target=worker, args=(task_queue, done_queue)).start()
        batchSize = len(TASKS1)
        # Get and print results
        outfile_ip= open(OUTPUT_IP_FILE_PATH,"a")
        for i in range(len(TASKS1)):
            res = done_queue.get()
            if(res[1] == True):
                outfile_ip.write(res[0])
                outfile_ip.write("\n")
        outfile_ip.close()

        # Tell child processes to stop
        for i in range(NUMBER_OF_PROCESSES):
            task_queue.put('STOP')
        TASKS1.clear
        try:
            t_sbp=subprocess.Popen(["cat "+TEMP_FOLDER+"/*.txt >> "+OUTPUT_FILE_PATH],shell=True,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
            t_sbp.wait()
            t_sbp=subprocess.Popen(["rm "+TEMP_FOLDER+"/*.txt"],shell=True,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
            t_sbp.wait()
        except: 
            print("*")
        print("Batch of size", batchSize, "took", time.time() - start,"sec to finish")      

        if index != PROCESS_BATCHSIZE:
            break
        if (total_ip_scanned == MAX_NUMBER_OF_IP_TO_SCAN) and LIMIT_MAX_NUMBER_OF_IP_SCAN_FLAG :
            break
        index = 0
    print("Number of IPs scanned is :", total_ip_scanned)
    searchFile.close()
    os.rmdir(TEMP_FOLDER)

if __name__ == '__main__':
    freeze_support()
    start_ = time.time()
    scan_coap()
    print("Total time :", time.time() - start_)

