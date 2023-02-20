"""
    @file pre_scan_data_fetcher.py

    File to fetch DB from TUM hitlist 

    @author Peter Jose

"""

import re
import requests
import argparse
import pathlib
import subprocess
import json
import textwrap

# DB link and authentication details
HITLIST_DATABASE_BASE_LINK = "https://alcatraz.net.in.tum.de/ipv6-hitlist-service/registered"
HITLIST_APD_PAGE_EXT   = "/apd"
HITLIST_INPUT_PAGE_EXT = "/input"
HITLIST_USER_AUTH_NAME = ""
HITLIST_USER_AUTH_PWD  = ""
HITLIST_USER_AUTH_NAME = "YOUR_USER_NAME"
HITLIST_USER_AUTH_PWD  = "YOUR_USER_PASSWORD"

# file suffix given in the TUM website
DB_INPUT_FILE_NAME_SUFFIX   = "input.txt.xz"
DB_APD_FILE_NAME_SUFFIX     = "aliased.txt.xz"
DB_NON_APD_FILE_NAME_SUFFIX = "nonaliased.txt.xz" 

# storage location directory structure
# APD_DB_DIR = "apd_TUM"           
# INPUT_DB_DIR = "hitlist_TUM_input"
# ADDITIONAL_EXTERNAL_DB_FILE_DIR = "additional_external_db"

# DB fetch and extract Timeout
DB_FILE_FETCH_AND_EXTRACT_TIMEOUT = 1200 # seconds

# Maxmind Geolite
MAX_MIND_GEOLITE2_DB_COMMON_PREFIX  = 'GeoLite2-' 
MAX_MIND_GEOLITE2_ASN_DB_NAME       = 'GeoLite2-ASN'
MAX_MIND_GEOLITE2_COUNTRY_DB_NAME   = 'GeoLite2-Country'

MAX_MIND_LICENSE_KEY   = ''

MAX_MIND_AS_DB_LINK      = 'https://download.maxmind.com/app/geoip_download?edition_id=GeoLite2-ASN&license_key='+MAX_MIND_LICENSE_KEY+'&suffix=tar.gz'
MAX_MIND_COUNTRY_DB_LINK = 'https://download.maxmind.com/app/geoip_download?edition_id=GeoLite2-Country&license_key='+MAX_MIND_LICENSE_KEY+'&suffix=tar.gz'

# ASDB from stanford
CATEGORIZED_ASES_DATABASE_BASE_LINK  = "https://asdb.stanford.edu/"
CATEGORIZED_ASES_DB_FILE_NAME_SUFFIX = "categorized_ases.csv"

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

def webpath_finder(link, auth_username = '',auth_pass = '', regex = None, fromTop=False):
    """"Function developed to scroll through a list from the 
        webpage for a first occrence of pattern 

    Args:
        link: webpage link to search
        auth_username: user name to be used
        auth_pass: user password 
        regex: pattern to search
        fromTop: the direction from which search should be started
    
    Returns:
        matched item, if match is found
        None, if match is not found
    
    Raises:
        Exception: Database not reachable
    """
    if (auth_username != '') and (auth_pass != ''):
        response = requests.get(link, auth=(auth_username, auth_pass),timeout=20)
    else:
        response = requests.get(link,timeout=20)
    if response.status_code != 200:
        raise Exception("Database not reachable, link", link)

    web_content = response.text
    data_list = web_content.strip().split("\n")

    if not fromTop:
        data_list = reversed(data_list)

    if regex != None:
        for link_data in data_list:
            match = re.search(regex,link_data)
            if match != None:
                return match.group(0)
    return None

def get_latest_hitlist_related_DB_path():
    """ Function to get the path to the latest DB
    Args:
        Nil
    
    Returns:
        Link to latest input file in database,
        Link to APD file in database,
        Link to Non APD file in database
    
    Raises:
        Exception: Database not reachable
    """
    try:
        apd_path_yyyy_mm = webpath_finder(HITLIST_DATABASE_BASE_LINK + HITLIST_APD_PAGE_EXT,
                                HITLIST_USER_AUTH_NAME,
                                HITLIST_USER_AUTH_PWD,
                                '\d{4}-\d{2}/')
        # print(apd_path_yyyy_mm)

        apd_file_prefix = webpath_finder(HITLIST_DATABASE_BASE_LINK + HITLIST_APD_PAGE_EXT + '/' + apd_path_yyyy_mm,
                                    HITLIST_USER_AUTH_NAME,
                                    HITLIST_USER_AUTH_PWD,
                                    '\d{4}-\d{2}-\d{2}-')
        # print(apd_file_prefix)

        input_path_yyyy_mm = webpath_finder(HITLIST_DATABASE_BASE_LINK + HITLIST_INPUT_PAGE_EXT,
                                HITLIST_USER_AUTH_NAME,
                                HITLIST_USER_AUTH_PWD,
                                '\d{4}-\d{2}/')
        # print(input_path_yyyy_mm)

        input_file_prefix = webpath_finder(HITLIST_DATABASE_BASE_LINK + HITLIST_INPUT_PAGE_EXT + '/' + input_path_yyyy_mm,
                                    HITLIST_USER_AUTH_NAME,
                                    HITLIST_USER_AUTH_PWD,
                                    '\d{4}-\d{2}-\d{2}-')
        # print(input_file_prefix)
        base_link = HITLIST_DATABASE_BASE_LINK
        if (HITLIST_USER_AUTH_NAME != '' and HITLIST_USER_AUTH_PWD != ''):
            base_link = HITLIST_DATABASE_BASE_LINK.replace('https://','https://'+HITLIST_USER_AUTH_NAME+':'+HITLIST_USER_AUTH_PWD+'@') 
        return (base_link + HITLIST_INPUT_PAGE_EXT + '/' + input_path_yyyy_mm + input_file_prefix + DB_INPUT_FILE_NAME_SUFFIX,
                base_link + HITLIST_APD_PAGE_EXT + '/' + apd_path_yyyy_mm + apd_file_prefix + DB_APD_FILE_NAME_SUFFIX,
                base_link + HITLIST_APD_PAGE_EXT + '/' + apd_path_yyyy_mm + apd_file_prefix + DB_NON_APD_FILE_NAME_SUFFIX)
    
    except Exception as e:
        raise e

def DB_downloader(DB_file_download_storgae_location, 
                    DB_file_fetch_from_location,
                    DB_file_type="xz",
                    DB_Download_file_name = None,
                    timeout=DB_FILE_FETCH_AND_EXTRACT_TIMEOUT):
    """ Function to download and extract the DB files
    Args:
        DB_file_download_storgae_location: the location in which the downloaded content should be placed
        DB_file_fetch_from_location: the link from which the database should be downloaded
        DB_file_type: the type of file to be downloaded
        DB_Download_file_name: file name of the downloaded file
        timeout: subprocess timeout, default value is DB_FILE_FETCH_AND_EXTRACT_TIMEOUT
    
    Returns:
        Link to latest input file in database,
        Link to APD file in database,
        Link to Non APD file in database,
    
    Raises:
        Nil
    """
    # Extraction options 
    decompress_cmd = ''
    extract_file_name = '*.'+DB_file_type
    if DB_Download_file_name != None:
        extract_file_name = DB_Download_file_name
    if DB_file_type == 'xz':
        decompress_cmd = ' && unxz '+extract_file_name
    elif DB_file_type == 'tar.gz':
        decompress_cmd = ' && tar -xzf '+extract_file_name
    elif DB_file_type == 'csv':
        pass
    else:
        raise Warning("Warning: Decompression not applicable")

    download_file_name = ''
    if DB_Download_file_name != None:
        download_file_name = ' -O '+DB_Download_file_name 
    subprocess.call(["cd "+ DB_file_download_storgae_location+";"+
        "wget "+DB_file_fetch_from_location+download_file_name+
        " --no-check-certificate" + decompress_cmd],shell=True, timeout=timeout)
    # subprocess.call(["cd "+ DB_file_download_storgae_location],shell=True, timeout=timeout)

def argument_parser_fn():
    """Function to to parse the arguments received by the program
    Args:
        Nil
    
    Returns:
        parsed arguments list

    Raises:
        Nil
    """
    parser = argparse.ArgumentParser(prog='pre_scan_data_fetcher.py',
             formatter_class=argparse.RawDescriptionHelpFormatter,
            description=textwrap.dedent('''\
            \rProgram to do multistage filteration of IP list
            \r--------------------------------
            \rHitlist, apd, non apd is obtained from https://ipv6hitlist.github.io/
            \nGeoIP country and ASN is obtained from MaxMind
            \nAS category DB from stanford is obtained from https://asdb.stanford.edu/
            '''))

    parser.add_argument("-s", "--hitlist-dir", required=True, type=pathlib.Path, 
                            help="Directory to store the histlist")
    parser.add_argument("-a", "--apd-dir", required=True, type=pathlib.Path, 
                            help="Directory to store the apd related DB")
    parser.add_argument("-d", "--db-storage-path-list", required=True, type=pathlib.Path, 
                            help="json file with the list of storage location created")
    parser.add_argument("-e", "--other-db-dir", type=pathlib.Path, 
                            help="Directory to store additional external DBs")

    return parser.parse_args()

def create_dir(base_parent_path,child_path):
    """Create a directory from the based Path if not present
    Args:
        base_parent_path: the base path from which reset of the path should be created
        child_path: path that has to be created form the base parent path
    
    Returns:
        complete created path

    Raises:
        Error: Invalid Base Parent Path, when base path is not a valid directory
    """
    # continue only if the base path is present else throw and exception
    if(pathlib.Path(base_parent_path).is_dir()):
        complete_path = pathlib.Path(base_parent_path,child_path)
        # interatively build the child directories
        if not pathlib.Path.is_dir(complete_path):
            path_list = str(complete_path).split('/')
            p = pathlib.Path()
            for i in range(0,len(path_list)):
                p = pathlib.Path(p,path_list[i])
                if not pathlib.Path.is_dir(p):
                    pathlib.Path.mkdir(p)
        return complete_path
    else:
        raise Exception("Error: Invalid Base Parent Path", base_parent_path)


def get_TUM_db(args,database_location_json):
    """Function to get hitlist and ASN
    Args:
        args: args list
        database_location_json: json object that contain the database location info
    
    Returns:
        database_location_json

    Raises:
        Error: Not valid directory    
    """
    # get the links for the DB related to hitlist
    input_raw_histlist_db_link, apd_db_link, non_apd_db_link = get_latest_hitlist_related_DB_path()
    if not args.hitlist_dir.is_dir():
        raise Exception("Not valid directory:",args.hitlist_dir)
    DB_downloader(str(args.hitlist_dir.resolve()),input_raw_histlist_db_link)
    hitlist_file = ([x for x in args.hitlist_dir.iterdir() 
                        if (x.is_file() and x.name.find((input_raw_histlist_db_link.split('/')[-1]).split('.')[0]) != -1) and x.suffix != '.xz'])  
    database_location_json.update({RAW_HITLIST_KEY:str(hitlist_file[0].resolve())})


    if not args.apd_dir.is_dir():
        raise Exception("Not valid directory:",args.apd_dir)
    adp_db_storage_path = str(args.apd_dir.resolve())
    DB_downloader(adp_db_storage_path,apd_db_link)
    apd_file = ([x for x in args.apd_dir.iterdir() 
                    if (x.is_file() and x.name.find((apd_db_link.split('/')[-1]).split('.')[0]) != -1) and x.suffix != '.xz'])
        
    database_location_json.update({APD_KEY:str(apd_file[0].resolve())})
    
    DB_downloader(adp_db_storage_path,non_apd_db_link)
    non_apd_file = ([x for x in args.apd_dir.iterdir() 
                if (x.is_file() and x.name.find((non_apd_db_link.split('/')[-1]).split('.')[0]) != -1) and x.suffix != '.xz'])
        
    database_location_json.update({NON_APD_KEY:str(non_apd_file[0].resolve())})

    return database_location_json
        
def get_additional_db(args,database_location_json):
    """Function to get all the additional db
    Args:
        args: args list
        database_location_json: json object that contain the database location info
    
    Returns:
        database_location_json

    Raises:
        Error: Not valid directory  
    """
    # if the output dir is present then download the maxmind DB and also AS categories
    if (args.other_db_dir != None):
        if not args.other_db_dir.is_dir():
            raise Exception("Not valid directory:",args.other_db_dir)
        additional_ext_db_path = str(args.other_db_dir.resolve())
        DB_downloader(additional_ext_db_path, 
                        "\""+MAX_MIND_COUNTRY_DB_LINK+"\"", 
                        DB_Download_file_name=MAX_MIND_GEOLITE2_COUNTRY_DB_NAME+'.tar.gz', 
                        DB_file_type='tar.gz' )
        geoip_country_dir = ([x for x in args.other_db_dir.iterdir() 
                    if (x.is_dir() and x.name.find(MAX_MIND_GEOLITE2_COUNTRY_DB_NAME) != -1)])
        print(geoip_country_dir)
        geoip_country_file = ([x for x in geoip_country_dir[0].iterdir() 
                    if (x.is_file() and x.name.find(MAX_MIND_GEOLITE2_DB_COMMON_PREFIX) != -1) and x.suffix == '.mmdb'])
        print(geoip_country_file)

        database_location_json.update({MAXMIND_GEOIP_COUNTRY_KEY:str(geoip_country_file[0].resolve())})        


        DB_downloader(additional_ext_db_path, 
                        "\""+MAX_MIND_AS_DB_LINK+"\"", 
                        DB_Download_file_name=MAX_MIND_GEOLITE2_ASN_DB_NAME+'.tar.gz',                            
                        DB_file_type='tar.gz')
        geoip_asn_dir = ([x for x in args.other_db_dir.iterdir() 
                    if (x.is_dir() and x.name.find(MAX_MIND_GEOLITE2_ASN_DB_NAME) != -1)])
        geoip_asn_file = ([x for x in geoip_asn_dir[0].iterdir() 
                    if (x.is_file() and x.name.find(MAX_MIND_GEOLITE2_DB_COMMON_PREFIX) != -1) and x.suffix == '.mmdb'])
            
        database_location_json.update({MAXMIND_GEOIP_ASN_KEY:str(geoip_asn_file[0].resolve())})  
        
        
        asn_cat_db_link = webpath_finder(CATEGORIZED_ASES_DATABASE_BASE_LINK+"#data", 
                                regex='\d{4}-\d{2}_'+CATEGORIZED_ASES_DB_FILE_NAME_SUFFIX)
        DB_downloader(additional_ext_db_path,
                        CATEGORIZED_ASES_DATABASE_BASE_LINK+'data/'+asn_cat_db_link,
                        DB_file_type='csv')
        asn_cat_file = ([x for x in args.other_db_dir.iterdir() 
                    if (x.is_file() and x.name.find(CATEGORIZED_ASES_DB_FILE_NAME_SUFFIX) != -1)])
            
        database_location_json.update({ASN_CATEGORY_KEY:str(asn_cat_file[0].resolve())})  

    return database_location_json

def main():
    """Function main

    """
    try:
        # process the arguments
        args = argument_parser_fn()
        
        database_location_json = {}
        # read the file with the list of the database location
        if args.db_storage_path_list.is_file() :
            with open(args.db_storage_path_list, 'r') as file:
                database_location_json = json.load(file)
        
        database_location_json = get_TUM_db(args,database_location_json)
        database_location_json = get_additional_db(args,database_location_json)

        # store back the json with the list of the database
        with open(args.db_storage_path_list, 'w') as f:
            json.dump(database_location_json, f)

    except Exception as e:
        print(e)

if __name__ == "__main__":
    exit(main())

# EOF