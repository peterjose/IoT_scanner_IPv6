""" country_filteration.py
    Provides functionalities for filtering based on country

    @author Peter Jose
"""

import pathlib
import filter_package
import ipaddress
import SubnetTree
import geoip2.database
import pandas as pd
from enum import Enum

class state(Enum):
    NOT_INITIALISED = 0
    VALIDATED = 1
    INITIALISED = 2
    
    def __eq__(self, other):
        return self.__class__ is other.__class__ and other.value == self.value

class country_filtering_option:
    """
    Country filteration option
    """
    def __init__(self, as_file='', as_org_file='', country_code_list=''):
        self.as_file=as_file
        self.as_org_file = as_org_file
        self.country_code_list=country_code_list
        self.stateFlag = state.NOT_INITIALISED
        self.verifyIP = True 
        self.maxMind = False
        self.geoip_country = ''   
        self.tree_country = ''
        self.asn_filter_list = []


    def validator(self):
        """Funtion to initialise the module
        Args:
            Nil
        
        Returns:
            Nil
        
        Raises:
            File open error
        """
        
        if self.as_file != '':
            if not pathlib.Path.is_file(pathlib.Path(self.as_file)):
                raise Exception("File not present: "+self.as_file)
            if pathlib.PurePosixPath(self.as_file).suffix == '.mmdb':
                self.maxMind =  True
            elif self.as_org_file != '':
                if not pathlib.Path.is_file(pathlib.Path(self.as_org_file)):
                    raise Exception("File not present: "+self.as_org_file)
        if ((self.as_file != '' and self.as_org_file != '') 
                or self.maxMind == True) and len(self.country_code_list) != 0:
            self.stateFlag = state.VALIDATED
            print("Files used for Country based filtering: ")
            print("  of countries:",self.country_code_list )
            print("    AS-info list:", pathlib.Path(self.as_file).resolve())
            if not self.maxMind:
                print("    AS-org list:", pathlib.Path(self.as_org_file).resolve())


    def deinitialise(self):
        """Funtion to deinitialise the module
        Args:
            Nil
        
        Returns:
            Nil
        
        Raises:
            Nil
        """
        if(self.geoip_country):
            self
        self.as_file = ''
        self.as_org_file = ''
        self.country_code_list = ''
        self.stateFlag = state.NOT_INITIALISED
        self.verifyIP = True
        self.asn_filter_list = []


    def enable_verify_IP(self):
        """Function to enable the verify IP option

        """
        self.verifyIP = True


    def disable_verify_IP(self):
        """Function to disable the verify IP option

        """
        self.verifyIP = False


    def initialise(self):
        """Function to inititalise module

        Args:
            Nil 

        Returns:
            Tree with country info
        
        Raises:
            File open error
            ValidationError: Module not validated
        """
        if self.stateFlag == state.VALIDATED:
            self.tree_country = SubnetTree.SubnetTree()
            # Read aliased and non-aliased prefixes
            try:
                if self.maxMind:
                    self.geoip_country = geoip2.database.Reader(self.as_file)
                    self.stateFlag = state.INITIALISED
                    return
                else:
                    df = pd.DataFrame.from_dict(filter_package.create_AS_country_dict_fn(
                                    self.as_org_file),orient='index').reset_index() 

                    for country_code in self.country_code_list:
                        data = df[(df["country"]==country_code)]["asn"].to_list()
                        if len(data):
                            self.asn_filter_list.append(data)
                        
                    self.tree_country = filter_package.create_AS_tree_fn(self.as_file)
                    self.stateFlag = state.INITIALISED
                    return self.tree_country
            except Exception as e:
                raise e
        raise Exception("ValidationError: Module not validated")
   

    def check_country_IP_fn(self,ip_address):
        """Function to check if an IP address is aliased prefix IP address 

        Args:
            ip_address: IP_address
        
        Returns:
            True: Alised prefixed IP address
            False: Not alised prefixed IP address

        Raises:
            Initialisation Error: 
        """
        if self.stateFlag == state.INITIALISED:
            if(self.verifyIP):
                try:
                    # Check if the ip is proper or not
                    # currently not checking if IPv6 or IPv4
                    ipaddress.ip_address(ip_address)
                except Exception as e:
                    raise e
            # if max mind based filteration
            if self.maxMind:
                try:
                    ip_country_code = self.geoip_country.country(ip_address).country.iso_code
                    for country_code in self.country_code_list:
                        if country_code == ip_country_code:
                            return True
                    return False
                except Exception as e:
                    return False
            else:
                try:
                    AS_info = self.tree_country[ip_address].split(",")
                    for asn in AS_info:
                        for asn_filter_list_individual in self.asn_filter_list:
                            if asn in asn_filter_list_individual:
                                return True
                    return False

                except Exception as e:
                # print("Skipped line '" + line +":",repr(e))
                    pass
                    return False
        raise Exception("InitialisationError: Module not initialised")
# EOF