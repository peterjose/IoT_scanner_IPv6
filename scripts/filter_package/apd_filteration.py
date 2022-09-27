""" apd_filteration.py
    Provides functionalities for filtering based on aliased prefix detection

    @author Peter Jose
"""

import pathlib
import SubnetTree
import ipaddress
from enum import Enum

class state(Enum):
    NOT_INITIALISED = 0
    VALIDATED = 1
    INITIALISED = 2
    
    def __eq__(self, other):
        return self.__class__ is other.__class__ and other.value == self.value


class apd_filtering_option:
    """
    Aliased Prefix Detection filteration option
    """
    def __init__(self, apd_file='', non_apd_file=''):
        self.apd_file = apd_file
        self.non_apd_file = non_apd_file
        self.tree_apd = ''
        self.stateFlag = state.NOT_INITIALISED
        self.verifyIP = True    

    def validator(self):
        """Funtion to validate the module
        Args:
            Nil
        
        Returns:
            Nil
        
        Raises:
            File open error
        """
        
        if self.apd_file != '':
            if not pathlib.Path.is_file(pathlib.Path(self.apd_file)):
                raise Exception("File not present: " + self.apd_file)
        if self.non_apd_file != '':
            if not pathlib.Path.is_file(pathlib.Path(self.non_apd_file)):
                raise Exception("File not present: " + self.non_apd_file)
        if self.apd_file != '' and self.non_apd_file != '':
            self.stateFlag = state.VALIDATED
            print("Files used for APD filtering: ")
            print("    Aliased Prefixed IP list:", pathlib.Path(self.apd_file).resolve())
            print("    Non-Aliased Prefixed IP list:", pathlib.Path(self.non_apd_file).resolve())

    def deinitialise(self):
        """Funtion to deinitialise the module
        Args:
            Nil
        
        Returns:
            Nil
        
        Raises:
            Nil
        """
        
        self.apd_file = ''
        self.non_apd_file = ''
        self.tree_apd = ''
        self.stateFlag = state.NOT_INITIALISED
        self.verifyIP = True  


    def enable_verify_IP(self):
        """Function to enable the verify IP option

        """
        self.verifyIP = True


    def disable_verify_IP(self):
        """Function to disable the verify IP option

        """
        self.verifyIP = False


    def __fill_tree(self, file_path, suffix):
        """Function to fill the tree

        Args:
            tree: tree into which the 
            file_path: file path with IP addresses
            suffix: value to fill with
        
        Returns:
            Nil
        
        Raises:
            File open error
        """
        try:
            with open(file_path,'r') as file_ptr:
                for line in file_ptr:
                    line = line.strip()
                    try:
                        self.tree_apd[line] = line + suffix
                    except ValueError as e:
                        pass
                        # print("Skipped line '" + line + "'", file=sys.stderr)
        except Exception as e:
            raise e


    def initialise(self):
        """Function to inititalise module 

        Args:
            Nil
        
        Returns:
            Tree with APD info
        
        Raises:
            File open error
            ValidationError: Module not validated
        """
        # Read aliased and non-aliased prefixes
        if self.stateFlag == state.VALIDATED:
            self.tree_apd = SubnetTree.SubnetTree()
            try:
                self.__fill_tree(self.apd_file,",1")
                self.__fill_tree(self.non_apd_file, ",0")
                self.stateFlag = state.INITIALISED
                return self.tree_apd
            except Exception as e:
                raise e
        raise Exception("ValidationError: Module not validated")

    def check_apd_IP_fn(self,ip_address):
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

            # Alias prefix based filtering
            if self.tree_apd[ip_address][-1] == '1':
                # ip address is alised prefixed address
                return True
            else:
                return False
        raise Exception("InitialisationError: Module not initialised")  
# EOF