""" blocklist_filteration.py
    Provides functionalities for filtering based on blocklist

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


class blocklist_filtering_option:
    """
    Block list filteration option
    """
    def __init__(self, blocklist_file=''):
        self.blocklist_file = blocklist_file
        self.tree_bl = ''
        self.stateFlag = state.NOT_INITIALISED
        self.verifyIP = True    

    def validator(self):
        """Funtion to initialise the module
        Args:
            Nil
        
        Returns:
            Nil
        
        Raises:
            File open error
        """
        if self.blocklist_file != '':
            if not pathlib.Path.is_file(pathlib.Path(self.blocklist_file)):
                raise Exception("File not present: " + self.blocklist_file)
            self.stateFlag = state.VALIDATED
            print("Files used for blocklist filtering: ")
            print("    Blocklisted IP list:", pathlib.Path(self.blocklist_file).resolve())


    def deinitialise(self):
        """Funtion to deinitialise the module
        Args:
            Nil
        
        Returns:
            Nil
        
        Raises:
            Nil
        """
        
        self.blocklist_file = ''
        self.tree_bl = ''
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


    def initialise(self):
        """Function to inititalise module

        Args:
            Nil
        
        Returns:
            Tree with blocklisted IP
        
        Raises:
            File open error
            ValidationError: Module not validated
        """
        if self.stateFlag == state.VALIDATED:
            self.tree_bl = SubnetTree.SubnetTree()
            try:
                with open(self.blocklist_file,'r') as file_ptr:
                    for ip_prefix in file_ptr:
                        ip_prefix=ip_prefix.split()
                        if ip_prefix:
                            try:
                                self.tree_bl.insert(ip_prefix[0])
                            except Exception as e:
                                pass 
                                # print("Ignoring failed to process entry", ip_prefix[0], "Error :", repr(e))
                self.stateFlag = state.INITIALISED
                return self.tree_bl
            except Exception as e:
                raise e
        raise Exception("ValidationError: Module not validated")

    def check_blocklisted_IP_fn(self,ip_address):
        """Function to check if an IP address is aliased prefix IP address 

        Args:
            ip_address: IP_address
        
        Returns:
            True: Blocklisted IP address
            False: Not blocklisted IP address

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
            if ip_address in self.tree_bl:
                # ip address is alised prefixed address
                return True
            else:
                return False
        raise Exception("InitialisationError: Module not initialised")
# EOF