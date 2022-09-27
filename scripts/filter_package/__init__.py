""" @package filer_package
    package with functionalities for filtering

    @author Peter Jose
"""

import SubnetTree
import json


def create_AS_country_dict_fn(as_organisation_file_path):
    """Function to create AS Country dictionary

    Args:
        as_organisation_file_path: Path to the AS Orgainisation file
    
    Returns:
        Dictionary with AS and country information 
    
    Raises:
        File open error
    """

    try:
        AS_country_dict = dict()
        with open(as_organisation_file_path,'r') as as_country_file:
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
                        if orgID in AS_country_dict.keys():
                            elem = AS_country_dict[orgID]
                            if country != '':
                                elem["country"] = country
                            if asn != '':
                                elem["asn"] = asn
                        AS_country_dict[orgID] = elem
            except Exception as e:
                pass
                #print("Error", repr(e))
        return AS_country_dict
    except Exception as e:
        raise e


def create_AS_tree_fn(AS_file_path):
    """Function to create AS tree function
    
    Args:
        AS_file_path: Path to the AS file
    
    Returns:
        AS and IP subnet tree 
    
    Raises:
        File open error
    """
    
    # subnet tree with AS info
    tree = SubnetTree.SubnetTree()
    try:
        with open(AS_file_path,'r') as AS_file:
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
    except Exception as e:
        raise e

    return tree