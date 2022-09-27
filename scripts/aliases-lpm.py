#!/usr/bin/env python
#########################################################
#
# Script obtained from https://ipv6hitlist.github.io/#apd-lpm
# All the license and rights of this script belongs to the original Authors.
#
#########################################################

from __future__ import print_function

import argparse
import sys
import pathlib
import os

try:
    import SubnetTree
except Exception as e:
    print(e, file=sys.stderr)
    print("Use `pip install pysubnettree` to install the required module", file=sys.stderr)
    sys.exit(1)


def read_non_aliased(tree, fh):
    return fill_tree(tree, fh, ",0")

def read_aliased(tree, fh):
    return fill_tree(tree, fh, ",1")

def fill_tree(tree, fh, suffix):
    for line in fh:
        line = line.strip()
        try:
            tree[line] = line + suffix
        except ValueError as e:
            print("Skipped line '" + line + "'", file=sys.stderr)
    return tree


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-a", "--aliased-file", required=True, type=argparse.FileType('r'), help="File containing aliased prefixes")
    parser.add_argument("-n", "--non-aliased-file", required=True, type=argparse.FileType('r'), help="File containing non-aliased prefixes")
    parser.add_argument("-i", "--ip-address-file", required=True, type=argparse.FileType('r'), help="File containing IP addresses to be matched against (non-)aliased prefixes")
    parser.add_argument("-o", "--out-directory-path", required=True, type=pathlib.Path, help="Directory path where the output files to be stored")
    args = parser.parse_args()

    # Store aliased and non-aliased prefixes in a single subnet tree
    tree = SubnetTree.SubnetTree()

    # Read aliased and non-aliased prefixes
    tree = read_aliased(tree, args.aliased_file)
    tree = read_non_aliased(tree, args.non_aliased_file)

    # create the output directory if not present
    if not os.path.exists(args.out_directory_path):
        os.makedirs(args.out_directory_path)

    alias_out_file=open(args.out_directory_path.as_posix()+"/alias_output_ip.txt", "w")
    non_alias_out_file=open(args.out_directory_path.as_posix()+"/nonalias_output_ip.txt", "w")
    alias_ip_count=0
    non_alias_ip_count=0
    total_ip_processed=0
    # Read IP address file, match each address to longest prefix and print output
    for line in args.ip_address_file:
        line = line.strip()
        try:
            total_ip_processed += 1
        #    print(line + "," + tree[line][-1])
            if tree[line][-1] == '1':
                if alias_ip_count:
                   alias_out_file.write("\n")
                alias_out_file.write(line)
                alias_ip_count +=1
            else :
                if non_alias_ip_count:
                   non_alias_out_file.write("\n")
                non_alias_out_file.write(line)
                non_alias_ip_count += 1

        except KeyError as e:
            print("Skipped line '" + line + "'", file=sys.stderr)

    print("\nIP addresses processed\t", total_ip_processed)
    print("Aliased IP found \t", alias_ip_count)
    print("Non aliased IP found\t", non_alias_ip_count)
    # close files
    alias_out_file.close()
    non_alias_out_file.close()

if __name__ == "__main__":
    main()
