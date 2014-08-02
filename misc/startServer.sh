#!/bin/bash


#cd 
#cd dev/opencluster/server/ 
#ocd -l $(ifconfig | grep "inet addr" | head -n 1 | sed 's/:/ /g' | awk '{print $3}'):7777 -g logs/logs- -vvvvvvvv

# get the IP address of the current machine
IP=$(ip addr | grep "inet"|grep "scope global"|head -n 1 | awk '{print $2}' | awk -F/ '{print $1}')

# create a new conninfo file updated with the IP addr.
conninfo_set node_a.conninfo.src "ip" "$IP">node_a.conninfo

ocd -l node_a.conninfo -g logs/logs- -vvvvvvvv



