#!/bin/bash

cd 
cd dev/opencluster/server/ 
ocd -l $(ifconfig | grep "inet addr" | head -n 1 | sed 's/:/ /g' | awk '{print $3}'):7777 -g logs/logs- -vvvvvvvv
