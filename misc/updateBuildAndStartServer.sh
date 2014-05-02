#!/bin/bash

cd 
cd dev/opencluster 
git pull
cd server/ 
sudo make clean
make debug
sudo make install
ocd -l $(ifconfig | grep "inet addr" | head -n 1 | sed 's/:/ /g' | awk '{print $3}'):7777 -g logs/logs- -vvvvvvvv
