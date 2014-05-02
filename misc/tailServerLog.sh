#!/bin/bash

tail -f ~/dev/opencluster/server/logs/$(ls -alFtr dev/opencluster/server/logs/ | tail -n 1 | awk '{print $NF}')
