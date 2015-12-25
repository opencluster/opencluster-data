#!/bin/bash

# set some users
./oc_set -m userid -k barney -V 1
./oc_set -m userid -k fred -V 2
./oc_set -m userid -k wilma -V 3
./oc_set -m userid -k betty -V 4

./oc_set -m username -K 1 -v barney
./oc_set -m email -K 1 -v barney\@example.com
./oc_set -m address -K 1 -v "132 wilma ave, farmtown"
./oc_set -m balance -K 1 -V 1933223

./oc_set -m username -K 2 -v fred
./oc_set -m email -K 2 -v fred\@example.com
./oc_set -m address -K 2 -v "653 Whoovile street"
./oc_set -m balance -K 2 -V 8373483

./oc_set -m username -K 3 -v wilma
./oc_set -m email -K 3 -v wilma\@example.com
./oc_set -m address -K 3 -v "5454 Market st"
./oc_set -m balance -K 3 -V 84844

./oc_set -m username -K 4 -v betty
./oc_set -m email -K 4 -v betty\@example.com
./oc_set -m address -K 4 -v "556 weheverher"
./oc_set -m balance -K 4 -V 34324

