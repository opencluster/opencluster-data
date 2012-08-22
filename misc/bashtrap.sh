#!/bin/bash

# script to test interrupt trapping in bash, which will be used for some automated testing tools.

XXX=0

trap 'XXX=1' 2


while [ $XXX -eq 0 ] ; do

  echo "waiting..."
  sleep 1

done
echo
echo "Thankyou"
