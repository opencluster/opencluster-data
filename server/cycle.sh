#!/bin/bash

# This script will automatically and randomly add and remove nodes from the cluster.  This will be
# used with other testing scripts and tools, to ensure that the data retains integrity and
# accessibility even when nodes are joining and leaving the cluster resulting in data being moved
# around the cluster.

# Arrays in Bash are done like:
#   Somearray[2]="something"
#
# and to access the data from the array:
#   echo ${Somearray[2]}


# Notes:
#   1.  When adding nodes, we need to give an IP of a node that is already in the cluster, so that 
#       it can connect to it.  If we remove a node, we need to be sure that we can connect to a 
#       different node instead.
#
#   2.  Need to make sure that there is always at least one node in the cluster.
#
#   3.  We may eventually want to try closing multiple nodes at a time, but for starters, have it 
#       only doing one thing at a time.  When we do close multiple nodes at a time, we need to make 
#       sure that we keep things in track.

XXX=0



# Remove the existing logs from tmp
rm /tmp/cycle-logs-*;

make debug || (echo "Build Failed"; exit 1)

BASE_PORT=13600
NODES=0
LOG_SETTINGS=-vvvvvv
function logfile () {
  printf "/tmp/cycle-logs-%04d-" $1
}



# before we start the random process, we need to have at least one node in the cluster already 
# started and running with all the buckets.

echo "Starting Initial server Node" 

echo "./ocd.debug -g $(logfile $NODES) $LOG_SETTINGS &"
sleep 2
./ocd.debug -g $(logfile $NODES) $LOG_SETTINGS &
NODE[$NODES]=$!
NODES=$[NODES+1]
sleep 6
echo "Node-0000 PID: ${NODE[0]}"

echo "Press ENTER to start another node"
read

echo "./ocd.debug -g $(logfile $NODES) $LOG_SETTINGS &"
sleep 2
./ocd.debug -l 127.0.0.1:13601 -n 127.0.0.1:13600 -g $(logfile $NODES) $LOG_SETTINGS &
NODE[$NODES]=$!
NODES=$[NODES+1]
sleep 6
echo "Node-0001 PID: ${NODE[1]}"






echo "Press ENTER to start Cycling the server nodes randomly."
read

trap 'XXX=1' 2

while [ $XXX -eq 0 ] ; do
	echo "Sleeping..."
	sleep 1
done


# the user has indicated to exit, so we need to shutdown all the nodes.
while [ $NODES -gt 0 ]; do

	NODES=$[NODES-1]

	PID=${NODE[$NODES]}
	if [ $PID -gt 0 ]; then


		echo "Stopping Node-$NODES (PID: $PID)"
		kill -SIGINT $PID
		echo "Waiting for Node-$NODES to exit."
		wait $PID
		echo

	fi
done

echo "Shutdown complete"

