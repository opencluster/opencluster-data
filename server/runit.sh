#!/bin/bash

# The gamut test is a number of operations that are designed to test that everything is functioning completely.

# Remove the existing logs from tmp
rm /tmp/a-logs*;
rm /tmp/b-logs*;

make debug || (echo "Build Failed"; exit 1)

echo "Starting Node-A" 
./ocd.debug -vvvvvv -g /tmp/a-logs- &
PID_NODE_A=$!

sleep 6

echo "Starting Node-B" 
./ocd.debug -vvvvvv -l 127.0.0.1:13601 -n 127.0.0.1:13600 -g /tmp/b-logs- &
PID_NODE_B=$!

sleep 5

killall -SIGHUP ocd.debug


# dialog --menu hello 10 40 3 one one two two three three 2>>res.txt && (RESULT=$(cat res.txt); rm res.txt; clear; echo "RESULT: $RESULT" )

echo "Node-A PID: $PID_NODE_A"
echo "Node-B PID: $PID_NODE_B"
echo "Press ENTER to shutdown the cluster"
read

kill -SIGINT $PID_NODE_A
echo "Waiting for Node-A to exist"
wait $PID_NODE_A

kill -SIGINT $PID_NODE_B
echo "Waiting for Node-B to exist"
wait $PID_NODE_B

echo "Shutdown complete"

