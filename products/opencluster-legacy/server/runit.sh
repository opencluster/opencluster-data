#!/bin/bash

# The gamut test is a number of operations that are designed to test that everything is functioning completely.


function ticker() {
  local LIMIT=$1
  for II in `seq 1 $LIMIT`; do echo -n "."; sleep 1; done
  echo
}


# Remove the existing logs from tmp
rm /tmp/a-logs*;
rm /tmp/b-logs*;

test -e ocd.debug.b && rm $_

make debug || exit 1
ln ocd.debug ocd.debug.b

echo "Starting Node-A"
./ocd.debug -vvvvvv -l data/node_a.conninfo -a data/server_auth -g /tmp/a-logs- &
PID_NODE_A=$!
ticker 2

echo "Starting Node-B"
./ocd.debug.b -vvvvvv -l data/node_b.conninfo -a data/server_auth -n data/node_a.conninfo -g /tmp/b-logs- &
PID_NODE_B=$!
ticker 5

killall -SIGHUP ocd.debug
killall -SIGHUP ocd.debug.b


# dialog --menu hello 10 40 3 one one two two three three 2>>res.txt && (RESULT=$(cat res.txt); rm res.txt; clear; echo "RESULT: $RESULT" )

echo "Node-A PID: $PID_NODE_A"
echo "Node-B PID: $PID_NODE_B"
echo "Press ENTER to shutdown the cluster"
read

kill -SIGINT $PID_NODE_A
echo "Waiting for Node-A to exist"
wait $PID_NODE_A
sleep 1

kill -SIGINT $PID_NODE_B
echo "Waiting for Node-B to exist"
wait $PID_NODE_B

echo "Shutdown complete"

