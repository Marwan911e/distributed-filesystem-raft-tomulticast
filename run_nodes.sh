#!/bin/bash

# Script to run all 4 nodes

echo "Starting Distributed File System Nodes..."

# Create storage directories
mkdir -p storage/node1 storage/node2 storage/node3 storage/node4

# Start Node 1
echo "Starting Node1 on port 1099..."
java -cp target/classes com.distributed.filesystem.NodeServer Node1 localhost 1099 ./storage/node1 Node2:localhost:1100 Node3:localhost:1101 Node4:localhost:1102 &
NODE1_PID=$!

sleep 2

# Start Node 2
echo "Starting Node2 on port 1100..."
java -cp target/classes com.distributed.filesystem.NodeServer Node2 localhost 1100 ./storage/node2 Node1:localhost:1099 Node3:localhost:1101 Node4:localhost:1102 &
NODE2_PID=$!

sleep 2

# Start Node 3
echo "Starting Node3 on port 1101..."
java -cp target/classes com.distributed.filesystem.NodeServer Node3 localhost 1101 ./storage/node3 Node1:localhost:1099 Node2:localhost:1100 Node4:localhost:1102 &
NODE3_PID=$!

sleep 2

# Start Node 4
echo "Starting Node4 on port 1102..."
java -cp target/classes com.distributed.filesystem.NodeServer Node4 localhost 1102 ./storage/node4 Node1:localhost:1099 Node2:localhost:1100 Node3:localhost:1101 &
NODE4_PID=$!

echo ""
echo "All nodes started!"
echo "Node PIDs: Node1=$NODE1_PID, Node2=$NODE2_PID, Node3=$NODE3_PID, Node4=$NODE4_PID"
echo ""
echo "To stop all nodes, run: kill $NODE1_PID $NODE2_PID $NODE3_PID $NODE4_PID"

# Wait for user interrupt
trap "kill $NODE1_PID $NODE2_PID $NODE3_PID $NODE4_PID; exit" INT TERM
wait

