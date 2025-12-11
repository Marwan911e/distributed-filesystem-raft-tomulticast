@echo off
REM Script to run all 4 nodes on Windows

echo Starting Distributed File System Nodes...

REM Create storage directories
if not exist storage\node1 mkdir storage\node1
if not exist storage\node2 mkdir storage\node2
if not exist storage\node3 mkdir storage\node3
if not exist storage\node4 mkdir storage\node4

REM Start Node 1
echo Starting Node1 on port 1099...
start "Node1" cmd /k "java -cp target\classes com.distributed.filesystem.NodeServer Node1 localhost 1099 .\storage\node1 Node2:localhost:1100 Node3:localhost:1101 Node4:localhost:1102"

timeout /t 2 /nobreak >nul

REM Start Node 2
echo Starting Node2 on port 1100...
start "Node2" cmd /k "java -cp target\classes com.distributed.filesystem.NodeServer Node2 localhost 1100 .\storage\node2 Node1:localhost:1099 Node3:localhost:1101 Node4:localhost:1102"

timeout /t 2 /nobreak >nul

REM Start Node 3
echo Starting Node3 on port 1101...
start "Node3" cmd /k "java -cp target\classes com.distributed.filesystem.NodeServer Node3 localhost 1101 .\storage\node3 Node1:localhost:1099 Node2:localhost:1100 Node4:localhost:1102"

timeout /t 2 /nobreak >nul

REM Start Node 4
echo Starting Node4 on port 1102...
start "Node4" cmd /k "java -cp target\classes com.distributed.filesystem.NodeServer Node4 localhost 1102 .\storage\node4 Node1:localhost:1099 Node2:localhost:1100 Node3:localhost:1101"

echo.
echo All nodes started in separate windows!
echo Close the windows to stop the nodes.

