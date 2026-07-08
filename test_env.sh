#!/bin/bash
# ==============================================================================
# Script Name: test_env.sh
# Description: Spins up 3 lightweight Python HTTP servers on different ports 
#              so you can test your Go load balancer locally.
# ==============================================================================

echo "Spinning up 3 dummy backend servers..."

# Create temporary directories for each server to serve different content
mkdir -p /tmp/backend1 /tmp/backend2 /tmp/backend3
echo "Response from Server 1 (Port 8081)" > /tmp/backend1/index.html
echo "Response from Server 2 (Port 8082)" > /tmp/backend2/index.html
echo "Response from Server 3 (Port 8083)" > /tmp/backend3/index.html

# Start Python HTTP servers in the background
cd /tmp/backend1 && python3 -m http.server 8081 > /dev/null 2>&1 &
PID1=$!
cd /tmp/backend2 && python3 -m http.server 8082 > /dev/null 2>&1 &
PID2=$!
cd /tmp/backend3 && python3 -m http.server 8083 > /dev/null 2>&1 &
PID3=$!

echo "Servers running on ports 8081, 8082, and 8083."
echo "Press [CTRL+C] to kill the servers and exit."

# Trap CTRL+C to clean up the background processes
trap "echo -e '\nShutting down backends...'; kill $PID1 $PID2 $PID3; exit" INT

# Keep script running
wait
