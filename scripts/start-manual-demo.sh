#!/bin/bash
# Manual Demo Mode — TrafficStreamingApp
#
# Use this script to demonstrate Spark Streaming interactively during a live defense.
# You manually type traffic records into Terminal 2; Spark processes them in real time
# and prints results in Terminal 1.
#
# ┌─ WORKFLOW ────────────────────────────────────────────────────────────────────┐
# │                                                                               │
# │  Terminal 1 (Spark):   bash scripts/start-streaming.sh                       │
# │                        (submit the streaming job — waits for data)           │
# │                                                                               │
# │  Terminal 2 (Input):   bash scripts/start-manual-demo.sh                     │
# │                        (opens a netcat server inside spark-master)            │
# │                        Type one record per line and press Enter.              │
# │                                                                               │
# │  Record format:        timestamp,weather,traffic_volume                      │
# │  Example:              2026-01-01 08:00:00,Rain,8500                         │
# │                        2026-01-01 09:00:00,Clear,4200                        │
# │                        2026-01-01 10:00:00,Clouds,6100                       │
# │                                                                               │
# │  Congestion trigger:   traffic_volume > 6000                                 │
# └───────────────────────────────────────────────────────────────────────────────┘
#
# Prerequisites:
#   1. docker-compose up -d           — all 6 containers running
#   2. bash scripts/start-spark.sh    — Spark Master + Workers running
#   3. bash scripts/start-streaming.sh  (Terminal 1 — run BEFORE this script)

set -e

PORT="${1:-9999}"

echo ""
echo "========================================================"
echo "  Smart Traffic Monitoring — Manual Demo Mode"
echo "========================================================"
echo ""
echo "  This opens a TCP server on spark-master:$PORT."
echo "  The Spark Streaming app (Terminal 1) will connect."
echo ""
echo "  Record format:  timestamp,weather,traffic_volume"
echo ""
echo "  Example records to try:"
echo "    2026-01-01 08:00:00,Rain,8500      <- CONGESTION ALERT"
echo "    2026-01-01 09:00:00,Clear,4200     <- normal"
echo "    2026-01-01 10:00:00,Clouds,6100    <- CONGESTION ALERT"
echo "    2026-01-01 11:00:00,Snow,3800      <- normal"
echo "    2026-01-01 12:00:00,Fog,7200       <- CONGESTION ALERT"
echo ""
echo "  Type a record and press Enter. Watch Terminal 1 for output."
echo "  Press Ctrl-C to stop."
echo "========================================================"
echo ""

# Kill any previous netcat / producer holding the port
docker exec spark-master bash -c "
  PID=\$(fuser ${PORT}/tcp 2>/dev/null | awk '{print \$1}')
  if [ -n \"\$PID\" ]; then
    echo '[setup] Clearing port $PORT (PID '\$PID')...'
    kill \"\$PID\" 2>/dev/null || true
    sleep 1
  fi
" 2>/dev/null || true

echo "Waiting for Spark Streaming to connect on spark-master:$PORT ..."
echo "(Start 'bash scripts/start-streaming.sh' in Terminal 1 if not already running)"
echo ""

# Start netcat as TCP server inside spark-master.
# -l = listen mode, -k = keep listening after client disconnects (GNU netcat).
# Stdin of this process is piped to the connected Spark client.
docker exec -i spark-master bash -c "
  # Try GNU netcat first (nc -lk), fall back to OpenBSD (nc -l without -k)
  if nc --version 2>&1 | grep -q 'GNU'; then
    nc -lk -p $PORT
  else
    nc -l -p $PORT
  fi
"
