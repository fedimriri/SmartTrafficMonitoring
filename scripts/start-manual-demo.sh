#!/bin/bash
# Manual Demo Mode — Smart Traffic Monitoring
#
# Starts TrafficDataProducer in interactive mode inside spark-master.
# The user types traffic records; each is immediately forwarded to Spark
# through the existing TCP socket on port 9999. No netcat or external tools needed.
#
# ┌─ WORKFLOW ─────────────────────────────────────────────────────────────────┐
# │                                                                            │
# │  Terminal 1 (Spark):   bash scripts/start-streaming.sh                    │
# │                        (submit the streaming job — waits for data)        │
# │                                                                            │
# │  Terminal 2 (Input):   bash scripts/start-manual-demo.sh                  │
# │                        (interactive Java producer — type records + Enter) │
# │                                                                            │
# │  Record format:        timestamp,weather,traffic_volume                   │
# │  Example:              2026-01-01 08:00:00,Rain,8500                      │
# │                        2026-01-01 09:00:00,Clear,4200                     │
# │                        2026-01-01 10:00:00,Clouds,6100                    │
# │                                                                            │
# │  Congestion trigger:   traffic_volume > 6000                              │
# └────────────────────────────────────────────────────────────────────────────┘
#
# How it works:
#   TrafficDataProducer --interactive opens a ServerSocket on port 9999 and waits
#   for the Spark executor to connect. Once connected, it reads one record per line
#   from stdin, validates the format, and writes it to the Spark client socket.
#   Spark processes the record in the next 5-second micro-batch.
#
# Prerequisites:
#   1. docker-compose up -d              — all 6 containers running
#   2. bash scripts/start-spark.sh       — Spark Master + Workers running
#   3. mvn clean package -DskipTests     — JAR built
#   4. bash scripts/start-streaming.sh   (Terminal 1 — run BEFORE this script)

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

PORT="${1:-9999}"
JAR_SRC="$PROJECT_DIR/target/SmartTrafficMonitoring-1.0-SNAPSHOT.jar"
JAR_DST="/tmp/SmartTrafficMonitoring.jar"

echo ""
echo "========================================================"
echo "  Smart Traffic Monitoring — Manual Demo Mode"
echo "========================================================"
echo ""

# Verify JAR exists locally
if [ ! -f "$JAR_SRC" ]; then
    echo "[ERROR] JAR not found at: $JAR_SRC"
    echo "        Build it first: mvn clean package -DskipTests"
    exit 1
fi

echo "=== Copying JAR to spark-master ==="
docker cp "$JAR_SRC" "spark-master:$JAR_DST"

echo "=== Clearing port $PORT on spark-master ==="
docker exec spark-master bash -c "
  PID=\$(fuser ${PORT}/tcp 2>/dev/null | awk '{print \$1}')
  if [ -n \"\$PID\" ]; then
    echo '[setup] Stopping previous process on port $PORT (PID: '\$PID')...'
    kill \"\$PID\" 2>/dev/null || true
    sleep 1
  fi
" 2>/dev/null || true

echo ""
echo "  Sample records (copy-paste these into the prompt below):"
echo ""
echo "    2026-01-01 08:00:00,Rain,8500      <- CONGESTION ALERT (> 6000)"
echo "    2026-01-01 09:00:00,Clear,4200     <- normal traffic"
echo "    2026-01-01 10:00:00,Clouds,6100    <- CONGESTION ALERT (> 6000)"
echo "    2026-01-01 11:00:00,Snow,3800      <- normal traffic"
echo "    2026-01-01 12:00:00,Fog,7200       <- CONGESTION ALERT (> 6000)"
echo ""
echo "  Watch Terminal 1 for Spark output after each record (within 5 s)."
echo "  Press Ctrl-C to stop."
echo ""
echo "========================================================"
echo ""

# Start TrafficDataProducer in interactive mode inside spark-master.
# -i  keeps stdin open (user's keystrokes flow into the Java process).
# -t  allocates a pseudo-TTY so the terminal behaves correctly.
exec docker exec -it spark-master java \
    -cp "$JAR_DST" \
    com.traffic.streaming.TrafficDataProducer \
    --interactive "$PORT"
