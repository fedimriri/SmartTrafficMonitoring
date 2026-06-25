#!/bin/bash
# Start TrafficDataProducer inside the spark-master container.
#
# The producer is a plain Java TCP server — it does NOT need Spark.
# It runs on spark-master so that the receiver executor (running on
# spark-slave1 or spark-slave2) can reach it at spark-master:9999
# via the shared Docker 'hadoop' network.
#
# Usage:
#   bash scripts/start-producer.sh [port] [delay_ms]
#
# Defaults:
#   port      = 9999   (matches docker-compose port mapping 9999:9999)
#   delay_ms  = 500    (one record every 500 ms)

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

PORT="${1:-9999}"
DELAY="${2:-500}"
DATASET_SRC="$PROJECT_DIR/dataset/Metro_Interstate_Traffic_Volume.csv"
DATASET_DST="/tmp/Metro_Interstate_Traffic_Volume.csv"
JAR="/tmp/SmartTrafficMonitoring.jar"

echo "=== Copying dataset into spark-master ==="
docker cp "$DATASET_SRC" spark-master:"$DATASET_DST"

echo "=== Killing any previous producer on port $PORT ==="
docker exec spark-master bash -c "
  PID=\$(fuser ${PORT}/tcp 2>/dev/null | awk '{print \$1}')
  if [ -n \"\$PID\" ]; then
    echo \"Stopping old producer (PID \$PID)\"; kill \"\$PID\" 2>/dev/null || true; sleep 1
  fi
" 2>/dev/null || true

echo "=== Starting TrafficDataProducer on spark-master:$PORT (delay ${DELAY} ms) ==="
# Run detached; logs go to /tmp/producer.log inside the container.
docker exec -d spark-master bash -c "
  java -cp $JAR \
    com.traffic.streaming.TrafficDataProducer \
    $DATASET_DST $PORT $DELAY \
    > /tmp/producer.log 2>&1
"

echo "Producer started in background."
echo "  Listening on : spark-master:$PORT (host port $PORT)"
echo "  Log          : docker exec spark-master tail -f /tmp/producer.log"
