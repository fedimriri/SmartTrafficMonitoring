#!/bin/bash
# Submit TrafficStreamingApp to the Spark standalone cluster.
#
# Prerequisites (run once per container restart, in order):
#   1. bash scripts/start-spark.sh       — starts Master + Workers
#   2. bash scripts/start-producer.sh    — starts TCP data feed on port 9999
#   3. bash scripts/start-streaming.sh   — THIS script (submits the Spark job)
#
# The streaming job runs in FOREGROUND so you can see alerts and stats.
# Press Ctrl-C to stop it.
#
# Execution model:
#   --deploy-mode client  : Driver runs on spark-master (same host as producer).
#   Executors             : Launched by spark-slave1 and spark-slave2.
#   Socket receiver       : Runs as a task on one executor; connects to spark-master:9999.
#   Processing tasks      : Distributed across both workers by the Driver.

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

JAR_HOST="$PROJECT_DIR/target/SmartTrafficMonitoring-1.0-SNAPSHOT.jar"
JAR_CONTAINER="/tmp/SmartTrafficMonitoring.jar"

SOCKET_HOST="spark-master"
CHECKPOINT_DIR="/tmp/spark-checkpoint-traffic"
SOCKET_PORT="9999"

# ── 1. Build ────────────────────────────────────────────────────────────────
echo "=== Building JAR (mvn package) ==="
mvn -f "$PROJECT_DIR/pom.xml" package -DskipTests -q
echo "JAR built: $JAR_HOST"

# ── 2. Deploy JAR to spark-master ───────────────────────────────────────────
echo "=== Copying JAR to spark-master:$JAR_CONTAINER ==="
docker cp "$JAR_HOST" spark-master:"$JAR_CONTAINER"

# ── 3. Verify cluster is up ──────────────────────────────────────────────────
echo "=== Checking Spark cluster ==="
MASTER_PROC=$(docker exec spark-master jps 2>/dev/null | grep -v Jps | grep Master || true)
WORKER1_PROC=$(docker exec spark-slave1 jps 2>/dev/null | grep -v Jps | grep Worker || true)
WORKER2_PROC=$(docker exec spark-slave2 jps 2>/dev/null | grep -v Jps | grep Worker || true)

if [ -z "$MASTER_PROC" ] || [ -z "$WORKER1_PROC" ] || [ -z "$WORKER2_PROC" ]; then
    echo "ERROR: Spark cluster is not fully up."
    echo "  Master  : ${MASTER_PROC:-NOT RUNNING}"
    echo "  Worker1 : ${WORKER1_PROC:-NOT RUNNING}"
    echo "  Worker2 : ${WORKER2_PROC:-NOT RUNNING}"
    echo "Run:  bash scripts/start-spark.sh"
    exit 1
fi
echo "  Master  : OK ($MASTER_PROC)"
echo "  Worker1 : OK ($WORKER1_PROC)"
echo "  Worker2 : OK ($WORKER2_PROC)"

# ── 4. spark-submit ──────────────────────────────────────────────────────────
echo ""
echo "=== Submitting TrafficStreamingApp to spark://spark-master:7077 ==="
echo "    Socket  : $SOCKET_HOST:$SOCKET_PORT"
echo "    Checkpoint: $CHECKPOINT_DIR"
echo "    Spark UI: http://localhost:8080"
echo ""

# Run interactively so streaming output (alerts, stats) prints to this terminal.
docker exec -it spark-master \
  /opt/spark/bin/spark-submit \
    --class com.traffic.streaming.TrafficStreamingApp \
    --master spark://spark-master:7077 \
    --deploy-mode client \
    --conf spark.cores.max=2 \
    --conf spark.executor.cores=1 \
    --conf spark.streaming.receiver.writeAheadLog.enable=false \
    --conf spark.ui.port=4040 \
    "$JAR_CONTAINER" \
    "$SOCKET_HOST" "$CHECKPOINT_DIR" "$SOCKET_PORT"
