#!/bin/bash
# Start Spark cluster daemons (run after every container restart).
# Cleans up stale PID files left by previously crashed processes.

set -e

SPARK_SBIN=/opt/spark/sbin

# ── Clean stale PID files ──────────────────────────────────────────────────
# start-master.sh / start-slave.sh refuse to launch if a PID file exists,
# even when the process it refers to is long dead.
echo "=== Clearing stale Spark PID files ==="
docker exec spark-master  bash -c "rm -f /tmp/spark-*.pid" 2>/dev/null || true
docker exec spark-slave1  bash -c "rm -f /tmp/spark-*.pid" 2>/dev/null || true
docker exec spark-slave2  bash -c "rm -f /tmp/spark-*.pid" 2>/dev/null || true

# ── Master ─────────────────────────────────────────────────────────────────
echo "=== Starting Spark Master ==="
docker exec spark-master $SPARK_SBIN/start-master.sh

echo "Waiting 4 s for Master to register..."
sleep 4

# ── Workers ────────────────────────────────────────────────────────────────
echo "=== Starting Spark Workers ==="
docker exec spark-slave1 $SPARK_SBIN/start-slave.sh spark://spark-master:7077
docker exec spark-slave2 $SPARK_SBIN/start-slave.sh spark://spark-master:7077

echo "Waiting 3 s for Workers to connect..."
sleep 3

# ── Status ─────────────────────────────────────────────────────────────────
echo "=== JVM processes ==="
echo "--- spark-master ---"
docker exec spark-master jps
echo "--- spark-slave1 ---"
docker exec spark-slave1 jps
echo "--- spark-slave2 ---"
docker exec spark-slave2 jps

echo ""
echo "Spark Master UI : http://localhost:8080"
echo "Expected:  Master process on spark-master, Worker process on each slave."
