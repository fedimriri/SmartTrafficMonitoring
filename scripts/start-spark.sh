#!/bin/bash
# Start Spark cluster daemons (run this after every container restart)
# Spark daemons do not auto-start — this script must be run manually.

set -e

echo "=== Starting Spark Master ==="
docker exec spark-master /opt/spark/sbin/start-master.sh

echo "Waiting 3 seconds for Master to initialize..."
sleep 3

echo "=== Starting Spark Workers ==="
docker exec spark-slave1 /opt/spark/sbin/start-slave.sh spark://spark-master:7077
docker exec spark-slave2 /opt/spark/sbin/start-slave.sh spark://spark-master:7077

echo "Waiting 3 seconds..."
sleep 3

echo "=== Spark Cluster Status ==="
echo "--- spark-master ---"
docker exec spark-master jps
echo "--- spark-slave1 ---"
docker exec spark-slave1 jps
echo "--- spark-slave2 ---"
docker exec spark-slave2 jps

echo ""
echo "Spark Master UI: http://localhost:8080"
