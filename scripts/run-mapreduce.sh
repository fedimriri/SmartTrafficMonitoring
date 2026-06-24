#!/bin/bash
# Run both MapReduce jobs on the YARN cluster.
# Requires: JAR built locally, hadoop-master container running.

set -e

JAR_LOCAL="target/SmartTrafficMonitoring-1.0-SNAPSHOT.jar"
JAR_REMOTE="/tmp/SmartTrafficMonitoring.jar"
INPUT="/traffic-data/historical/Metro_Interstate_Traffic_Volume.csv"
OUTPUT_HOURLY="/traffic-data/output/hourly"
OUTPUT_WEATHER="/traffic-data/output/weather"

echo "=== Building project ==="
mvn clean package -q

echo "=== Copying JAR to hadoop-master ==="
docker cp "$JAR_LOCAL" "hadoop-master:$JAR_REMOTE"

echo "=== Cleaning old output directories ==="
docker exec hadoop-master hdfs dfs -rm -r "$OUTPUT_HOURLY" 2>/dev/null || true
docker exec hadoop-master hdfs dfs -rm -r "$OUTPUT_WEATHER" 2>/dev/null || true

echo ""
echo "=== Running Job 1: Traffic Volume by Hour ==="
docker exec hadoop-master hadoop jar "$JAR_REMOTE" \
    com.traffic.mapreduce.hourly.TrafficHourDriver \
    "$INPUT" "$OUTPUT_HOURLY"

echo ""
echo "=== Job 1 Output ==="
docker exec hadoop-master hdfs dfs -cat "$OUTPUT_HOURLY/part-r-00000"

echo ""
echo "=== Running Job 2: Traffic Volume by Weather ==="
docker exec hadoop-master hadoop jar "$JAR_REMOTE" \
    com.traffic.mapreduce.weather.TrafficWeatherDriver \
    "$INPUT" "$OUTPUT_WEATHER"

echo ""
echo "=== Job 2 Output ==="
docker exec hadoop-master hdfs dfs -cat "$OUTPUT_WEATHER/part-r-00000"

echo ""
echo "=== YARN Application History ==="
docker exec hadoop-master yarn application -list -appStates ALL 2>/dev/null | grep -E "Traffic|SUCCEEDED|FAILED"
