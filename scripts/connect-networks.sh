#!/bin/bash
# Connect Spark containers to the Hadoop network.
# Required so Spark can access HDFS and YARN.
# Run once after containers are first started.

set -e

echo "=== Connecting Spark containers to Hadoop network ==="
docker network connect hadoop spark-master  || echo "spark-master already connected"
docker network connect hadoop spark-slave1  || echo "spark-slave1 already connected"
docker network connect hadoop spark-slave2  || echo "spark-slave2 already connected"

echo ""
echo "=== Verifying connectivity ==="
echo "spark-master -> hadoop-master:"
docker exec spark-master getent hosts hadoop-master
echo "spark-slave1 -> hadoop-master:"
docker exec spark-slave1 getent hosts hadoop-master

echo ""
echo "All Spark containers can now reach Hadoop network."
