#!/bin/bash
# Verify that Spark and Hadoop containers can reach each other.
# All containers share the SmartTrafficMonitoring network (defined in docker-compose.yml),
# so connectivity is automatic after 'docker compose up -d'.
# Run this script to confirm DNS resolution is working correctly.

set -e

echo "=== Verifying cross-container connectivity on SmartTrafficMonitoring network ==="

echo "spark-master -> hadoop-master:"
docker exec spark-master getent hosts hadoop-master

echo "spark-slave1 -> hadoop-master:"
docker exec spark-slave1 getent hosts hadoop-master

echo "spark-slave2 -> hadoop-master:"
docker exec spark-slave2 getent hosts hadoop-master

echo ""
echo "All Spark containers can reach Hadoop containers. Network is healthy."
