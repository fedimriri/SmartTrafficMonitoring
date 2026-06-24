# SmartTrafficMonitoring

<p align="center">
	<img src="https://img.shields.io/badge/Java-8-blue.svg" alt="Java 8" />
	<img src="https://img.shields.io/badge/Hadoop-3.3.6-orange.svg" alt="Hadoop 3.3.6" />
	<img src="https://img.shields.io/badge/Spark%20Streaming-2.4.5-red.svg" alt="Spark Streaming 2.4.5" />
	<img src="https://img.shields.io/badge/Maven-Build-C71A36.svg" alt="Maven" />
	<img src="https://img.shields.io/badge/YARN-Cluster-yellow.svg" alt="YARN" />
</p>

<p align="center">
	<b>Smart Traffic Monitoring System using Hadoop MapReduce and Apache Spark Streaming</b>
</p>

<p align="center">
	Batch analytics for historical traffic data + real-time stream processing for live traffic alerts.
</p>

---

## Overview

SmartTrafficMonitoring is a Big Data project that analyzes road traffic data using two complementary approaches:

- **Batch processing with Hadoop MapReduce** — historical traffic analysis (average per hour, average per weather, peak/lowest identification)
- **Real-time processing with Apache Spark Streaming** — live traffic monitoring with congestion detection, windowed weather aggregation, and running statistics

The project runs on a Docker-based Hadoop + Spark cluster using the Metro Interstate Traffic Volume dataset (48,204 records).

---

## Technology Stack

| Layer | Technology |
| --- | --- |
| Language | Java 8 |
| Batch Processing | Apache Hadoop MapReduce 3.3.6 |
| Streaming | Apache Spark Streaming 2.4.5 |
| Cluster Manager | YARN |
| Distributed Storage | HDFS |
| Build Tool | Maven |
| Deployment | Docker |

---

## Repository Structure

```text
SmartTrafficMonitoring/
├── dataset/
│   └── Metro_Interstate_Traffic_Volume.csv
├── scripts/
│   ├── connect-networks.sh      # Connect Spark containers to Hadoop network
│   ├── start-spark.sh           # Start Spark Master and Workers
│   └── run-mapreduce.sh         # Build + run both MapReduce jobs on YARN
├── src/main/java/com/traffic/
│   ├── mapreduce/
│   │   ├── hourly/
│   │   │   ├── TrafficHourDriver.java
│   │   │   ├── TrafficHourMapper.java
│   │   │   └── TrafficHourReducer.java
│   │   └── weather/
│   │       ├── TrafficWeatherDriver.java
│   │       ├── TrafficWeatherMapper.java
│   │       └── TrafficWeatherReducer.java
│   ├── streaming/
│   │   ├── TrafficStreamingApp.java   # Spark Streaming consumer
│   │   ├── TrafficDataProducer.java   # Socket-based traffic simulator
│   │   └── TrafficRecord.java         # Traffic POJO
│   └── utils/
│       └── CsvParser.java
├── pom.xml
└── README.md
```

---

## Dataset

**Metro Interstate Traffic Volume Dataset**

- File: `dataset/Metro_Interstate_Traffic_Volume.csv`
- Records: 48,204 hourly observations on Interstate 94
- HDFS path: `/traffic-data/historical/Metro_Interstate_Traffic_Volume.csv`

### CSV Schema

```text
holiday,temp,rain_1h,snow_1h,clouds_all,weather_main,weather_description,date_time,traffic_volume
```

Fields used by MapReduce:
- `date_time` (index 7) — extracts the hour (HH)
- `weather_main` (index 5) — weather condition category
- `traffic_volume` (index 8) — vehicle count

---

## HDFS Directory Structure

```text
/traffic-data/
├── historical/
│   └── Metro_Interstate_Traffic_Volume.csv
├── streaming/
│   └── incoming-data/
└── output/
    ├── hourly/          (created by Job 1)
    └── weather/         (created by Job 2)
```

---

## Step-by-Step Execution

### 0 — Prerequisites: Docker cluster running

```bash
docker ps
```

Expected containers: `hadoop-master`, `hadoop-worker1`, `hadoop-worker2`,
`spark-master`, `spark-slave1`, `spark-slave2`

---

### 1 — Connect Docker Networks (once, first time only)

Spark containers must reach Hadoop containers over the same Docker network:

```bash
bash scripts/connect-networks.sh
```

Or manually:

```bash
docker network connect hadoop spark-master
docker network connect hadoop spark-slave1
docker network connect hadoop spark-slave2
```

Verify:

```bash
docker exec spark-master getent hosts hadoop-master
docker exec spark-slave1 getent hosts hadoop-master
```

---

### 2 — Verify Hadoop Cluster

```bash
# Check all daemons
docker exec hadoop-master jps
# Expected: NameNode, SecondaryNameNode, ResourceManager

docker exec hadoop-worker1 jps
# Expected: DataNode, NodeManager

docker exec hadoop-worker2 jps
# Expected: DataNode, NodeManager

# YARN nodes
docker exec hadoop-master yarn node -list

# HDFS report
docker exec hadoop-master hdfs dfsadmin -report
```

---

### 3 — Verify HDFS Dataset

```bash
# List HDFS structure
docker exec hadoop-master hdfs dfs -ls -R /traffic-data

# Confirm dataset is present
docker exec hadoop-master hdfs dfs -ls /traffic-data/historical
```

If the dataset is missing, upload it:

```bash
docker exec hadoop-master hdfs dfs -mkdir -p /traffic-data/historical
docker cp dataset/Metro_Interstate_Traffic_Volume.csv hadoop-master:/tmp/
docker exec hadoop-master hdfs dfs -put /tmp/Metro_Interstate_Traffic_Volume.csv /traffic-data/historical/
```

---

### 4 — Start Spark Cluster

Spark daemons do not auto-start. Run after every container restart:

```bash
bash scripts/start-spark.sh
```

Or manually:

```bash
docker exec spark-master /opt/spark/sbin/start-master.sh
docker exec spark-slave1  /opt/spark/sbin/start-slave.sh spark://spark-master:7077
docker exec spark-slave2  /opt/spark/sbin/start-slave.sh spark://spark-master:7077
```

Verify:

```bash
docker exec spark-master jps   # Expected: Master
docker exec spark-slave1 jps   # Expected: Worker
docker exec spark-slave2 jps   # Expected: Worker
```

Spark Master UI: http://localhost:8080

---

### 5 — Build the Project

```bash
mvn clean package
```

This produces `target/SmartTrafficMonitoring-1.0-SNAPSHOT.jar`.

---

### 6 — Run MapReduce Job 1: Traffic by Hour (via YARN)

```bash
# Copy JAR into the Hadoop container
docker cp target/SmartTrafficMonitoring-1.0-SNAPSHOT.jar hadoop-master:/tmp/SmartTrafficMonitoring.jar

# Remove old output if it exists
docker exec hadoop-master hdfs dfs -rm -r /traffic-data/output/hourly 2>/dev/null || true

# Submit to YARN
docker exec hadoop-master hadoop jar /tmp/SmartTrafficMonitoring.jar \
    com.traffic.mapreduce.hourly.TrafficHourDriver \
    /traffic-data/historical/Metro_Interstate_Traffic_Volume.csv \
    /traffic-data/output/hourly
```

Read the output:

```bash
docker exec hadoop-master hdfs dfs -cat /traffic-data/output/hourly/part-r-00000
```

Expected output:

```text
00      834
01      516
...
16      5663
...
23      1469
PEAK_HOUR       16  (avg = 5663 vehicles/hour)
LOWEST_HOUR     03  (avg = 371 vehicles/hour)
```

---

### 7 — Run MapReduce Job 2: Traffic by Weather (via YARN)

```bash
# Remove old output if it exists
docker exec hadoop-master hdfs dfs -rm -r /traffic-data/output/weather 2>/dev/null || true

# Submit to YARN
docker exec hadoop-master hadoop jar /tmp/SmartTrafficMonitoring.jar \
    com.traffic.mapreduce.weather.TrafficWeatherDriver \
    /traffic-data/historical/Metro_Interstate_Traffic_Volume.csv \
    /traffic-data/output/weather
```

Read the output:

```bash
docker exec hadoop-master hdfs dfs -cat /traffic-data/output/weather/part-r-00000
```

Expected output:

```text
Clear           3055
Clouds          3618
Drizzle         3290
Fog             2703
Haze            3502
Mist            2932
Rain            3317
Smoke           3237
Snow            3016
Squall          2061
Thunderstorm    3001
HIGHEST_WEATHER Clouds  (avg = 3618 vehicles/hour)
LOWEST_WEATHER  Squall  (avg = 2061 vehicles/hour)
```

---

### 8 — Check YARN Job History

```bash
docker exec hadoop-master yarn application -list -appStates ALL
```

Both jobs should appear as `FINISHED / SUCCEEDED`.

---

### 9 — Start the Streaming Producer (Terminal 1)

The producer reads the dataset row by row and streams records to port 9999.  
Start this **before** the Spark Streaming app.

```bash
mvn exec:java \
  -Dexec.mainClass="com.traffic.streaming.TrafficDataProducer" \
  -Dexec.args="dataset/Metro_Interstate_Traffic_Volume.csv 9999 500"
```

Arguments:
- `dataset/Metro_Interstate_Traffic_Volume.csv` — input dataset
- `9999` — TCP port (must match Spark app)
- `500` — milliseconds between records (500 ms = 2 records/second)

The producer will print:
```text
[PRODUCER] Waiting for Spark Streaming to connect...
```

---

### 10 — Start the Spark Streaming App (Terminal 2)

```bash
mvn exec:java \
  -Dexec.mainClass="com.traffic.streaming.TrafficStreamingApp"
```

The app connects to port 9999 and processes events in 5-second micro-batches.

Expected output per batch:

```text
╔══════════════════════════════════════╗
║       ⚠  CONGESTION ALERT  ⚠         ║
╠══════════════════════════════════════╣
║  Time    : 2012-10-02 09:00:00       ║
║  Weather : Clouds                    ║
║  Volume  : 7120                      ║
╚══════════════════════════════════════╝

[STATS] Running Average Traffic : 4823 vehicles/hour  (total processed: 540 records)

[WEATHER] Traffic by condition (last 30 s):
  Clouds               -> 43200 vehicles
  Clear                -> 18400 vehicles
  Rain                 -> 9900 vehicles
```

---

### 11 — Web UIs

| UI | URL |
| --- | --- |
| HDFS NameNode | http://localhost:9870 |
| YARN ResourceManager | http://localhost:8088 |
| Spark Master | http://localhost:8080 |

---

## Streaming Input Format

When using `nc` instead of the producer (for manual testing):

```bash
# Terminal 1: start a netcat server
nc -lk 9999

# Terminal 2: start the streaming app
mvn exec:java -Dexec.mainClass="com.traffic.streaming.TrafficStreamingApp"

# Then type records into Terminal 1:
2026-06-18 08:00:00,Clouds,7200
2026-06-18 08:05:00,Clear,3500
2026-06-18 08:10:00,Rain,6500
```

---

## Congestion Detection Rule

Per project specification:

```java
traffic_volume > 6000  →  CONGESTION ALERT
```

---

## Author

**Fadi Mriri** | Cloud & DevOps Engineer

University Big Data Project — Smart Traffic Monitoring using Hadoop MapReduce and Apache Spark Streaming.
