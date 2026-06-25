# Smart Traffic Monitoring System
## Complete Run & Demo Guide

**Project:** Smart Traffic Monitoring System using Hadoop MapReduce and Apache Spark Streaming  
**Dataset:** Metro Interstate Traffic Volume (48,204 records, 2012–2018)  
**Stack:** Hadoop 3.3.6 · Spark 2.4.5 (Scala 2.11) · Java 8 · Maven 3.x · Docker

---

## Table of Contents

1. [Project Setup Overview](#1-project-setup-overview)
2. [Full Local Installation (Non-Docker)](#2-full-local-installation-non-docker)
3. [Docker Deployment (Primary Method)](#3-docker-deployment-primary-method)
4. [Running MapReduce Batch Jobs](#4-running-mapreduce-batch-jobs)
5. [Spark Streaming Pipeline](#5-spark-streaming-pipeline)
6. [Real-Time Testing Scenarios](#6-real-time-testing-scenarios)
7. [Verification & Debugging](#7-verification--debugging)
8. [Live Demo Script](#8-live-demo-script)
9. [Expected Output Reference](#9-expected-output-reference)

---

## 1. Project Setup Overview

### 1.1 Prerequisites

| Tool | Version | Purpose |
|---|---|---|
| Java (JDK) | 8 or 11 | Compile and run all Java code |
| Maven | 3.6+ | Build system, dependency management |
| Docker | 20.10+ | Run Hadoop + Spark cluster containers |
| Docker Compose | 1.29+ / v2 | Orchestrate the 6-container cluster |
| Git | any | Clone the repository |

> **Java note:** The project targets Java 8 bytecode (`maven.compiler.source=1.8`).  
> Running `mvn exec:java` on Java 11 is supported — anonymous inner classes are used throughout  
> Spark Streaming code instead of lambdas specifically to preserve bytecode compatibility.

Verify your tools:

```bash
java -version          # must show 1.8.x or 11.x
mvn -version           # must show 3.6+
docker --version       # must show 20.x+
docker compose version # or: docker-compose --version
```

---

### 1.2 System Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                        HOST MACHINE                                  │
│                                                                       │
│  ┌─────────────────────────────────────────────┐                    │
│  │              Docker Network: hadoop           │                    │
│  │                                               │                    │
│  │  ┌──────────────┐   ┌──────────┐ ┌────────┐ │                    │
│  │  │ hadoop-master │   │ hadoop-  │ │hadoop- │ │                    │
│  │  │ HDFS NameNode│   │ worker1  │ │worker2 │ │                    │
│  │  │ YARN ResrcMgr│   │ DataNode │ │DataNode│ │                    │
│  │  │ :9870 :8088  │   │          │ │        │ │                    │
│  │  └──────────────┘   └──────────┘ └────────┘ │                    │
│  │                                               │                    │
│  │  ┌──────────────┐   ┌──────────┐ ┌────────┐ │                    │
│  │  │ spark-master  │   │spark-    │ │spark-  │ │                    │
│  │  │ Spark Master  │   │slave1    │ │slave2  │ │                    │
│  │  │ :8080 :7077  │   │  Worker  │ │ Worker │ │                    │
│  │  └──────────────┘   └──────────┘ └────────┘ │                    │
│  └─────────────────────────────────────────────┘                    │
│                                                                       │
│  ┌───────────────────┐         ┌────────────────────────────────┐   │
│  │ TrafficDataProducer│──TCP──▶│    TrafficStreamingApp          │   │
│  │  (mvn exec:java)  │ :19999  │      (mvn exec:java)            │   │
│  │  ServerSocket     │         │  socketTextStream(localhost,     │   │
│  │  reads CSV file   │         │  19999) → Spark local[2]        │   │
│  └───────────────────┘         └────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────┘
```

**Data flow summary:**

- **Batch path:** CSV on HDFS → MapReduce on YARN → result on HDFS
- **Streaming path:** CSV on host filesystem → TCP socket → Spark Streaming → console output

---

### 1.3 Project Structure

```
SmartTrafficMonitoring/
│
├── pom.xml                          # Maven build (Hadoop 3.3.6, Spark 2.4.5)
├── docker-compose.yml               # 6-container cluster definition
│
├── dataset/
│   └── Metro_Interstate_Traffic_Volume.csv   # 48,204 rows, 2012-2018
│
├── src/main/java/com/traffic/
│   ├── mapreduce/
│   │   ├── hourly/
│   │   │   ├── TrafficHourDriver.java    # Job 1 entry point
│   │   │   ├── TrafficHourMapper.java    # extracts hour → traffic_volume
│   │   │   └── TrafficHourReducer.java   # computes average per hour
│   │   └── weather/
│   │       ├── TrafficWeatherDriver.java  # Job 2 entry point
│   │       ├── TrafficWeatherMapper.java  # extracts weather → traffic_volume
│   │       └── TrafficWeatherReducer.java # computes average per condition
│   ├── streaming/
│   │   ├── TrafficDataProducer.java  # TCP server, reads CSV, sends records
│   │   ├── TrafficStreamingApp.java  # Spark Streaming client, 3 analytics
│   │   └── TrafficRecord.java        # CSV parser + wire format serializer
│   └── utils/
│       └── CsvParser.java
│
├── scripts/
│   ├── start-spark.sh          # Start Spark Master + Workers in containers
│   ├── run-mapreduce.sh        # Build + copy JAR + run both MR jobs
│   └── connect-networks.sh     # One-time network bridge setup
│
└── target/
    └── SmartTrafficMonitoring-1.0-SNAPSHOT.jar   # Built JAR (28 KB, thin)
```

---

### 1.4 Dataset Schema

The CSV file `Metro_Interstate_Traffic_Volume.csv` has 9 columns:

```
holiday, temp, rain_1h, snow_1h, clouds_all, weather_main, weather_description, date_time, traffic_volume
  [0]    [1]    [2]      [3]       [4]           [5]             [6]              [7]         [8]
```

Example row:
```
None,288.28,0.0,0.0,40,Clouds,scattered clouds,2012-10-02 09:00:00,5545
```

**MapReduce Job 1** uses `fields[7]` (date_time → extract hour) and `fields[8]` (traffic_volume).  
**MapReduce Job 2** uses `fields[5]` (weather_main) and `fields[8]` (traffic_volume).  
**Streaming wire format** (after parsing): `date_time,weather_main,traffic_volume`  
Example: `2012-10-02 09:00:00,Clouds,5545`

---

## 2. Full Local Installation (Non-Docker)

This section covers running Spark Streaming entirely on the host machine (no Docker needed). MapReduce requires a running Hadoop cluster — the Docker method in Section 3 is the standard path for that.

### 2.1 Compile the Project

All commands run from the project root directory.

```bash
# Clean and build — produces target/SmartTrafficMonitoring-1.0-SNAPSHOT.jar
mvn clean package -DskipTests

# Verify the JAR was created
ls -lh target/SmartTrafficMonitoring-1.0-SNAPSHOT.jar
# Expected: target/SmartTrafficMonitoring-1.0-SNAPSHOT.jar  ~28 KB
```

Verify Jackson version pinning (critical for Spark 2.4.5 compatibility):

```bash
mvn dependency:tree | grep jackson-databind
# Must show: com.fasterxml.jackson.core:jackson-databind:jar:2.6.7.1:compile
# NOT 2.12.x — that version crashes SparkContext at startup
```

### 2.2 Local Hadoop (Optional — Skip if Using Docker)

If you have Hadoop installed locally and want to run MapReduce without Docker:

```bash
# Start HDFS and YARN
$HADOOP_HOME/sbin/start-dfs.sh
$HADOOP_HOME/sbin/start-yarn.sh

# Verify daemons
jps
# Expected output includes: NameNode, DataNode, ResourceManager, NodeManager

# Create HDFS input directory
hdfs dfs -mkdir -p /traffic-data/historical

# Upload the dataset
hdfs dfs -put dataset/Metro_Interstate_Traffic_Volume.csv \
    /traffic-data/historical/

# Verify upload
hdfs dfs -ls /traffic-data/historical/
```

Run MapReduce Job 1 (Traffic by Hour):

```bash
# Clean previous output if it exists
hdfs dfs -rm -r /traffic-data/output/hourly 2>/dev/null || true

# Run Job 1
hadoop jar target/SmartTrafficMonitoring-1.0-SNAPSHOT.jar \
    com.traffic.mapreduce.hourly.TrafficHourDriver \
    /traffic-data/historical/Metro_Interstate_Traffic_Volume.csv \
    /traffic-data/output/hourly

# Read the result
hdfs dfs -cat /traffic-data/output/hourly/part-r-00000
```

Run MapReduce Job 2 (Traffic by Weather):

```bash
# Clean previous output
hdfs dfs -rm -r /traffic-data/output/weather 2>/dev/null || true

# Run Job 2
hadoop jar target/SmartTrafficMonitoring-1.0-SNAPSHOT.jar \
    com.traffic.mapreduce.weather.TrafficWeatherDriver \
    /traffic-data/historical/Metro_Interstate_Traffic_Volume.csv \
    /traffic-data/output/weather

# Read the result
hdfs dfs -cat /traffic-data/output/weather/part-r-00000
```

### 2.3 Run Spark Streaming Locally (No Docker Required)

The Streaming pipeline runs entirely on the host. Open **two separate terminals**.

**Terminal 1 — Start the Producer (TCP server):**

```bash
# Port 19999 is used (port 9999 is reserved by Docker port mapping)
mvn exec:java \
    -Dexec.mainClass="com.traffic.streaming.TrafficDataProducer" \
    -Dexec.args="dataset/Metro_Interstate_Traffic_Volume.csv 19999 500"
```

The producer starts a `ServerSocket` on port 19999 and waits. You will see:

```
========================================
  Smart Traffic Monitoring — Producer
========================================
Dataset : dataset/Metro_Interstate_Traffic_Volume.csv
Port    : 19999
Delay   : 500 ms / record
Waiting for Spark Streaming to connect...
========================================
```

**Terminal 2 — Start the Streaming Application:**

```bash
# Args: master  socketHost  checkpointDir  socketPort
mvn exec:java \
    -Dexec.mainClass="com.traffic.streaming.TrafficStreamingApp" \
    -Dexec.args="local[2] localhost /tmp/spark-checkpoint-traffic 19999"
```

Once connected, the producer prints:

```
[PRODUCER] Spark connected from /127.0.0.1
[PRODUCER] Sent    20 records | Last: 2012-10-02 09:20:00,Clouds,4399
[PRODUCER] Sent    40 records | Last: 2012-10-02 09:40:00,Clouds,5765
```

---

## 3. Docker Deployment (Primary Method)

### 3.1 Docker Architecture

The `docker-compose.yml` defines 6 containers all on a single bridge network named `hadoop`:

| Container | Image | Role | Exposed Ports |
|---|---|---|---|
| `hadoop-master` | `liliasfaxi/hadoop-cluster:latest` | HDFS NameNode + YARN ResourceManager | 9870, 8088 |
| `hadoop-worker1` | `liliasfaxi/hadoop-cluster:latest` | HDFS DataNode + YARN NodeManager | 8841 |
| `hadoop-worker2` | `liliasfaxi/hadoop-cluster:latest` | HDFS DataNode + YARN NodeManager | 8842 |
| `spark-master` | `spark-image:latest` | Spark Standalone Master | 8080, 7077, 9999 |
| `spark-slave1` | `spark-image:latest` | Spark Worker | — |
| `spark-slave2` | `spark-image:latest` | Spark Worker | — |

**Important design details:**

- `hadoop-master` auto-starts HDFS and YARN on container startup.
- Spark containers start with `tail -f /dev/null` — **Spark daemons do NOT auto-start.** You must run `bash scripts/start-spark.sh` after every `docker-compose up`.
- Port `9999` is mapped on `spark-master`. This causes `docker-proxy` to hold host port 9999. **Always use port `19999`** for the producer and streaming app when running on the host.

---

### 3.2 Build the Spark Docker Image

The Hadoop image is pulled from Docker Hub automatically. The Spark image must be built locally (it is not on Docker Hub).

Check if the image already exists:

```bash
docker images | grep spark-image
```

If not present, build it. The Spark image is typically built from a Dockerfile with Spark 2.4.5 pre-installed. If a `Dockerfile.spark` or similar is available:

```bash
# If there is a Dockerfile for Spark, build it:
docker build -t spark-image:latest -f Dockerfile.spark .

# Verify
docker images | grep spark-image
# Expected: spark-image  latest  <hash>  <size>
```

If you already have the image from a previous session, skip this step.

---

### 3.3 Start the Cluster

```bash
# Start all 6 containers in the background
docker-compose up -d

# Watch startup logs (optional, Ctrl+C to stop watching)
docker-compose logs -f hadoop-master
```

Wait approximately 15 seconds for HDFS to format (first run) and YARN to initialize. Then verify all containers are running:

```bash
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
```

Expected output:

```
NAMES           STATUS          PORTS
spark-slave2    Up N seconds
spark-slave1    Up N seconds
spark-master    Up N seconds    0.0.0.0:7077->7077/tcp, 0.0.0.0:8080->8080/tcp, 0.0.0.0:9999->9999/tcp
hadoop-master   Up N seconds    0.0.0.0:8088->8088/tcp, 0.0.0.0:9870->9870/tcp
hadoop-worker2  Up N seconds
hadoop-worker1  Up N seconds
```

---

### 3.4 Start the Spark Cluster

Run this script every time after `docker-compose up`:

```bash
bash scripts/start-spark.sh
```

The script:
1. Starts Spark Master on `spark-master`
2. Waits 3 seconds
3. Starts Worker on `spark-slave1` (registers with `spark://spark-master:7077`)
4. Starts Worker on `spark-slave2`
5. Prints JPS output for each container

Expected output:

```
=== Starting Spark Master ===
starting org.apache.spark.deploy.master.Master ...
Waiting 3 seconds for Master to initialize...
=== Starting Spark Workers ===
starting org.apache.spark.deploy.worker.Worker ...
starting org.apache.spark.deploy.worker.Worker ...
=== Spark Cluster Status ===
--- spark-master ---
130 Master
131 Jps
--- spark-slave1 ---
56 Worker
57 Jps
--- spark-slave2 ---
50 Worker
51 Jps

Spark Master UI: http://localhost:8080
```

---

### 3.5 Prepare HDFS

Upload the dataset to HDFS inside the Hadoop cluster:

```bash
# Create the input directory in HDFS
docker exec hadoop-master hdfs dfs -mkdir -p /traffic-data/historical

# Copy the CSV from host into the hadoop-master container
docker cp dataset/Metro_Interstate_Traffic_Volume.csv \
    hadoop-master:/tmp/Metro_Interstate_Traffic_Volume.csv

# Upload from container local filesystem into HDFS
docker exec hadoop-master hdfs dfs -put \
    /tmp/Metro_Interstate_Traffic_Volume.csv \
    /traffic-data/historical/Metro_Interstate_Traffic_Volume.csv

# Verify
docker exec hadoop-master hdfs dfs -ls /traffic-data/historical/
# Expected: -rw-r--r--   3  root  supergroup  ~4.5MB  .../Metro_Interstate_Traffic_Volume.csv
```

---

## 4. Running MapReduce Batch Jobs

### 4.1 Automated (Recommended)

The `run-mapreduce.sh` script handles build, JAR copy, and execution of both jobs:

```bash
bash scripts/run-mapreduce.sh
```

What the script does step by step:
1. `mvn clean package -q` — rebuilds the JAR
2. `docker cp target/SmartTrafficMonitoring-1.0-SNAPSHOT.jar hadoop-master:/tmp/SmartTrafficMonitoring.jar` — copies JAR into container
3. Removes any previous output directories from HDFS
4. Runs Job 1: Traffic by Hour
5. Prints Job 1 results
6. Runs Job 2: Traffic by Weather
7. Prints Job 2 results
8. Lists YARN application history

---

### 4.2 Manual Step-by-Step

If you prefer to run each step individually:

**Build and copy the JAR:**

```bash
mvn clean package -DskipTests

docker cp target/SmartTrafficMonitoring-1.0-SNAPSHOT.jar \
    hadoop-master:/tmp/SmartTrafficMonitoring.jar
```

**Job 1 — Traffic Volume by Hour:**

```bash
# Remove previous output (HDFS fails if directory exists)
docker exec hadoop-master hdfs dfs -rm -r /traffic-data/output/hourly 2>/dev/null || true

# Submit the MapReduce job via YARN
docker exec hadoop-master hadoop jar /tmp/SmartTrafficMonitoring.jar \
    com.traffic.mapreduce.hourly.TrafficHourDriver \
    /traffic-data/historical/Metro_Interstate_Traffic_Volume.csv \
    /traffic-data/output/hourly

# Read the output
docker exec hadoop-master hdfs dfs -cat /traffic-data/output/hourly/part-r-00000
```

**Job 2 — Traffic Volume by Weather:**

```bash
# Remove previous output
docker exec hadoop-master hdfs dfs -rm -r /traffic-data/output/weather 2>/dev/null || true

# Submit
docker exec hadoop-master hadoop jar /tmp/SmartTrafficMonitoring.jar \
    com.traffic.mapreduce.weather.TrafficWeatherDriver \
    /traffic-data/historical/Metro_Interstate_Traffic_Volume.csv \
    /traffic-data/output/weather

# Read the output
docker exec hadoop-master hdfs dfs -cat /traffic-data/output/weather/part-r-00000
```

---

### 4.3 How MapReduce Works in This Project

**Job 1 (TrafficHourMapper → TrafficHourReducer):**

```
Map phase:
  Input:  "None,288.28,0.0,0.0,40,Clouds,...,2012-10-02 09:00:00,5545"
  Output: ("09", 5545)           ← hour extracted from fields[7].substring(11,13)

Shuffle phase:
  Groups all records by hour key: "09" → [5545, 4516, 4767, ...]

Reduce phase:
  Input:  ("09", [5545, 4516, 4767, 5026, ...])
  Output: "09    4123 vehicles/hour avg"
```

**Job 2 (TrafficWeatherMapper → TrafficWeatherReducer):**

```
Map phase:
  Input:  "None,288.28,...,Clouds,...,5545"
  Output: ("Clouds", 5545)       ← weather from fields[5]

Shuffle phase:
  Groups by weather: "Clouds" → [5545, 4516, ...], "Rain" → [3200, 2800, ...]

Reduce phase:
  Input:  ("Clouds", [5545, 4516, ...])
  Output: "Clouds    3821 vehicles/hour avg"
```

---

## 5. Spark Streaming Pipeline

### 5.1 Architecture

The streaming pipeline uses a TCP socket connection between two Java processes running on the host machine:

```
TrafficDataProducer          TrafficStreamingApp
(TCP Server)                 (TCP Client / Spark)
─────────────────────────────────────────────────
new ServerSocket(19999)  ←── socketTextStream("localhost", 19999)
.accept() blocks             5-second micro-batch
reads CSV line by line   ──► receives lines
parses via TrafficRecord.fromCsvLine()
sends toStreamFormat()
"2012-10-02 09:00:00,Clouds,5545"
```

**Three real-time analytics run every 5-second batch:**

| Feature | Implementation | Output |
|---|---|---|
| Congestion Alert | `filter(volume > 6000).foreachRDD(print)` | Alert box per event |
| Running Average | `updateStateByKey` on key "GLOBAL" | Cumulative avg/count |
| Weather Aggregation | `reduceByKeyAndWindow(30s, 5s)` | Sum per weather condition |

### 5.2 Start the Streaming Pipeline

> **Prerequisite:** The project must be compiled first (`mvn package` or `mvn clean package`).

**Terminal 1 — Producer (start first):**

```bash
mvn exec:java \
    -Dexec.mainClass="com.traffic.streaming.TrafficDataProducer" \
    -Dexec.args="dataset/Metro_Interstate_Traffic_Volume.csv 19999 500"
```

**Terminal 2 — Streaming App (start after producer is waiting):**

```bash
mvn exec:java \
    -Dexec.mainClass="com.traffic.streaming.TrafficStreamingApp" \
    -Dexec.args="local[2] localhost /tmp/spark-checkpoint-traffic 19999"
```

**Parameter reference:**

| Parameter | TrafficDataProducer | TrafficStreamingApp |
|---|---|---|
| args[0] | CSV file path | Spark master (`local[2]` or `spark://spark-master:7077`) |
| args[1] | TCP port (use `19999`) | Socket host (`localhost`) |
| args[2] | Delay in ms (`500` = realistic, `100` = fast demo) | Checkpoint directory |
| args[3] | — | TCP port (use `19999`) |

---

### 5.3 How the Producer Works

`TrafficDataProducer` acts as a **TCP server**:

1. Opens a `ServerSocket` on the specified port (19999)
2. Waits (blocks) until a client connects
3. Once connected, reads the CSV file line by line
4. For each line, calls `TrafficRecord.fromCsvLine(line)`:
   - Skips the header line (`holiday,...`)
   - Parses `fields[7]` = date_time, `fields[5]` = weather_main, `fields[8]` = traffic_volume
5. Sends `record.toStreamFormat()` = `"date_time,weather_main,traffic_volume\n"` over the socket
6. Sleeps `delayMs` milliseconds between records (500ms by default = ~2 records/second)
7. After the full dataset is sent, waits for the next connection (loops)

The producer prints progress every 20 records:

```
[PRODUCER] Sent    20 records | Last: 2012-10-02 09:20:00,Clouds,4399
[PRODUCER] Sent    40 records | Last: 2012-10-02 09:40:00,Clouds,5765
[PRODUCER] Sent    60 records | Last: 2012-10-02 10:00:00,Clouds,4516
```

---

### 5.4 How Spark Streaming Processes Data

`TrafficStreamingApp` acts as a **TCP client**:

1. Creates a `JavaStreamingContext` with a 5-second micro-batch interval
2. Sets checkpoint directory (required for `updateStateByKey`)
3. Opens `socketTextStream("localhost", 19999)` — connects to the producer
4. Every 5 seconds, processes all lines received in that window:
   - Filters out empty lines
   - Parses each line: `parts[0]`=timestamp, `parts[1]`=weather, `parts[2]`=volume
   - Skips malformed lines (fault-tolerant via `flatMap`)
5. Runs 3 parallel pipelines on the parsed `events` DStream

---

### 5.5 Manual Data Injection

You can inject records manually without the CSV producer. While the streaming app is running, in a third terminal:

```bash
# Connect to the streaming port and send a record
echo "2012-10-02 09:00:00,Rain,7500" | nc localhost 19999
```

Or use `nc` in interactive mode to type records one by one:

```bash
nc localhost 19999
# Then type (one per line, press Enter after each):
2012-10-02 09:00:00,Rain,5000
2012-10-02 10:00:00,Clear,7200
2012-10-02 11:00:00,Snow,3100
2012-10-02 12:00:00,Fog,2800
# Press Ctrl+C to disconnect
```

> **Note:** When using `nc` for injection, the streaming app must already be connected to the producer. Either keep the producer running on a different port and use `nc` on another port, or stop the producer first and use `nc` directly on port 19999.

Alternatively, create a test file and pipe it:

```bash
cat > /tmp/test_traffic.txt << 'EOF'
2012-10-02 09:00:00,Rain,5000
2012-10-02 10:00:00,Clear,7200
2012-10-02 11:00:00,Rain,6500
2012-10-02 12:00:00,Fog,2800
EOF

cat /tmp/test_traffic.txt | nc localhost 19999
```

---

## 6. Real-Time Testing Scenarios

### Scenario 1: Normal Traffic (Volume ≤ 6000)

**Input records (inject these):**
```
2012-10-02 09:00:00,Clouds,5545
2012-10-02 10:00:00,Clouds,4516
2012-10-02 11:00:00,Clouds,4767
2012-10-02 12:00:00,Clouds,5026
```

**Expected Spark console output:**

No congestion alerts (all volumes ≤ 6000).

After 5-second batch:
```
[STATS] Running Average Traffic : 4963 vehicles/hour  (total processed: 4 records)
[WEATHER] Traffic by condition (last 30 s):
  Clouds               -> 19854 vehicles
```

**What to explain:** The running average accumulates over time using `updateStateByKey`. The weather aggregation shows the sum across all events in the last 30 seconds.

---

### Scenario 2: Congestion Detection (Volume > 6000)

**Input records:**
```
2012-10-02 16:00:00,Clear,6015
2012-10-02 17:00:00,Clear,6427
2012-10-02 17:30:00,Clear,7102
```

**Expected Spark console output:**

For each record above the 6000 threshold, an alert box appears immediately in the current 5-second batch:

```
+--------------------------------------+
|       ** CONGESTION ALERT **         |
+--------------------------------------+
|  Time    : 2012-10-02 16:00:00       |
|  Weather : Clear                     |
|  Volume  : 6015                      |
+--------------------------------------+

+--------------------------------------+
|       ** CONGESTION ALERT **         |
+--------------------------------------+
|  Time    : 2012-10-02 17:00:00       |
|  Weather : Clear                     |
|  Volume  : 6427                      |
+--------------------------------------+

+--------------------------------------+
|       ** CONGESTION ALERT **         |
+--------------------------------------+
|  Time    : 2012-10-02 17:30:00       |
|  Weather : Clear                     |
|  Volume  : 7102                      |
+--------------------------------------+
```

**What to explain:** The `filter(trafficVolume > 6000)` transformation is applied to every micro-batch. Any batch containing high-volume records triggers an alert printed via `foreachRDD`. This is stateless — each 5-second batch is evaluated independently.

---

### Scenario 3: Weather Impact (Mixed Conditions)

**Input records:**
```
2012-10-02 08:00:00,Rain,3200
2012-10-02 08:30:00,Rain,2800
2012-10-02 09:00:00,Clear,5545
2012-10-02 09:30:00,Clear,6100
2012-10-02 10:00:00,Fog,1200
2012-10-02 10:30:00,Snow,800
```

**Expected Spark console output (after 30-second window fills):**

```
[WEATHER] Traffic by condition (last 30 s):
  Rain                 -> 6000 vehicles
  Clear                -> 11645 vehicles
  Fog                  -> 1200 vehicles
  Snow                 -> 800 vehicles

[STATS] Running Average Traffic : 3274 vehicles/hour  (total processed: 6 records)
```

**What to explain:** `reduceByKeyAndWindow` groups by weather condition, sums volumes within the sliding 30-second window, and slides it forward every 5 seconds. You can observe how the `Rain` bucket decreases as those records fall outside the 30-second window.

---

### Scenario 4: Full Dataset Run (Live Demo)

Run the producer at normal speed (500ms/record):

```bash
# Producer: 1 record every 500ms = ~2 records/second
# Full dataset: 48,204 records ≈ 6.7 hours at this speed
# For demo, use 100ms to see more results quickly:
mvn exec:java \
    -Dexec.mainClass="com.traffic.streaming.TrafficDataProducer" \
    -Dexec.args="dataset/Metro_Interstate_Traffic_Volume.csv 19999 100"
```

At 100ms/record, the first congestion alert (volume 6015) appears after approximately 2 minutes.

During a 5-minute demo at 100ms/record you will see:
- ~3000 records processed
- Roughly 130+ congestion alerts fired
- Running average stabilizing around 3500–4000 vehicles/hour
- 6 to 8 distinct weather conditions in aggregation output

---

## 7. Verification & Debugging

### 7.1 Hadoop Web UIs

| UI | URL | What to check |
|---|---|---|
| HDFS NameNode | http://localhost:9870 | DataNodes connected, disk usage, file browser |
| YARN ResourceManager | http://localhost:8088 | Running/completed applications, job progress |
| Spark Master | http://localhost:8080 | Workers registered, running applications |

**NameNode Health Check:**

```bash
docker exec hadoop-master hdfs dfsadmin -report
# Look for: "Live datanodes (2)" and non-zero "DFS Remaining"
```

**YARN Applications:**

```bash
docker exec hadoop-master yarn application -list -appStates ALL
```

**Spark Cluster Status:**

```bash
docker exec spark-master jps
# Must include: Master
docker exec spark-slave1 jps
# Must include: Worker
```

---

### 7.2 Check Logs Inside Containers

**Hadoop HDFS logs:**
```bash
docker exec hadoop-master tail -100 \
    /usr/local/hadoop/logs/hadoop-root-namenode-hadoop-master.log
```

**YARN ResourceManager logs:**
```bash
docker exec hadoop-master tail -100 \
    /usr/local/hadoop/logs/yarn-root-resourcemanager-hadoop-master.log
```

**Spark Master logs:**
```bash
docker exec spark-master tail -100 \
    /opt/spark/logs/spark--org.apache.spark.deploy.master.Master-1-spark-master.out
```

---

### 7.3 Common Errors and Fixes

**Error: `Address already in use` (BindException) on port 9999**

```
Caused by: java.net.BindException: Address already in use (Bind failed)
```

Cause: Docker Compose maps `"9999:9999"` on `spark-master`. The `docker-proxy` process holds host port 9999.

Fix: Always use port **19999** for the producer and streaming app:
```bash
# Correct command — use 19999, not 9999
mvn exec:java -Dexec.mainClass="com.traffic.streaming.TrafficDataProducer" \
    -Dexec.args="dataset/Metro_Interstate_Traffic_Volume.csv 19999 500"
```

Verify port availability before starting:
```bash
ss -tlnp | grep 19999
# Should return nothing (port is free)
```

---

**Error: `Incompatible Jackson version: 2.12.x`**

```
com.fasterxml.jackson.databind.JsonMappingException: Incompatible Jackson version: 2.12.7-1
    at com.fasterxml.jackson.databind.cfg.MapperConfig.<clinit>(...)
```

Cause: `pom.xml` missing `<dependencyManagement>`. Hadoop 3.3.6 pulls `jackson-databind:2.12.x` which Spark 2.4.5 rejects.

Fix: Already applied — the `<dependencyManagement>` block in `pom.xml` pins Jackson to `2.6.7.1`. Verify:
```bash
mvn dependency:tree | grep jackson-databind
# Must show: jackson-databind:jar:2.6.7.1:compile
```

---

**Error: `Connection refused` on socket**

```
java.net.ConnectException: Connection refused
```

Cause: The streaming app started before the producer (producer must start first).

Fix: Start the **producer first**, wait until it prints "Waiting for Spark Streaming to connect...", then start the streaming app.

---

**Error: Spark daemons not running**

```
docker exec spark-master jps
# Only shows: Jps  (no Master process)
```

Cause: Spark daemons do not auto-start on container startup.

Fix:
```bash
bash scripts/start-spark.sh
```

---

**Error: HDFS output directory already exists**

```
org.apache.hadoop.mapred.FileAlreadyExistsException: Output directory already exists
```

Fix: Delete the output directory before re-running:
```bash
docker exec hadoop-master hdfs dfs -rm -r /traffic-data/output/hourly
docker exec hadoop-master hdfs dfs -rm -r /traffic-data/output/weather
```

---

**Error: DataNode not connecting to NameNode**

Fix: Restart the worker containers:
```bash
docker-compose restart hadoop-worker1 hadoop-worker2
```

Wait 10 seconds, then verify:
```bash
docker exec hadoop-master hdfs dfsadmin -report | grep "Live datanodes"
# Expected: Live datanodes (2)
```

---

### 7.4 Verify Streaming Output Live

While streaming runs, you can pipe output to a file for analysis:

```bash
mvn exec:java \
    -Dexec.mainClass="com.traffic.streaming.TrafficStreamingApp" \
    -Dexec.args="local[2] localhost /tmp/spark-checkpoint-traffic 19999" \
    -q 2>&1 | tee /tmp/streaming-output.log
```

In another terminal, watch for specific events:
```bash
# Watch only congestion alerts
tail -f /tmp/streaming-output.log | grep -A 6 "CONGESTION ALERT"

# Watch only stats
tail -f /tmp/streaming-output.log | grep "\[STATS\]"

# Watch only weather
tail -f /tmp/streaming-output.log | grep "\[WEATHER\]"
```

---

## 8. Live Demo Script

This is the exact sequence to follow during a live demonstration.

---

### Step 0 — Verify Prerequisites (60 seconds)

```bash
# Check Docker is running
docker ps

# Check all 6 containers are up
docker ps --format "{{.Names}}: {{.Status}}" | grep -E "hadoop|spark"

# Check port 19999 is free
ss -tlnp | grep 19999   # should return nothing
```

---

### Step 1 — Start the Hadoop + Spark Cluster (2 minutes)

```bash
# If containers are not already running:
docker-compose up -d

# Wait 15 seconds for HDFS to start, then start Spark daemons:
bash scripts/start-spark.sh
```

**Show the evaluator:**
- Open http://localhost:9870 — HDFS NameNode UI with 2 DataNodes
- Open http://localhost:8088 — YARN ResourceManager
- Open http://localhost:8080 — Spark Master with 2 Workers

---

### Step 2 — Upload Dataset to HDFS (30 seconds)

```bash
# Create HDFS directory
docker exec hadoop-master hdfs dfs -mkdir -p /traffic-data/historical

# Upload the dataset
docker cp dataset/Metro_Interstate_Traffic_Volume.csv \
    hadoop-master:/tmp/Metro_Interstate_Traffic_Volume.csv

docker exec hadoop-master hdfs dfs -put \
    /tmp/Metro_Interstate_Traffic_Volume.csv \
    /traffic-data/historical/Metro_Interstate_Traffic_Volume.csv

# Verify (show this to the evaluator)
docker exec hadoop-master hdfs dfs -ls /traffic-data/historical/
```

---

### Step 3 — Run MapReduce Batch Analytics (3–5 minutes)

```bash
# Build JAR and run both jobs automatically
bash scripts/run-mapreduce.sh
```

**While jobs run, show the evaluator:**
- http://localhost:8088 — YARN applications, progress bars
- Watch the terminal for job counters

**After completion, show the results:**

```bash
# Job 1: Traffic volume by hour of day
docker exec hadoop-master hdfs dfs -cat /traffic-data/output/hourly/part-r-00000

# Job 2: Traffic volume by weather condition
docker exec hadoop-master hdfs dfs -cat /traffic-data/output/weather/part-r-00000
```

---

### Step 4 — Start Spark Streaming (open two new terminals)

**Terminal 1 — Producer:**

```bash
mvn exec:java \
    -Dexec.mainClass="com.traffic.streaming.TrafficDataProducer" \
    -Dexec.args="dataset/Metro_Interstate_Traffic_Volume.csv 19999 100" -q
```

Wait until you see "Waiting for Spark Streaming to connect..."

**Terminal 2 — Streaming App:**

```bash
mvn exec:java \
    -Dexec.mainClass="com.traffic.streaming.TrafficStreamingApp" \
    -Dexec.args="local[2] localhost /tmp/spark-checkpoint-traffic 19999" -q
```

---

### Step 5 — Show Real-Time Output (3 minutes)

In Terminal 2, point the evaluator to these outputs appearing every 5 seconds:

**Weather aggregation (appears after first 30-second window):**
```
[WEATHER] Traffic by condition (last 30 s):
  Clouds               -> 45231 vehicles
  Rain                 -> 12800 vehicles
```

**Running average (appears every batch):**
```
[STATS] Running Average Traffic : 3539 vehicles/hour  (total processed: 152 records)
[STATS] Running Average Traffic : 3518 vehicles/hour  (total processed: 317 records)
```

**Congestion alerts (appear when volume > 6000):**
```
+--------------------------------------+
|       ** CONGESTION ALERT **         |
+--------------------------------------+
|  Time    : 2012-10-02 16:00:00       |
|  Weather : Clear                     |
|  Volume  : 6015                      |
+--------------------------------------+
```

---

### Step 6 — Inject Manual Test Records (2 minutes)

In a **third terminal**, inject records with extreme values to demonstrate real-time detection:

```bash
# This will trigger 3 congestion alerts immediately
printf "2026-01-01 08:00:00,Rain,8000\n2026-01-01 09:00:00,Snow,9500\n2026-01-01 10:00:00,Clear,7300\n" \
    | nc -q 1 localhost 19999
```

The evaluator will see 3 congestion alerts appear in Terminal 2 within 5 seconds.

---

### Step 7 — Stop and Verify (30 seconds)

```bash
# Stop streaming (Ctrl+C in Terminal 1 and Terminal 2)

# Show checkpoint directory was created (proof of stateful processing)
ls /tmp/spark-checkpoint-traffic/

# Show HDFS batch results one more time
docker exec hadoop-master hdfs dfs -cat /traffic-data/output/weather/part-r-00000
```

---

### Step 8 — Shut Down (optional)

```bash
# Stop Docker cluster
docker-compose down
```

---

## 9. Expected Output Reference

### MapReduce Job 1 — Traffic by Hour

```
00      2038 vehicles/hour avg
01      1473 vehicles/hour avg
02      1037 vehicles/hour avg
03       840 vehicles/hour avg
04      1143 vehicles/hour avg
05      2174 vehicles/hour avg
06      3681 vehicles/hour avg
07      4703 vehicles/hour avg
08      4890 vehicles/hour avg
09      4672 vehicles/hour avg
10      4521 vehicles/hour avg
11      4566 vehicles/hour avg
12      4638 vehicles/hour avg
13      4695 vehicles/hour avg
14      4817 vehicles/hour avg
15      5113 vehicles/hour avg
16      5345 vehicles/hour avg
17      5468 vehicles/hour avg  ← typical peak hour
18      5179 vehicles/hour avg
19      4581 vehicles/hour avg
20      4010 vehicles/hour avg
21      3480 vehicles/hour avg
22      2934 vehicles/hour avg
23      2476 vehicles/hour avg
```

### MapReduce Job 2 — Traffic by Weather Condition

```
Clouds      3821 vehicles/hour avg
Clear       4039 vehicles/hour avg
Rain        3482 vehicles/hour avg
Drizzle     3209 vehicles/hour avg
Mist        3156 vehicles/hour avg
Fog         2843 vehicles/hour avg
Snow        2401 vehicles/hour avg
Haze        2987 vehicles/hour avg
Thunderstorm 3101 vehicles/hour avg
Squall      2200 vehicles/hour avg
```

### Spark Streaming — Expected Live Output Sample

```
========================================
  Smart Traffic Monitoring - Streaming
========================================
Master       : local[2]
Socket       : localhost:19999
Checkpoint   : /tmp/spark-checkpoint-traffic
Alert threshold: traffic_volume > 6000
========================================

[STATS] Running Average Traffic : 3539 vehicles/hour  (total processed: 152 records)

[WEATHER] Traffic by condition (last 30 s):
  Clouds               -> 45231 vehicles
  Rain                 -> 12800 vehicles
  Mist                 -> 2400 vehicles

+--------------------------------------+
|       ** CONGESTION ALERT **         |
+--------------------------------------+
|  Time    : 2012-10-02 16:00:00       |
|  Weather : Clear                     |
|  Volume  : 6015                      |
+--------------------------------------+

[STATS] Running Average Traffic : 3518 vehicles/hour  (total processed: 317 records)

[WEATHER] Traffic by condition (last 30 s):
  Clouds               -> 91400 vehicles
  Clear                -> 8015 vehicles
  Rain                 -> 9600 vehicles
  Drizzle              -> 3200 vehicles
```

---

## Quick Reference Card

```
┌────────────────────────────────────────────────────────────┐
│                   QUICK START COMMANDS                      │
├────────────────────────────────────────────────────────────┤
│ 1. Start cluster    docker-compose up -d                    │
│ 2. Start Spark      bash scripts/start-spark.sh             │
│ 3. Upload data      docker cp + hdfs dfs -put               │
│ 4. Run batch        bash scripts/run-mapreduce.sh           │
│ 5a. Producer        mvn exec:java TrafficDataProducer       │
│                       args: "CSV 19999 500"                  │
│ 5b. Streaming       mvn exec:java TrafficStreamingApp       │
│                       args: "local[2] localhost /tmp/ck 19999"│
│                                                              │
│ CRITICAL: Always use port 19999 (not 9999)                  │
│           Docker holds host port 9999 via docker-proxy       │
├────────────────────────────────────────────────────────────┤
│ Web UIs:                                                     │
│   HDFS    http://localhost:9870                              │
│   YARN    http://localhost:8088                              │
│   Spark   http://localhost:8080                              │
└────────────────────────────────────────────────────────────┘
```
