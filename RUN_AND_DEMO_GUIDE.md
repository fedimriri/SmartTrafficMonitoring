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
> Anonymous inner classes are used throughout Spark Streaming code instead of lambdas  
> specifically to preserve bytecode compatibility with Spark 2.4.5's ASM 5.x library.

Verify your tools:

```bash
java -version          # must show 1.8.x or 11.x
mvn -version           # must show 3.6+
docker --version       # must show 20.x+
docker compose version # or: docker-compose --version
```

---

### 1.2 System Architecture

All components — Hadoop, Spark, and the traffic data producer — run **inside Docker containers** on a shared bridge network named `hadoop`.

```
┌──────────────────────────────────────────────────────────────────────────┐
│                      Docker Network: hadoop                               │
│                                                                           │
│  ┌──────────────────┐  ┌───────────────────┐  ┌───────────────────┐     │
│  │   hadoop-master   │  │  hadoop-worker1   │  │  hadoop-worker2   │     │
│  │  HDFS NameNode    │  │    DataNode       │  │    DataNode       │     │
│  │  YARN ResourceMgr │  │    NodeManager    │  │    NodeManager    │     │
│  │  :9870  :8088     │  │                   │  │                   │     │
│  └──────────────────┘  └───────────────────┘  └───────────────────┘     │
│                                                                           │
│  ┌───────────────────────────────────────┐                               │
│  │              spark-master              │                               │
│  │  Spark Standalone Master  :8080 :7077  │                               │
│  │  TrafficDataProducer TCP Server  :9999 │  ← Producer lives here        │
│  └──────────────┬────────────────────────┘                               │
│                 │  spark://spark-master:7077                              │
│          ┌──────┴──────────┐                                             │
│          ▼                 ▼                                             │
│  ┌───────────────┐  ┌───────────────┐                                   │
│  │  spark-slave1  │  │  spark-slave2  │                                  │
│  │    Worker      │  │    Worker      │                                  │
│  │  Executor 0    │  │  Executor 1    │                                  │
│  └───────────────┘  └───────────────┘                                   │
└──────────────────────────────────────────────────────────────────────────┘
```

**Streaming execution model:**

```
TrafficDataProducer           SparkStreamingContext (Driver)
(spark-master, port 9999)     (spark-master, client deploy mode)
          │                              │
          │◄── TCP socket stream ────────┤  socketTextStream("spark-master", 9999)
          │                              │
          │                    ┌─────────┴──────────┐
          │                    ▼                    ▼
          │           Executor 0             Executor 1
          │          (spark-slave1)         (spark-slave2)
          │                    │                    │
          │                    └─────────┬──────────┘
          │                              ▼
          │                   Micro-batch processing
          │                   (filter · updateStateByKey · window)
          │                              │
          │                              ▼
          │                   Driver collects & prints:
          │                   STATS / WEATHER / CONGESTION ALERTS
```

**Data flow summary:**

- **Batch path:** CSV on HDFS → MapReduce on YARN → result on HDFS
- **Streaming path:** CSV on spark-master filesystem → TCP socket → Spark cluster (master + workers) → console output

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
│   ├── start-producer.sh       # Copy dataset + start TrafficDataProducer in spark-master
│   ├── start-streaming.sh      # Build JAR + spark-submit to cluster
│   ├── run-mapreduce.sh        # Build + copy JAR + run both MR jobs
│   └── connect-networks.sh     # One-time network bridge setup
│
└── target/
    └── SmartTrafficMonitoring-1.0-SNAPSHOT.jar   # Built JAR (29 KB, thin)
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

### 2.1 Compile the Project

All commands run from the project root directory.

```bash
# Clean and build — produces target/SmartTrafficMonitoring-1.0-SNAPSHOT.jar
mvn clean package -DskipTests

# Verify the JAR was created
ls -lh target/SmartTrafficMonitoring-1.0-SNAPSHOT.jar
# Expected: target/SmartTrafficMonitoring-1.0-SNAPSHOT.jar  ~29 KB
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

### 2.3 Spark Streaming — Cluster Mode (Primary Method)

> **The canonical approach is Spark cluster mode running entirely inside Docker.**
> See [Section 5](#5-spark-streaming-pipeline) for the full setup.
> This section documents the execution model for reference only.

The streaming job is submitted to the Spark standalone cluster using `spark-submit`. The workload is distributed across `spark-slave1` and `spark-slave2`. There is no host-machine execution and no `local[*]` mode.

Quick-start from the project root (after Section 3 cluster setup):

```bash
# 1. Start the data producer inside spark-master
bash scripts/start-producer.sh

# 2. Submit streaming job to the cluster (foreground — Ctrl-C to stop)
bash scripts/start-streaming.sh
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
| `spark-master` | `spark-image:latest` | Spark Standalone Master + Producer host | 8080, 7077, 9999 |
| `spark-slave1` | `spark-image:latest` | Spark Worker (Executor 0) | — |
| `spark-slave2` | `spark-image:latest` | Spark Worker (Executor 1) | — |

**Important design details:**

- `hadoop-master` auto-starts HDFS and YARN on container startup.
- Spark containers start with `tail -f /dev/null` — **Spark daemons do NOT auto-start.** You must run `bash scripts/start-spark.sh` after every `docker-compose up`.
- Port `9999` is mapped on `spark-master`. `TrafficDataProducer` listens on this port **inside the container**. Workers connect to `spark-master:9999` over the Docker network — no host-level binding needed for internal traffic.

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
1. Clears stale PID files from any previous run (prevents `start-master.sh` / `start-slave.sh` from refusing to start)
2. Starts Spark Master on `spark-master`
3. Waits 4 seconds for the Master to open port 7077
4. Starts Worker on `spark-slave1` (registers with `spark://spark-master:7077`)
5. Starts Worker on `spark-slave2`
6. Prints JPS output for each container

Expected output:

```
=== Clearing stale Spark PID files ===
=== Starting Spark Master ===
starting org.apache.spark.deploy.master.Master ...
Waiting 4 s for Master to register...
=== Starting Spark Workers ===
starting org.apache.spark.deploy.worker.Worker ...
starting org.apache.spark.deploy.worker.Worker ...
=== JVM processes ===
--- spark-master ---
216 Master
283 Jps
--- spark-slave1 ---
61 Worker
129 Jps
--- spark-slave2 ---
61 Worker
129 Jps

Spark Master UI : http://localhost:8080
Expected:  Master process on spark-master, Worker process on each slave.
```

Then confirm both workers are registered:

```bash
# "Alive Workers: 2" confirms both slaves are connected to the master
curl -s http://localhost:8080/ | grep -i "alive workers"
# Expected: Alive Workers: 2
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

### 5.1 Architecture and Execution Model

The streaming pipeline runs **entirely inside the Docker cluster**. The workload is distributed across the two Spark workers — no local execution, no `local[*]` mode.

```
┌─────────────────────────────────────────────────────────────────────┐
│                      spark-master container                          │
│                                                                      │
│  TrafficDataProducer                  SparkStreamingContext (Driver) │
│  TCP Server :9999                     --master spark://spark-master:7077│
│  reads CSV line-by-line               5-second micro-batches         │
│       │                                        │                     │
│       │◄──────── socketTextStream("spark-master", 9999) ────────────┤│
└───────┼────────────────────────────────────────┼─────────────────────┘
        │                                        │ distributes tasks
        │                           ┌────────────┴────────────┐
        │                           ▼                         ▼
        │                  ┌─────────────────┐    ┌─────────────────┐
        │                  │   spark-slave1  │    │   spark-slave2  │
        │                  │   Executor 0    │    │   Executor 1    │
        │                  │ (receives batch)│    │ (processes batch)│
        │                  └────────┬────────┘    └────────┬────────┘
        │                           └────────────┬──────────┘
        │                                        ▼
        │                              Driver collects & prints
        └──────────────────────────── STATS · WEATHER · ALERTS
```

**Roles in this project:**

| Component | Role |
|---|---|
| **Spark Master** | Accepts the `spark-submit` job, allocates executors to workers, tracks the application in the UI at :8080 |
| **Driver** (runs on spark-master, client mode) | Creates `JavaStreamingContext`, opens the socket connection, orchestrates micro-batch scheduling, collects results and prints output |
| **Executor 0** (spark-slave1) | Receives the socket stream as a long-running Receiver task; forwards blocks to the Driver |
| **Executor 1** (spark-slave2) | Processes transformation tasks (filter, mapToPair, updateStateByKey, window) dispatched by the Driver |

**Three real-time analytics run every 5-second batch:**

| Feature | Implementation | Output |
|---|---|---|
| Congestion Alert | `filter(volume > 6000).foreachRDD(print)` | Alert box per event |
| Running Average | `updateStateByKey` on key "GLOBAL" | Cumulative avg/count |
| Weather Aggregation | `reduceByKeyAndWindow(30s, 5s)` | Sum per weather condition |

---

### 5.2 Start the Streaming Pipeline

> **Prerequisite:** Docker cluster must be up and Spark cluster started (`bash scripts/start-spark.sh`). The JAR is built and deployed automatically by `start-streaming.sh`.

Open **three separate terminals**.

---

**Terminal 1 — Start the data producer (runs in background inside spark-master):**

```bash
bash scripts/start-producer.sh
```

What this script does:
1. Copies `dataset/Metro_Interstate_Traffic_Volume.csv` into `spark-master:/tmp/`
2. Stops any previously running producer on port 9999
3. Launches `TrafficDataProducer` as a background process inside `spark-master`
4. Logs go to `/tmp/producer.log` inside the container

Expected output:

```
=== Copying dataset into spark-master ===
=== Killing any previous producer on port 9999 ===
=== Starting TrafficDataProducer on spark-master:9999 (delay 500 ms) ===
Producer started in background.
  Listening on : spark-master:9999 (host port 9999)
  Log          : docker exec spark-master tail -f /tmp/producer.log
```

Verify it is waiting for a connection:

```bash
docker exec spark-master cat /tmp/producer.log
```

Expected:
```
========================================
  Smart Traffic Monitoring — Producer
========================================
Dataset : /tmp/Metro_Interstate_Traffic_Volume.csv
Port    : 9999
Delay   : 500 ms / record
Waiting for Spark Streaming to connect...
========================================
```

---

**Terminal 2 — Submit the streaming job to the Spark cluster (foreground):**

```bash
bash scripts/start-streaming.sh
```

What this script does:
1. Builds `target/SmartTrafficMonitoring-1.0-SNAPSHOT.jar` (`mvn package`)
2. Copies the JAR to `spark-master:/tmp/SmartTrafficMonitoring.jar`
3. Verifies that Master and both Workers are running
4. Runs `spark-submit` inside `spark-master` in interactive mode

The exact `spark-submit` command it issues (for reference):

```bash
docker exec -it spark-master \
  /opt/spark/bin/spark-submit \
    --class com.traffic.streaming.TrafficStreamingApp \
    --master spark://spark-master:7077 \
    --deploy-mode client \
    --conf spark.cores.max=2 \
    --conf spark.executor.cores=1 \
    --conf spark.streaming.receiver.writeAheadLog.enable=false \
    --conf spark.ui.port=4040 \
    /tmp/SmartTrafficMonitoring.jar \
    spark-master /tmp/spark-checkpoint-traffic 9999
```

**TrafficStreamingApp argument reference:**

| Position | Argument | Value |
|---|---|---|
| `args[0]` | Socket host | `spark-master` (reachable from all containers via Docker network) |
| `args[1]` | Checkpoint directory | `/tmp/spark-checkpoint-traffic` (local to driver on spark-master) |
| `args[2]` | Socket port | `9999` |

> **Why `spark-master` as socket host (not `localhost`):**  
> In cluster mode, the socket Receiver runs as a task on an executor — either on `spark-slave1` or `spark-slave2`. From those containers, `localhost` refers to themselves, not to the machine running the producer. The hostname `spark-master` resolves correctly from every container on the shared `hadoop` Docker network.

---

**Terminal 3 — Monitor producer progress:**

```bash
docker exec spark-master tail -f /tmp/producer.log
```

Once the streaming job connects you will see:

```
[PRODUCER] Spark connected from /172.20.0.X
[PRODUCER] Sent    20 records | Last: 2012-10-02 09:20:00,Clouds,4399
[PRODUCER] Sent    40 records | Last: 2012-10-02 09:40:00,Clouds,5765
[PRODUCER] Sent    60 records | Last: 2012-10-02 10:00:00,Clouds,4516
```

---

### 5.3 How the Producer Works

`TrafficDataProducer` acts as a **TCP server running inside `spark-master`**:

1. Opens a `ServerSocket` on port 9999, bound to all interfaces
2. Waits (blocks) until a client connects (the Spark Receiver task)
3. Once connected, reads the CSV file line by line
4. For each line, calls `TrafficRecord.fromCsvLine(line)`:
   - Skips the header line (`holiday,...`)
   - Parses `fields[7]` = date_time, `fields[5]` = weather_main, `fields[8]` = traffic_volume
5. Sends `record.toStreamFormat()` = `"date_time,weather_main,traffic_volume\n"` over the socket
6. Sleeps `delayMs` milliseconds between records (500 ms by default = ~2 records/second)
7. After the full dataset is sent, waits for the next connection (loops)

The producer is a plain Java process — it does **not** need Spark and does **not** run via `mvn exec:java`. It runs from the pre-built JAR:

```bash
# What start-producer.sh executes inside spark-master:
java -cp /tmp/SmartTrafficMonitoring.jar \
  com.traffic.streaming.TrafficDataProducer \
  /tmp/Metro_Interstate_Traffic_Volume.csv 9999 500
```

---

### 5.4 How Spark Streaming Processes Data

`TrafficStreamingApp` is submitted via `spark-submit` with `--master spark://spark-master:7077`:

1. The Spark Master allocates 2 executor slots (one per worker)
2. A `JavaStreamingContext` is created with a 5-second micro-batch interval
3. A checkpoint directory is set at `/tmp/spark-checkpoint-traffic` (required for `updateStateByKey`)
4. `socketTextStream("spark-master", 9999)` — the Receiver task runs on Executor 0 and connects to the producer
5. Every 5 seconds, the Driver schedules processing of the received blocks:
   - Filters empty lines
   - Parses each line: `parts[0]`=timestamp, `parts[1]`=weather, `parts[2]`=volume
   - Skips malformed lines (fault-tolerant via `flatMap`)
6. Three parallel pipelines run on the parsed `events` DStream
7. Results are collected on the Driver and printed to the console

> **Checkpoint note:** `/tmp/spark-checkpoint-traffic` is a local path on the Driver (spark-master). In `--deploy-mode client`, the Driver runs on spark-master, so this path is accessible. For production fault tolerance, replace with an HDFS path: `hdfs://hadoop-master:9000/spark-checkpoint-traffic`.

---

### 5.5 Manual Data Injection Inside Docker

You can inject records manually without the CSV producer. While the streaming app is running in Terminal 2, open a **fourth terminal** and connect directly to the producer socket port from inside the Docker network:

```bash
# From inside spark-master itself:
docker exec -it spark-master bash -c "echo '2026-01-01 08:00:00,Rain,8500' | nc spark-master 9999"
```

Or use interactive `nc` for multiple records:

```bash
docker exec -it spark-master bash
# Inside the container:
nc spark-master 9999
# Then type records one per line and press Enter:
2026-01-01 08:00:00,Rain,8500
2026-01-01 09:00:00,Clear,9100
2026-01-01 10:00:00,Snow,3100
# Press Ctrl+C to disconnect
```

You can also inject from the **host machine** since port 9999 is mapped:

```bash
# From host terminal:
printf "2026-01-01 08:00:00,Rain,8500\n2026-01-01 09:00:00,Clear,9100\n" \
    | nc localhost 9999
```

> **Note:** Manual injection only works if the streaming app has already established a connection to the producer's `ServerSocket`. The producer accepts only **one client at a time**. If the streaming app is connected, `nc` will block waiting. Stop the streaming app first, then use `nc` directly on port 9999 for manual-only testing.

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

**Inject command:**
```bash
printf "2012-10-02 09:00:00,Clouds,5545\n2012-10-02 10:00:00,Clouds,4516\n2012-10-02 11:00:00,Clouds,4767\n2012-10-02 12:00:00,Clouds,5026\n" \
    | nc localhost 9999
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

**Inject command:**
```bash
printf "2012-10-02 16:00:00,Clear,6015\n2012-10-02 17:00:00,Clear,6427\n2012-10-02 17:30:00,Clear,7102\n" \
    | nc localhost 9999
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

**Inject command:**
```bash
printf "2012-10-02 08:00:00,Rain,3200\n2012-10-02 08:30:00,Rain,2800\n2012-10-02 09:00:00,Clear,5545\n2012-10-02 09:30:00,Clear,6100\n2012-10-02 10:00:00,Fog,1200\n2012-10-02 10:30:00,Snow,800\n" \
    | nc localhost 9999
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

Run the producer at normal speed (500 ms/record) to stream the full historical dataset through the Spark cluster:

```bash
# Stop any running producer, restart at 100ms/record for a faster demo
docker exec spark-master pkill -f TrafficDataProducer 2>/dev/null || true
bash scripts/start-producer.sh 9999 100
```

At 100 ms/record, the first congestion alert (volume 6015) appears after approximately 2 minutes.

During a 5-minute demo at 100 ms/record you will see:
- ~3000 records processed
- Roughly 130+ congestion alerts fired
- Running average stabilizing around 3500–4000 vehicles/hour
- 6 to 8 distinct weather conditions in aggregation output
- Both workers (spark-slave1 and spark-slave2) actively executing tasks (visible in Spark UI at http://localhost:8080)

---

## 7. Verification & Debugging

### 7.1 Hadoop and Spark Web UIs

| UI | URL | What to check |
|---|---|---|
| HDFS NameNode | http://localhost:9870 | DataNodes connected, disk usage, file browser |
| YARN ResourceManager | http://localhost:8088 | Running/completed applications, job progress |
| Spark Master | http://localhost:8080 | Workers registered, active streaming application, worker utilization |
| Spark App UI | http://localhost:4040 | Active jobs, stages, receivers, streaming statistics |

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
# Spark Master process
docker exec spark-master jps
# Must include: Master

# Worker processes
docker exec spark-slave1 jps
# Must include: Worker

docker exec spark-slave2 jps
# Must include: Worker
```

**Verify streaming application is registered with Spark Master:**

```bash
# Should show "SmartTrafficMonitoring" with status RUNNING
curl -s http://localhost:8080/ | grep -i "SmartTrafficMonitoring"
```

**Confirm workers are receiving tasks** (check after streaming job starts):

```bash
# Executor 0 and Executor 1 should appear in the task log
docker exec spark-master grep "executor" /tmp/streaming.log | grep "Starting task" | head -10
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

**Streaming application log (if submitted via start-streaming.sh background):**
```bash
# Live tail of streaming output
docker exec spark-master tail -f /tmp/streaming.log

# Filter for specific events:
docker exec spark-master grep "\[STATS\]" /tmp/streaming.log
docker exec spark-master grep "CONGESTION" /tmp/streaming.log
docker exec spark-master grep "executor" /tmp/streaming.log | grep "Starting task" | head -20
```

**Producer log:**
```bash
docker exec spark-master tail -f /tmp/producer.log
```

---

### 7.3 Common Errors and Fixes

**Error: Spark daemons not running**

```
docker exec spark-master jps
# Only shows: Jps  (no Master process)
```

Cause: Spark daemons do not auto-start on container startup, or a previous crash left stale PID files.

Fix:
```bash
bash scripts/start-spark.sh
```

The updated script clears stale PID files before starting, so it is safe to re-run at any time.

---

**Error: Workers not connecting to Master**

```
# spark-slave1 jps shows Worker, but Spark UI at :8080 shows "Workers: 0"
```

Cause: Master was not fully started when `start-slave.sh` ran.

Fix:
```bash
# Increase the wait time or re-run the script
docker exec spark-master /opt/spark/sbin/stop-master.sh 2>/dev/null || true
docker exec spark-slave1 /opt/spark/sbin/stop-slave.sh  2>/dev/null || true
docker exec spark-slave2 /opt/spark/sbin/stop-slave.sh  2>/dev/null || true
sleep 2
bash scripts/start-spark.sh
```

---

**Error: `Connection refused` on socket (streaming cannot connect to producer)**

```
java.net.ConnectException: Connection refused (spark-master:9999)
```

Cause: Producer is not running, or the streaming app started before the producer.

Fix:
```bash
# Check producer is running inside spark-master
docker exec spark-master cat /tmp/producer.log | tail -5

# If not running, restart it
bash scripts/start-producer.sh
```

Always start the producer **before** submitting the streaming job.

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

While streaming runs, monitor specific event types in real time:

```bash
# Watch only congestion alerts (from streaming log inside container)
docker exec spark-master tail -f /tmp/streaming.log | grep -A 6 "CONGESTION ALERT"

# Watch only stats
docker exec spark-master tail -f /tmp/streaming.log | grep "\[STATS\]"

# Watch only weather aggregation
docker exec spark-master tail -f /tmp/streaming.log | grep -A 5 "\[WEATHER\]"

# Confirm tasks are being dispatched to both workers
docker exec spark-master tail -f /tmp/streaming.log | grep "executor [01]"
```

If running `start-streaming.sh` in the foreground (interactive), pipe its output:

```bash
bash scripts/start-streaming.sh 2>&1 | tee /tmp/streaming-local.log
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

# Confirm port 9999 is mapped on spark-master
docker ps | grep spark-master
# Expected: 0.0.0.0:9999->9999/tcp in the PORTS column
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
- Open http://localhost:8080 — Spark Master showing **2 alive Workers**

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

### Step 4 — Launch Producer Inside spark-master (Terminal 1)

```bash
bash scripts/start-producer.sh
```

Verify it is running and waiting:

```bash
docker exec spark-master cat /tmp/producer.log
# Expected last line: "Waiting for Spark Streaming to connect..."
```

**What to explain to the evaluator:**
- The producer is a TCP server process running **inside the `spark-master` container**
- It listens on port 9999, which is reachable from all containers via the Docker network
- It will begin streaming records as soon as the Spark job connects

---

### Step 5 — Submit Streaming Application to Spark Cluster (Terminal 2)

```bash
bash scripts/start-streaming.sh
```

**What to explain to the evaluator as the job starts:**

1. The JAR is built and copied to `spark-master:/tmp/SmartTrafficMonitoring.jar`
2. `spark-submit` sends the job to the Master at `spark://spark-master:7077`
3. The Master allocates **Executor 0 on spark-slave1** and **Executor 1 on spark-slave2**
4. The Driver (running on spark-master in client mode) schedules micro-batches every 5 seconds

Look for these lines confirming true cluster execution:

```
Granted executor ID .../0 on hostPort 172.20.0.X:...  ← spark-slave1
Granted executor ID .../1 on hostPort 172.20.0.Y:...  ← spark-slave2

Starting task 0.0 in stage 0.0 ... executor 1, partition 0
Starting task 1.0 in stage 0.0 ... executor 0, partition 1
Finished task 0.0 in stage 0.0 ... on 172.20.0.X (executor 1)
Finished task 1.0 in stage 0.0 ... on 172.20.0.Y (executor 0)
```

**Open Spark UI** at http://localhost:8080 — show `SmartTrafficMonitoring` listed under **Running Applications** with 2 active executors.

---

### Step 6 — Observe Real-Time Streaming Output (3 minutes)

In Terminal 2, point the evaluator to these outputs appearing every 5 seconds:

**Weather aggregation (appears after first 30-second window fills):**
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

### Step 7 — Inject Manual Traffic Records (Terminal 3)

Open a third terminal and inject extreme values to demonstrate real-time detection:

```bash
# From the host (port 9999 is mapped from spark-master to host)
printf "2026-01-01 08:00:00,Rain,8500\n2026-01-01 09:00:00,Clear,9100\n2026-01-01 10:00:00,Snow,7800\n" \
    | nc localhost 9999
```

Or from inside the Docker network:

```bash
docker exec -it spark-master bash -c \
  "printf '2026-01-01 08:00:00,Rain,8500\n2026-01-01 09:00:00,Clear,9100\n' | nc spark-master 9999"
```

Within 5 seconds, Terminal 2 will show **3 congestion alerts**:

```
+--------------------------------------+
|       ** CONGESTION ALERT **         |
+--------------------------------------+
|  Time    : 2026-01-01 08:00:00       |
|  Weather : Rain                      |
|  Volume  : 8500                      |
+--------------------------------------+

+--------------------------------------+
|       ** CONGESTION ALERT **         |
+--------------------------------------+
|  Time    : 2026-01-01 09:00:00       |
|  Weather : Clear                     |
|  Volume  : 9100                      |
+--------------------------------------+
```

---

### Step 8 — Verify Spark UI with Active Application and Workers

While the streaming job is running, open http://localhost:8080 and show:

- **Workers (2):** both `spark-slave1` and `spark-slave2` listed with cores and memory
- **Running Applications (1):** `SmartTrafficMonitoring` with status `RUNNING`
- Click on the application → shows **2 executors**, last task time, memory used

```bash
# Confirm via CLI as well
docker exec spark-master jps
# Expected: Master, SparkSubmit (the Driver)

docker exec spark-slave1 jps
# Expected: Worker, CoarseGrainedExecutorBackend

docker exec spark-slave2 jps
# Expected: Worker, CoarseGrainedExecutorBackend
```

---

### Step 9 — Stop and Verify (30 seconds)

```bash
# Stop streaming (Ctrl+C in Terminal 2)

# Show checkpoint directory was created (proof of stateful processing)
docker exec spark-master ls /tmp/spark-checkpoint-traffic/

# Show HDFS batch results one more time
docker exec hadoop-master hdfs dfs -cat /traffic-data/output/weather/part-r-00000
```

---

### Step 10 — Shut Down (optional)

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

### Spark Streaming — Startup Banner (Cluster Mode)

```
========================================
  Smart Traffic Monitoring - Streaming
========================================
Socket       : spark-master:9999
Checkpoint   : /tmp/spark-checkpoint-traffic
Alert threshold: traffic_volume > 6000
========================================
```

### Spark Streaming — Executor Allocation (confirms cluster mode)

```
INFO StandaloneSchedulerBackend: Granted executor ID app-.../0 on hostPort 172.20.0.X:... with 1 core(s), 1024.0 MB RAM
INFO StandaloneSchedulerBackend: Granted executor ID app-.../1 on hostPort 172.20.0.Y:... with 1 core(s), 1024.0 MB RAM
INFO CoarseGrainedSchedulerBackend$DriverEndpoint: Registered executor ... (172.20.0.X:...) with ID 0
INFO CoarseGrainedSchedulerBackend$DriverEndpoint: Registered executor ... (172.20.0.Y:...) with ID 1
```

### Spark Streaming — Worker Task Dispatch

```
INFO TaskSetManager: Starting task 0.0 in stage 0.0 (TID 0, 172.20.0.X, executor 1, partition 0, ...)
INFO TaskSetManager: Starting task 1.0 in stage 0.0 (TID 1, 172.20.0.Y, executor 0, partition 1, ...)
INFO TaskSetManager: Finished task 0.0 in stage 0.0 (TID 0) in 1403 ms on 172.20.0.X (executor 1) (1/50)
INFO TaskSetManager: Finished task 1.0 in stage 0.0 (TID 1) in 1346 ms on 172.20.0.Y (executor 0) (2/50)
```

### Spark Streaming — Live Analytics Output Sample

```
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
┌────────────────────────────────────────────────────────────────┐
│                    QUICK START COMMANDS                         │
├────────────────────────────────────────────────────────────────┤
│ 1. Start cluster     docker-compose up -d                       │
│ 2. Start Spark       bash scripts/start-spark.sh               │
│ 3. Upload data       docker cp + hdfs dfs -put                 │
│ 4. Run batch         bash scripts/run-mapreduce.sh             │
│ 5. Start producer    bash scripts/start-producer.sh            │
│ 6. Submit streaming  bash scripts/start-streaming.sh           │
│                                                                  │
│ Streaming runs on:  spark://spark-master:7077                   │
│ Workers:            spark-slave1 (Executor 0)                   │
│                     spark-slave2 (Executor 1)                   │
│ Producer location:  inside spark-master, port 9999             │
├────────────────────────────────────────────────────────────────┤
│ Web UIs:                                                         │
│   HDFS      http://localhost:9870                               │
│   YARN      http://localhost:8088                               │
│   Spark     http://localhost:8080  (shows active workers + app) │
│   App UI    http://localhost:4040  (streaming stats, executors) │
├────────────────────────────────────────────────────────────────┤
│ Manual record injection (triggers congestion alerts):           │
│   printf "2026-01-01 08:00:00,Rain,8500\n" | nc localhost 9999 │
└────────────────────────────────────────────────────────────────┘
```
