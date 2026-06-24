# Smart Traffic Monitoring System
## Final Defense Report

**Course:** Big Data Engineering  
**Academic Year:** 2025–2026  
**Author:** Fadi Mriri  
**Date:** June 2026  
**Technologies:** Apache Hadoop 3.3.6 · Apache Spark Streaming 2.4.5 · Java 8 · Docker  

---

## Table of Contents

1. [Introduction](#1-introduction)
2. [Objectives](#2-objectives)
3. [Dataset Description](#3-dataset-description)
4. [Technologies Used](#4-technologies-used)
5. [HDFS Architecture](#5-hdfs-architecture)
6. [MapReduce Job 1 — Traffic by Hour](#6-mapreduce-job-1--traffic-by-hour)
7. [MapReduce Job 2 — Traffic by Weather](#7-mapreduce-job-2--traffic-by-weather)
8. [Spark Streaming Pipeline](#8-spark-streaming-pipeline)
9. [Results](#9-results)
10. [Screenshots](#10-screenshots)
11. [Conclusion](#11-conclusion)

---

## 1. Introduction

Road traffic congestion is a major urban challenge, driving up commute times, fuel consumption, and carbon emissions. Data-driven approaches to traffic analysis can help city planners and transport authorities make better operational decisions.

This project, **SmartTrafficMonitoring**, implements a complete Big Data pipeline for road traffic analysis using Apache Hadoop and Apache Spark. It combines two complementary processing paradigms:

- **Batch processing** with Hadoop MapReduce for historical traffic analysis over the full dataset
- **Stream processing** with Apache Spark Streaming for real-time monitoring and congestion alerting

The system runs on a 6-node Docker cluster (3 Hadoop nodes + 3 Spark nodes), faithfully reproducing a production-grade Big Data environment on a single development machine.

---

## 2. Objectives

| # | Objective | Achieved |
|---|---|---|
| 1 | Ingest the Metro Interstate Traffic Volume dataset into HDFS | ✅ |
| 2 | Compute average traffic volume per hour of day using MapReduce | ✅ |
| 3 | Compute average traffic volume per weather condition using MapReduce | ✅ |
| 4 | Identify peak and lowest traffic hours and weather conditions | ✅ |
| 5 | Simulate a live traffic stream from the dataset | ✅ |
| 6 | Detect congestion events in real time (volume > 6000) | ✅ |
| 7 | Compute a running global traffic average across all processed records | ✅ |
| 8 | Aggregate traffic by weather condition in a sliding window (30 s / 5 s) | ✅ |
| 9 | Deploy the full system on a Docker-based Hadoop + Spark cluster | ✅ |
| 10 | Persist MapReduce results to HDFS for reporting | ✅ |

---

## 3. Dataset Description

**Dataset:** Metro Interstate Traffic Volume  
**Source:** UCI Machine Learning Repository (publicly available)  
**File:** `dataset/Metro_Interstate_Traffic_Volume.csv`  
**HDFS Path:** `/traffic-data/historical/Metro_Interstate_Traffic_Volume.csv`  

### Statistics

| Attribute | Value |
|---|---|
| Total records | 48,204 rows |
| Time range | 2012 – 2018 |
| Location | Interstate 94, Minneapolis–St. Paul, MN |
| Granularity | Hourly observations |
| File size | ~3.1 MB |

### CSV Schema

```
holiday, temp, rain_1h, snow_1h, clouds_all, weather_main, weather_description, date_time, traffic_volume
```

| Column | Index | Type | Used by |
|---|---|---|---|
| `weather_main` | 5 | String (11 categories) | MapReduce Job 2, Spark Streaming |
| `date_time` | 7 | `YYYY-MM-DD HH:mm:ss` | MapReduce Job 1, Spark Streaming |
| `traffic_volume` | 8 | Integer (0 – 7,280) | Both MapReduce jobs, Spark Streaming |

### Weather Categories

`Clear`, `Clouds`, `Drizzle`, `Fog`, `Haze`, `Mist`, `Rain`, `Smoke`, `Snow`, `Squall`, `Thunderstorm`

---

## 4. Technologies Used

| Layer | Technology | Version |
|---|---|---|
| Language | Java | 8 (OpenJDK) |
| Batch Framework | Apache Hadoop MapReduce | 3.3.6 |
| Distributed Storage | HDFS | 3.3.6 |
| Cluster Manager | YARN | 3.3.6 |
| Streaming Framework | Apache Spark Streaming | 2.4.5 |
| Cluster Mode | Spark Standalone | 2.4.5 |
| Build Tool | Apache Maven | 3.x |
| Deployment | Docker + Docker Compose | 20.x / 3.8 |
| Container Images | `liliasfaxi/hadoop-cluster`, `spark-image` | custom |

### Project Structure

```
SmartTrafficMonitoring/
├── dataset/
│   └── Metro_Interstate_Traffic_Volume.csv
├── docs/
│   ├── Final_Project_Report.md         ← this file
│   └── architecture-diagram.md
├── scripts/
│   ├── start-spark.sh                  ← start Spark daemons
│   ├── connect-networks.sh             ← connect Docker networks
│   └── run-mapreduce.sh                ← full MapReduce pipeline
├── src/main/java/com/traffic/
│   ├── mapreduce/hourly/               ← Job 1 (Driver, Mapper, Reducer)
│   ├── mapreduce/weather/              ← Job 2 (Driver, Mapper, Reducer)
│   ├── streaming/
│   │   ├── TrafficStreamingApp.java    ← Spark Streaming consumer
│   │   ├── TrafficDataProducer.java    ← TCP socket producer
│   │   └── TrafficRecord.java          ← CSV row POJO
│   └── utils/
│       └── CsvParser.java
├── docker-compose.yml
└── pom.xml
```

---

## 5. HDFS Architecture

### Cluster Topology

The Hadoop cluster runs across 3 Docker containers, all on the `hadoop` Docker bridge network:

| Container | Role | Daemons |
|---|---|---|
| `hadoop-master` | Master node | NameNode, SecondaryNameNode, ResourceManager, NodeManager |
| `hadoop-worker1` | Worker node | DataNode, NodeManager |
| `hadoop-worker2` | Worker node | DataNode, NodeManager |

### HDFS Directory Layout

```
/traffic-data/
├── historical/
│   └── Metro_Interstate_Traffic_Volume.csv    ← source dataset (3.1 MB, 2 replicas)
├── streaming/
│   └── incoming-data/                         ← reserved for streaming output
└── output/
    ├── hourly/
    │   └── part-r-00000                       ← Job 1 result (24 averages + peak/lowest)
    └── weather/
        └── part-r-00000                       ← Job 2 result (11 averages + highest/lowest)
```

### Key HDFS Parameters

| Parameter | Value |
|---|---|
| Replication factor | 2 |
| Block size | 128 MB |
| NameNode port | 9000 (RPC), 9870 (Web UI) |
| DataNode storage | Default (`/tmp/hadoop-root/dfs/data`) |

### HDFS Commands Used

```bash
# Upload dataset
docker exec hadoop-master hdfs dfs -mkdir -p /traffic-data/historical
docker exec hadoop-master hdfs dfs -put /tmp/Metro_Interstate_Traffic_Volume.csv /traffic-data/historical/

# Verify
docker exec hadoop-master hdfs dfs -ls -R /traffic-data
docker exec hadoop-master hdfs dfsadmin -report
```

---

## 6. MapReduce Job 1 — Traffic by Hour

### Goal

Compute the average traffic volume for each hour of the day (0–23) across all 48,204 records, and identify the peak and lowest traffic hours.

### Implementation

**Class:** `com.traffic.mapreduce.hourly.TrafficHourDriver`

**Mapper** (`TrafficHourMapper`):
- Input: one CSV line
- Skips header and malformed lines
- Extracts hour from `date_time` column (index 7): `"2012-10-02 09:00:00"` → `"09"`
- Extracts `traffic_volume` (index 8)
- Emits: `(hour_string, traffic_volume_int)`

**Reducer** (`TrafficHourReducer`):
- Groups all volumes by hour
- Computes: `avg = sum / count`
- Tracks peak and lowest hour across all 24 keys using instance variables
- `cleanup()` emits `PEAK_HOUR` and `LOWEST_HOUR` entries

> **Design note:** No Combiner is used. Combiners operate on partial sums and cannot correctly compute averages — adding a Combiner would produce wrong averages.

**Job Configuration:**

```java
job.setMapOutputKeyClass(Text.class);
job.setMapOutputValueClass(IntWritable.class);
job.setOutputKeyClass(Text.class);
job.setOutputValueClass(Text.class);   // Text output: "avg" or "HH (avg = N vehicles/hour)"
```

### Execution

```bash
# Submit to YARN
docker exec hadoop-master hadoop jar /tmp/SmartTrafficMonitoring.jar \
    com.traffic.mapreduce.hourly.TrafficHourDriver \
    /traffic-data/historical/Metro_Interstate_Traffic_Volume.csv \
    /traffic-data/output/hourly

# Read output
docker exec hadoop-master hdfs dfs -cat /traffic-data/output/hourly/part-r-00000
```

### Output (excerpt)

```
00      834
01      516
02      411
03      371
04      524
05      1170
06      2850
07      4354
08      4842
09      4641
10      4465
11      4707
12      4892
13      4841
14      5076
15      5558
16      5663
17      5548
18      5030
19      4286
20      3697
21      2853
22      2088
23      1469
PEAK_HOUR       16  (avg = 5663 vehicles/hour)
LOWEST_HOUR     03  (avg = 371 vehicles/hour)
```

**Insight:** Peak traffic occurs at 16:00 (4 PM), consistent with the end of the American workday. Minimum traffic is at 03:00 AM.

---

## 7. MapReduce Job 2 — Traffic by Weather

### Goal

Compute the average traffic volume for each weather condition, and identify which condition correlates with the highest and lowest traffic volumes.

### Implementation

**Class:** `com.traffic.mapreduce.weather.TrafficWeatherDriver`

**Mapper** (`TrafficWeatherMapper`):
- Extracts `weather_main` (column index 5) as key
- Extracts `traffic_volume` (column index 8) as value
- Emits: `(weather_condition, traffic_volume_int)`

**Reducer** (`TrafficWeatherReducer`):
- Same average-compute + peak-tracking pattern as Job 1
- `cleanup()` emits `HIGHEST_WEATHER` and `LOWEST_WEATHER`

### Execution

```bash
docker exec hadoop-master hadoop jar /tmp/SmartTrafficMonitoring.jar \
    com.traffic.mapreduce.weather.TrafficWeatherDriver \
    /traffic-data/historical/Metro_Interstate_Traffic_Volume.csv \
    /traffic-data/output/weather

docker exec hadoop-master hdfs dfs -cat /traffic-data/output/weather/part-r-00000
```

### Output

```
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

**Insight:** Cloudy conditions correlate with the highest average traffic (3,618 vehicles/hour), while Squall (severe weather) has the lowest (2,061 vehicles/hour), which is counterintuitive — severe weather appears to deter traffic more than any other condition.

---

## 8. Spark Streaming Pipeline

### Architecture

The streaming pipeline uses two components:

1. **`TrafficDataProducer`** — a `ServerSocket` on port 9999 that reads the CSV dataset row-by-row and streams records in real time, simulating live sensor data
2. **`TrafficStreamingApp`** — a Spark Streaming application consuming the socket stream

### Producer: TrafficDataProducer

```
Input:   Metro_Interstate_Traffic_Volume.csv (local file)
Output:  TCP socket :9999
Format:  "date_time,weather_main,traffic_volume"
Rate:    Configurable (default: 500 ms/record = ~2 records/second)
```

The producer loops indefinitely through the dataset (loops back to start after the last record), simulating a continuous sensor feed.

### Consumer: TrafficStreamingApp

**Spark Configuration:**
- Batch interval: 5 seconds
- Master: configurable (default `local[2]`, supports `spark://spark-master:7077`)
- Checkpoint: enabled (required for `updateStateByKey`)

**Three analytics streams:**

#### 1 — Congestion Detection (Stateless)
```java
events.filter(e -> e.trafficVolume > 6000)
      .foreachRDD(rdd -> { /* print boxed CONGESTION ALERT */ });
```
Threshold: 6,000 vehicles/hour per project specification.

#### 2 — Running Global Average (Stateful)
```java
events.mapToPair(e -> new Tuple2<>("GLOBAL", e.trafficVolume))
      .updateStateByKey(updateAvgFn)    // state: long[]{sum, count}
      .foreachRDD(rdd -> { /* print running avg */ });
```
Accumulates sum and count across ALL batches since startup.

#### 3 — Weather Window Aggregation (Windowed)
```java
events.mapToPair(e -> new Tuple2<>(e.weather, e.trafficVolume))
      .reduceByKeyAndWindow((a, b) -> a + b,
                             Durations.seconds(30),
                             Durations.seconds(5))
      .foreachRDD(rdd -> { /* print weather totals */ });
```
Aggregates total traffic per weather condition over the last 30 seconds, updated every 5 seconds (the batch interval).

### Running the Pipeline

**Local mode** (both driver and receiver in one JVM):
```bash
# Terminal 1 — start producer
mvn exec:java -Dexec.mainClass="com.traffic.streaming.TrafficDataProducer" \
              -Dexec.args="dataset/Metro_Interstate_Traffic_Volume.csv 9999 500"

# Terminal 2 — start streaming app (local mode)
mvn exec:java -Dexec.mainClass="com.traffic.streaming.TrafficStreamingApp"
```

**Cluster mode** (Spark standalone — verified):
```bash
# 1. Start producer inside hadoop-master (accessible from all cluster nodes)
docker exec -d hadoop-master java -cp /tmp/SmartTrafficMonitoring.jar \
    com.traffic.streaming.TrafficDataProducer \
    /tmp/Metro_Interstate_Traffic_Volume.csv 9999 200

# 2. Submit to Spark cluster from spark-master
docker exec spark-master /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --class com.traffic.streaming.TrafficStreamingApp \
    /tmp/SmartTrafficMonitoring.jar \
    spark://spark-master:7077 hadoop-master /tmp/spark-checkpoint
```

> **Cluster mode verification result:** Successfully connected to `spark://spark-master:7077`. Both executors allocated on spark-slave1 and spark-slave2 (8 cores, 1024 MB each). Application ID: `app-20260624150306-0000`.

### Sample Output

```
========================================
  Smart Traffic Monitoring — Streaming
========================================

╔══════════════════════════════════════════════╗
║           ⚠  CONGESTION ALERT  ⚠             ║
╠══════════════════════════════════════════════╣
║  Time    : 2012-10-02 09:00:00               ║
║  Weather : Clouds                            ║
║  Volume  : 7120                              ║
╚══════════════════════════════════════════════╝

[STATS] Running Average Traffic : 4823 vehicles/hour  (total processed: 540 records)

[WEATHER] Traffic by condition (last 30 s):
  Clouds               ->  43200 vehicles
  Clear                ->  18400 vehicles
  Rain                 ->   9900 vehicles
```

---

## 9. Results

### MapReduce Results Summary

| Metric | Value |
|---|---|
| Dataset records processed | 48,204 |
| YARN MapReduce executions | 2 (both SUCCEEDED) |
| Peak hour | 16:00 (avg 5,663 vehicles/hour) |
| Lowest hour | 03:00 (avg 371 vehicles/hour) |
| Highest-traffic weather | Clouds (avg 3,618 vehicles/hour) |
| Lowest-traffic weather | Squall (avg 2,061 vehicles/hour) |

### Spark Streaming Results Summary

| Feature | Implementation |
|---|---|
| Congestion threshold | 6,000 vehicles/hour |
| Alert latency | ≤ 5 seconds (one batch interval) |
| Running average | Maintained across all micro-batches (updateStateByKey) |
| Weather window | Last 30 seconds, updated every 5 seconds |
| Cluster mode | Verified on spark://spark-master:7077 |

### System Verification

| Component | Status |
|---|---|
| HDFS NameNode | ✅ Running |
| YARN ResourceManager | ✅ Running |
| DataNode × 2 | ✅ Running (2 workers) |
| NodeManager × 3 | ✅ Running |
| Spark Master | ✅ Running |
| Spark Worker × 2 | ✅ Running |
| MapReduce Job 1 | ✅ SUCCEEDED (YARN) |
| MapReduce Job 2 | ✅ SUCCEEDED (YARN) |
| Streaming (local mode) | ✅ Verified |
| Streaming (cluster mode) | ✅ Verified |

---

## 10. Screenshots

> **Instructions for defense:** Replace each placeholder below with the corresponding screenshot taken during a live demo.

### 10.1 — HDFS NameNode Web UI (port 9870)

```
[ Screenshot: http://localhost:9870 ]
Expected: NameNode status "started", 2 DataNodes live,
          capacity usage, /traffic-data directory tree
```

### 10.2 — HDFS Directory Listing

```
[ Screenshot: docker exec hadoop-master hdfs dfs -ls -R /traffic-data ]
Expected: historical/, output/hourly/, output/weather/, streaming/
```

### 10.3 — YARN ResourceManager Web UI (port 8088)

```
[ Screenshot: http://localhost:8088 ]
Expected: 2 applications (hourly, weather), both FINISHED/SUCCEEDED
```

### 10.4 — MapReduce Job 1 Output

```
[ Screenshot: hdfs dfs -cat /traffic-data/output/hourly/part-r-00000 ]
Expected: 24 hour averages (00–23), PEAK_HOUR, LOWEST_HOUR
```

### 10.5 — MapReduce Job 2 Output

```
[ Screenshot: hdfs dfs -cat /traffic-data/output/weather/part-r-00000 ]
Expected: 11 weather categories, HIGHEST_WEATHER, LOWEST_WEATHER
```

### 10.6 — Spark Master Web UI (port 8080)

```
[ Screenshot: http://localhost:8080 ]
Expected: 2 workers (spark-slave1, spark-slave2), both ALIVE,
          total cores and memory shown
```

### 10.7 — Spark Streaming in Action (local mode)

```
[ Screenshot: terminal running TrafficStreamingApp ]
Expected: CONGESTION ALERT box, running average stats, weather window output
```

### 10.8 — Spark Cluster Mode (spark://spark-master:7077)

```
[ Screenshot: spark-submit output showing executor allocation ]
Expected: "Executor added: app-XXXXX/0 on worker... 8 core(s)"
          "Executor updated: ... is now RUNNING"
```

### 10.9 — Docker Containers (all 6 running)

```
[ Screenshot: docker ps ]
Expected: hadoop-master, hadoop-worker1, hadoop-worker2,
          spark-master, spark-slave1, spark-slave2 — all Up
```

---

## 11. Conclusion

This project successfully implements a complete Big Data monitoring system for road traffic using industry-standard open-source technologies.

### What was accomplished

The system demonstrates both pillars of modern Big Data engineering:

1. **Batch analytics** — The two MapReduce jobs process the full 48,204-record dataset through the YARN cluster, producing accurate hourly and weather-based traffic profiles stored in HDFS. The computation of running averages with peak/lowest identification shows a realistic use of the Reducer `cleanup()` lifecycle hook.

2. **Stream analytics** — The Spark Streaming pipeline provides three concurrent analytics (congestion detection, running average, windowed weather aggregation) on a live socket stream with 5-second batch intervals. The use of `updateStateByKey` and `reduceByKeyAndWindow` demonstrates stateful and windowed streaming operations.

### Technical highlights

- Verified execution on a real 6-node Docker cluster (not a simulation)
- Both MapReduce jobs completed successfully via YARN (application IDs confirmed)
- Spark Streaming verified in both local mode and standalone cluster mode (`spark://spark-master:7077`)
- The `TrafficDataProducer` loops the dataset indefinitely, providing a continuous stream without modifying the original data

### Lessons learned

| Challenge | Solution |
|---|---|
| Wrong `Optional` type (Guava vs Spark) | `org.apache.spark.api.java.Optional` is the correct type for Spark 2.4.5 Java API |
| MapReduce output directory must not exist | Add `hdfs dfs -rm -r` before each job |
| Combiners cannot compute averages | No Combiner added; full shuffle required for correct average computation |
| Spark daemons do not auto-restart | `scripts/start-spark.sh` must be run after every container restart |
| `updateStateByKey` requires checkpoint | `ssc.checkpoint(dir)` called before stream operations |
| `SerializationException` in Spark | All POJOs passed through DStreams must implement `java.io.Serializable` |

---

*Report generated for academic defense — SmartTrafficMonitoring Project, June 2026*
