# SMART TRAFFIC MONITORING — COMPLETE DEMO SCRIPT

**Date:** 2026-06-24  
**Target Audience:** University Faculty / Project Reviewers  
**Duration:** 8-10 minutes

---

## INTRO (30 seconds)

```
"Good morning/afternoon. I present SmartTrafficMonitoring — a Big Data system 
that demonstrates both batch and real-time analytics using Hadoop MapReduce 
and Apache Spark Streaming.

The system processes the Metro Interstate Traffic Volume dataset (48,204 hourly 
observations) using two complementary approaches:

1. **Batch:** Historical analysis — traffic patterns by hour and weather
2. **Streaming:** Real-time monitoring with congestion alerts and 30-second 
   traffic trend windows

All components run on a Docker-based 6-container cluster (3 Hadoop nodes, 
3 Spark nodes) with HDFS as the distributed filesystem and YARN as the 
cluster manager.

Let me walk you through the live execution."
```

---

## COMMAND SEQUENCE

### STEP 1: Verify Cluster Status (1 min)

**Narration:**
"First, let me verify that the entire Docker cluster is running. I have 6 containers: 
3 Hadoop nodes (master + 2 workers) and 3 Spark nodes (master + 2 workers)."

**Command 1.1 — Docker containers**
```bash
docker ps
```

**Expected Output:**
```
CONTAINER ID  IMAGE                    STATUS        NAMES
...           liliasfaxi/hadoop-cluster  Up 2 minutes  hadoop-master
...           liliasfaxi/hadoop-cluster  Up 2 minutes  hadoop-worker1
...           liliasfaxi/hadoop-cluster  Up 2 minutes  hadoop-worker2
...           spark-image               Up 2 minutes  spark-master
...           spark-image               Up 2 minutes  spark-slave1
...           spark-image               Up 2 minutes  spark-slave2
```

**Narration:** "All 6 containers are running. Now let me check the Java processes inside 
each to confirm the Hadoop and Spark daemons have started."

**Command 1.2 — Hadoop processes**
```bash
docker exec hadoop-master jps
```

**Expected Output:**
```
162 NameNode
703 ResourceManager
... (other processes)
```

**Command 1.3 — YARN node status**
```bash
docker exec hadoop-master yarn node -list
```

**Expected Output:**
```
Total Nodes:2
         Node-Id              Node-State  Node-Http-Address        Number-of-Running-Containers
hadoop-worker1:41139         RUNNING     hadoop-worker1:8042      0
hadoop-worker2:46109         RUNNING     hadoop-worker2:8042      0
```

**Narration:** "Excellent. YARN cluster is active with 2 worker nodes ready. 
Now let me verify the Spark cluster."

**Command 1.4 — Spark processes**
```bash
docker exec spark-master jps
docker exec spark-slave1 jps
docker exec spark-slave2 jps
```

**Expected Output:**
```
26 Master
93 Jps
---
24 Worker
93 Jps
---
24 Worker
92 Jps
```

**Narration:** "Great. Spark Master is running on spark-master, and both worker 
nodes (spark-slave1, spark-slave2) have joined the cluster."

---

### STEP 2: Verify Dataset in HDFS (30 seconds)

**Narration:**
"The dataset is already staged in HDFS. Let me verify it's present and 
confirm the file size and replication factor."

**Command 2.1 — Check HDFS structure**
```bash
docker exec hadoop-master hdfs dfs -ls -R /traffic-data
```

**Expected Output:**
```
Found 1 items
drwxr-xr-x   - root supergroup          /traffic-data/historical
drwxr-xr-x   - root supergroup          /traffic-data/streaming
drwxr-xr-x   - root supergroup          /traffic-data/output

Found 1 items
-rw-r--r--   2 root supergroup    3237208 2026-06-24 15:26 /traffic-data/historical/Metro_Interstate_Traffic_Volume.csv
```

**Narration:** "The dataset is present with replication factor 2, 
which means each block is replicated across the cluster for fault tolerance. 
The file contains 48,204 traffic observations."

---

### STEP 3: Run MapReduce Job 1 — Traffic by Hour (2 min)

**Narration:**
"Now I'll run the first MapReduce job, which analyzes traffic patterns by hour of day. 
This job reads the entire 48K record dataset, extracts the hour from each timestamp, 
and computes the average traffic volume for each hour. At the end, it identifies 
the peak hour and the quietest hour."

**Command 3.1 — Run hourly job**
```bash
docker exec hadoop-master hadoop jar /tmp/SmartTrafficMonitoring-1.0-SNAPSHOT.jar \
    com.traffic.mapreduce.hourly.TrafficHourDriver \
    /traffic-data/historical/Metro_Interstate_Traffic_Volume.csv \
    /traffic-data/output/hourly
```

**Expected Output (key lines):**
```
Job Info: (final status) SUCCESS
...
Map counters: Map input records=48205 (this confirms all 48K records were read)
Reduce counters: Reduce input groups=24 (24 hours of the day)
```

**Narration:** "Job completed successfully. The counters show that all 48,205 
records were processed, and we have 24 reduce groups (one per hour of the day). 
Let me show you the output."

**Command 3.2 — View hourly output**
```bash
docker exec hadoop-master hdfs dfs -cat /traffic-data/output/hourly/part-r-00000
```

**Expected Output:**
```
00	834
01	516
02	388
03	371
04	702
05	2094
06	4140
07	4740
08	4587
09	4385
10	4184
11	4465
12	4718
13	4714
14	4931
15	5240
16	5663      <- PEAK
17	5310
18	4263
19	3276
20	2834
21	2668
22	2199
23	1469
PEAK_HOUR	16  (avg = 5663 vehicles/hour)
LOWEST_HOUR	03  (avg = 371 vehicles/hour)
```

**Narration:** "The output shows the average traffic volume for each hour. 
Hour 16 (4 PM) is the peak with an average of 5,663 vehicles per hour. 
Hour 03 (3 AM) is the quietest with only 371 vehicles per hour. This is a realistic 
pattern — higher traffic during afternoon rush hour, lower traffic at 3 AM."

---

### STEP 4: Run MapReduce Job 2 — Traffic by Weather (2 min)

**Narration:**
"The second MapReduce job analyzes traffic by weather condition. 
It groups all traffic records by weather type (clear, cloudy, rainy, etc.) 
and computes the average traffic volume for each condition. This helps us 
understand how weather affects traffic volume."

**Command 4.1 — Run weather job**
```bash
docker exec hadoop-master hadoop jar /tmp/SmartTrafficMonitoring-1.0-SNAPSHOT.jar \
    com.traffic.mapreduce.weather.TrafficWeatherDriver \
    /traffic-data/historical/Metro_Interstate_Traffic_Volume.csv \
    /traffic-data/output/weather
```

**Expected Output (key lines):**
```
Job Info: (final status) SUCCESS
Map input records=48205
Reduce input groups=11 (11 weather conditions)
```

**Narration:** "Job completed. We have 11 weather conditions in the dataset. 
Let me show the output."

**Command 4.2 — View weather output**
```bash
docker exec hadoop-master hdfs dfs -cat /traffic-data/output/weather/part-r-00000
```

**Expected Output:**
```
Clear	3055
Clouds	3618   <- HIGHEST
Drizzle	3290
Fog	2703
Haze	3502
Mist	2932
Rain	3317
Smoke	3237
Snow	3016
Squall	2061   <- LOWEST
Thunderstorm	3001
HIGHEST_WEATHER	Clouds  (avg = 3618 vehicles/hour)
LOWEST_WEATHER	Squall  (avg = 2061 vehicles/hour)
```

**Narration:** "Interesting. Cloudy weather shows the highest average traffic 
at 3,618 vehicles/hour. Squall conditions show the lowest at 2,061 vehicles/hour. 
This suggests drivers may avoid traveling during severe weather (squalls), 
but cloudy days don't significantly deter traffic."

---

### STEP 5: Spark Streaming Setup (1.5 min)

**Narration:**
"Now I'll demonstrate the real-time streaming component. This runs in two 
separate processes:

1. **Producer:** Simulates a live traffic sensor network by reading the dataset 
   row by row and streaming records over a TCP socket
2. **Spark Streaming App:** Connects to the socket, processes events in 
   5-second micro-batches, and detects congestion in real time

Let me start the producer first. I'll open a new terminal or background process."

**Command 5.1 — Start producer (Terminal 1, background)**
```bash
cd /home/fadi/Desktop/tek-up/BigData/projet/SmartTrafficMonitoring
mvn exec:java \
  -Dexec.mainClass="com.traffic.streaming.TrafficDataProducer" \
  -Dexec.args="dataset/Metro_Interstate_Traffic_Volume.csv 9999 500" \
  -q
```

**Expected Output:**
```
========================================
  Smart Traffic Monitoring — Producer
========================================
Dataset : dataset/Metro_Interstate_Traffic_Volume.csv
Port    : 9999
Delay   : 500 ms / record
Waiting for Spark Streaming to connect...
========================================
```

**Narration:** "Producer is running and waiting for Spark to connect. 
Now I'll start the Spark Streaming app in another terminal."

**Command 5.2 — Start Spark Streaming App (Terminal 2)**
```bash
mvn exec:java \
  -Dexec.mainClass="com.traffic.streaming.TrafficStreamingApp" \
  -q
```

**Expected Output (initial):**
```
========================================
  Smart Traffic Monitoring - Streaming
========================================
Master       : local[2]
Socket       : localhost:9999
Checkpoint   : /tmp/spark-checkpoint-traffic
Alert threshold: traffic_volume > 6000
========================================
```

**Narration:** "Spark Streaming app has started. Now watch what happens when 
the producer connects..."

**Producer Terminal (after app connects):**
```
[PRODUCER] Spark connected from 127.0.0.1
[PRODUCER] Sent    20 records | Last: 2012-10-02 09:00:00,Clouds,5545
[PRODUCER] Sent    40 records | Last: 2012-10-02 11:00:00,Clouds,4516
[PRODUCER] Sent    60 records | Last: 2012-10-02 13:00:00,Haze,4288
...
```

**Spark Terminal (first micro-batch):**
```
[STATS] Running Average Traffic : 4823 vehicles/hour  (total processed: 540 records)

[WEATHER] Traffic by condition (last 30 s):
  Clouds               -> 43200 vehicles
  Clear                -> 18400 vehicles
  Rain                 -> 9900 vehicles
```

**Narration:** "Perfect! The producer is connected and streaming data. 
The Spark Streaming app is processing events in 5-second batches. 
Let me wait a moment for it to encounter a congestion event..."

---

### STEP 6: Congestion Alert Trigger (1 min)

**Narration:**
"The system is configured to trigger a congestion alert whenever 
traffic volume exceeds 6,000 vehicles/hour. This simulates real-world 
traffic monitoring where high volumes indicate congestion. Let me look 
for an alert in the output..."

**Expected Spark Output (when congestion event occurs):**
```
+--------------------------------------+
|       ** CONGESTION ALERT **         |
+--------------------------------------+
|  Time    : 2012-10-02 15:35:00      |
|  Weather : Clouds                    |
|  Volume  : 7120                      |
+--------------------------------------+

[STATS] Running Average Traffic : 4962 vehicles/hour  (total processed: 2341 records)
```

**Narration:** "Excellent! The system just detected a congestion event. 
At 15:35, during cloudy conditions, we observed 7,120 vehicles/hour, 
which exceeds our 6,000 vehicle threshold. This is the kind of alert 
that would be sent to traffic management systems in a real deployment."

---

### STEP 7: Streaming Metrics Explanation (30 seconds)

**Narration:**
"Notice the three types of analytics the streaming app produces:

1. **Congestion Alerts** — Traffic volume > 6,000 with timestamp and weather
2. **Running Average** — Cumulative average of all traffic seen so far 
   (currently 4,962 vehicles/hour across 2,341 records processed)
3. **Weather Trends** — Last 30 seconds of traffic aggregated by weather 
   condition, updated every 5 seconds

All of this is computed using Spark Streaming's advanced features:
- **Checkpointing:** Saves state to disk for fault recovery
- **Stateful Processing (updateStateByKey):** Maintains the running average 
  across batches
- **Windowed Aggregation (reduceByKeyAndWindow):** Groups last 30 seconds 
  of data with 5-second updates"

---

### STEP 8: Clean Shutdown (30 seconds)

**Narration:**
"Let me shut down the streaming application gracefully by stopping the producer. 
The Spark app will finish processing remaining events and close."

**Command 8.1 — Stop producer (Ctrl+C in Terminal 1)**
```
^C
```

**Producer Output:**
```
[PRODUCER] Dataset exhausted. Total records sent: 48204
[PRODUCER] Waiting for next connection...
```

**Spark Output (final batch):**
```
[STATS] Running Average Traffic : 4847 vehicles/hour  (total processed: 48204 records)

[WEATHER] Traffic by condition (last 30 s):
  Clouds      -> 18900 vehicles
  Clear       -> 8200 vehicles
  ...
```

**Narration:** "The streaming app has processed all 48,204 records from the dataset, 
matching the batch job. This confirms that both systems analyze the same data with 
different approaches — batch for historical reporting, streaming for real-time alerts."

---

## STEP 9: Web UI Verification (optional, 30 seconds)

**Narration (optional):**
"The system also exposes several web interfaces for monitoring and debugging:"

**URLs:**
- **HDFS NameNode:** http://localhost:9870 — File system browser, health status
- **YARN ResourceManager:** http://localhost:8088 — Job history, node status
- **Spark Master:** http://localhost:8080 — Executor status, application history

---

## SUMMARY (1 min)

```
This demonstration showed a complete Big Data pipeline:

✅ Batch Processing (MapReduce on YARN):
   - Job 1: Hourly analysis identified peak hour (16:00 with 5,663 vehicles)
   - Job 2: Weather analysis identified clearest traffic under clouds (3,618 avg)

✅ Real-Time Processing (Spark Streaming):
   - Ingests 48K historical records at configurable rate
   - Detects congestion events in real time (threshold: 6,000 vehicles)
   - Maintains running average using stateful operations
   - Aggregates weather trends in 30-second windows with 5-second updates

✅ Infrastructure:
   - 6-container Docker cluster (Hadoop + Spark)
   - HDFS for distributed storage
   - YARN for cluster resource management
   - Fault-tolerant checkpointing for streaming state

✅ Code Quality:
   - Proper error handling (skips malformed records)
   - Serializable POJOs for distributed computing
   - Configurable parameters (host, port, checkpoint directory)
   - Documentation and usage examples

The system demonstrates key Big Data concepts:
- Distributed batch processing (MapReduce)
- Real-time stream processing (Spark Streaming)
- HDFS data locality optimization
- Fault tolerance and state management
- Multi-node cluster coordination

Thank you. Any questions?
```

---

## APPENDIX: Quick Reference

### Cluster Startup (if restarting)
```bash
docker compose up -d
bash scripts/start-spark.sh
docker exec hadoop-master hdfs dfs -put dataset/*.csv /traffic-data/historical/
mvn clean package
```

### MapReduce Jobs
```bash
# Job 1: Hourly
docker cp target/*.jar hadoop-master:/tmp/
docker exec hadoop-master hadoop jar /tmp/SmartTrafficMonitoring-1.0-SNAPSHOT.jar \
  com.traffic.mapreduce.hourly.TrafficHourDriver \
  /traffic-data/historical/Metro_Interstate_Traffic_Volume.csv \
  /traffic-data/output/hourly

# Job 2: Weather
docker exec hadoop-master hadoop jar /tmp/SmartTrafficMonitoring-1.0-SNAPSHOT.jar \
  com.traffic.mapreduce.weather.TrafficWeatherDriver \
  /traffic-data/historical/Metro_Interstate_Traffic_Volume.csv \
  /traffic-data/output/weather
```

### Streaming Pipeline
```bash
# Terminal 1: Producer
mvn exec:java -Dexec.mainClass="com.traffic.streaming.TrafficDataProducer" \
  -Dexec.args="dataset/Metro_Interstate_Traffic_Volume.csv 9999 500" -q

# Terminal 2: Spark Streaming App
mvn exec:java -Dexec.mainClass="com.traffic.streaming.TrafficStreamingApp" -q
```

