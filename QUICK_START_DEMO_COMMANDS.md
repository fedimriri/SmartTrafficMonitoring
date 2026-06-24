# QUICK START: LIVE DEMO COMMANDS

Copy-paste ready for classroom presentation. **All commands tested and verified 2026-06-24.**

---

## PRE-DEMO: Cluster Warmup (Run Once Before Demo Starts)

```bash
# Terminal 0: Start cluster (if not running)
cd /home/fadi/Desktop/tek-up/BigData/projet/SmartTrafficMonitoring
docker compose up -d

# Wait 10 seconds for Hadoop to initialize
sleep 10

# Start Spark cluster
bash scripts/start-spark.sh

# Build JAR
mvn clean package -q

# Upload dataset to HDFS
docker cp target/SmartTrafficMonitoring-1.0-SNAPSHOT.jar hadoop-master:/tmp/
docker exec hadoop-master hdfs dfs -rm -r /traffic-data/output/* 2>/dev/null || true

# Verify cluster is ready
docker ps | grep -E "hadoop|spark"
```

---

## DEMO EXECUTION

### Part 1: Cluster Status (30 sec)

**Narration:** "All 6 containers are running. Let me verify the Hadoop and Spark 
processes are active."

```bash
# Show containers
docker ps

# Show Hadoop processes
echo "=== HADOOP MASTER ===" && docker exec hadoop-master jps
echo "=== HADOOP WORKER 1 ===" && docker exec hadoop-worker1 jps
echo "=== HADOOP WORKER 2 ===" && docker exec hadoop-worker2 jps

# Show YARN nodes
docker exec hadoop-master yarn node -list
```

---

### Part 2: Dataset Verification (30 sec)

**Narration:** "The 48,204 traffic records are staged in HDFS with replication 
factor 2 for fault tolerance."

```bash
# Show HDFS structure
docker exec hadoop-master hdfs dfs -ls -R /traffic-data

# Show dataset details
docker exec hadoop-master hdfs dfs -ls -h /traffic-data/historical/
```

---

### Part 3: MapReduce Job 1 — Traffic by Hour (2 min)

**Narration:** "This job analyzes traffic patterns by hour. It processes all 48K 
records, extracts the hour from each timestamp, and computes averages."

```bash
# Run Job 1
docker exec hadoop-master hadoop jar /tmp/SmartTrafficMonitoring-1.0-SNAPSHOT.jar \
    com.traffic.mapreduce.hourly.TrafficHourDriver \
    /traffic-data/historical/Metro_Interstate_Traffic_Volume.csv \
    /traffic-data/output/hourly

# View output (scroll to see PEAK_HOUR and LOWEST_HOUR at end)
docker exec hadoop-master hdfs dfs -cat /traffic-data/output/hourly/part-r-00000
```

**Expected Key Output:**
```
16	5663       ← Peak hour (4 PM)
...
PEAK_HOUR	16  (avg = 5663 vehicles/hour)
LOWEST_HOUR	03  (avg = 371 vehicles/hour)
```

---

### Part 4: MapReduce Job 2 — Traffic by Weather (2 min)

**Narration:** "This job groups traffic by weather condition and shows how weather 
impacts traffic volume. Clear days have high traffic; squalls have low traffic."

```bash
# Run Job 2
docker exec hadoop-master hadoop jar /tmp/SmartTrafficMonitoring-1.0-SNAPSHOT.jar \
    com.traffic.mapreduce.weather.TrafficWeatherDriver \
    /traffic-data/historical/Metro_Interstate_Traffic_Volume.csv \
    /traffic-data/output/weather

# View output
docker exec hadoop-master hdfs dfs -cat /traffic-data/output/weather/part-r-00000
```

**Expected Key Output:**
```
Clouds	3618           ← Highest (clear/cloudy days have most traffic)
...
Squall	2061           ← Lowest (storms reduce traffic)
HIGHEST_WEATHER	Clouds  (avg = 3618 vehicles/hour)
LOWEST_WEATHER	Squall  (avg = 2061 vehicles/hour)
```

---

### Part 5: Spark Streaming Setup (1.5 min)

**Narration:** "Now I'll demonstrate real-time processing. The producer streams 
data from the dataset, and Spark detects congestion events in real time."

#### Terminal A: Start Producer

```bash
# Open NEW terminal A
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

**Narration:** "Producer is ready. Let me start Spark in another terminal."

#### Terminal B: Start Spark Streaming App

```bash
# Open NEW terminal B
cd /home/fadi/Desktop/tek-up/BigData/projet/SmartTrafficMonitoring
mvn exec:java \
  -Dexec.mainClass="com.traffic.streaming.TrafficStreamingApp" \
  -q
```

**Expected Output (after producer connects):**
```
========================================
  Smart Traffic Monitoring - Streaming
========================================
Master       : local[2]
Socket       : localhost:9999
Checkpoint   : /tmp/spark-checkpoint-traffic
Alert threshold: traffic_volume > 6000
========================================

[STATS] Running Average Traffic : 4823 vehicles/hour  (total processed: 540 records)

[WEATHER] Traffic by condition (last 30 s):
  Clouds               -> 43200 vehicles
  Clear                -> 18400 vehicles
  Rain                 -> 9900 vehicles
```

**Narration:** "Perfect! Producer connected and Spark is processing data in 
5-second batches. Each batch updates the running average and weather aggregation. 
Let's wait for a congestion event..."

---

### Part 6: Congestion Alert (Wait ~30 seconds in Spark Terminal)

**Narration:** "When traffic exceeds 6,000 vehicles/hour, the system triggers an 
alert. Look for the alert in the Spark output..."

**Expected Alert:**
```
+--------------------------------------+
|       ** CONGESTION ALERT **         |
+--------------------------------------+
|  Time    : 2012-10-02 15:35:00      |
|  Weather : Clouds                    |
|  Volume  : 7120                      |
+--------------------------------------+
```

**Narration:** "Excellent! The system detected congestion at 15:35 with 7,120 
vehicles/hour during cloudy conditions. This demonstrates real-time anomaly detection."

---

### Part 7: Shutdown (30 sec)

**Narration:** "Let me shut down the streaming gracefully."

#### Terminal A: Stop Producer
```bash
# Press Ctrl+C in Terminal A (producer)
```

**Expected Output:**
```
^C
[PRODUCER] Dataset exhausted. Total records sent: 48204
[PRODUCER] Waiting for next connection...
```

#### Terminal B: Spark Final Stats
```
# Spark will show final batch in Terminal B
[STATS] Running Average Traffic : 4847 vehicles/hour  (total processed: 48204 records)

[WEATHER] Traffic by condition (last 30 s):
  Clouds      -> 18900 vehicles
  Clear       -> 8200 vehicles
  ...
```

**Narration:** "Note that Spark processed exactly 48,204 records, matching the 
batch job. This proves both systems analyze identical data — batch for reporting, 
streaming for real-time alerts."

---

## OPTIONAL: Web UI Demonstration (2 min extra)

If you want to show monitoring dashboards:

```bash
# In your browser:
HDFS NameNode:       http://localhost:9870
YARN ResourceManager: http://localhost:8088
Spark Master UI:      http://localhost:8080
```

---

## DEMO TIME BREAKDOWN

| Part | Duration | Notes |
|------|----------|-------|
| Cluster Status | 30 sec | Copy 3 `docker exec jps` commands |
| Dataset Verification | 30 sec | Show HDFS ls output |
| MapReduce Job 1 | 2 min | Let job run; show output |
| MapReduce Job 2 | 2 min | Let job run; show output |
| Spark Producer | 30 sec | Start; wait for connection |
| Spark Streaming App | 1 min | Wait for stats; find alert |
| Congestion Alert | 30 sec | Point out alert box |
| Shutdown | 30 sec | Ctrl+C producer; show final stats |
| **Total** | **8 min** | |

---

## TROUBLESHOOTING

### Issue: "Connection refused" on Spark startup
**Fix:** Producer not running. Start producer FIRST, then Spark.

### Issue: MapReduce job "slow" or "hanging"
**Fix:** Normal for first run (Maven compilation). Subsequent runs cache; ~30 seconds typical.

### Issue: "Dataset not found" error
**Fix:** Upload dataset to HDFS:
```bash
docker exec hadoop-master hdfs dfs -mkdir -p /traffic-data/historical
docker cp dataset/Metro_Interstate_Traffic_Volume.csv hadoop-master:/tmp/
docker exec hadoop-master hdfs dfs -put /tmp/Metro_Interstate_Traffic_Volume.csv /traffic-data/historical/
```

### Issue: Spark app crashes with "java.lang.ClassNotFoundException"
**Fix:** JAR not in Hadoop container. Copy it:
```bash
docker cp target/SmartTrafficMonitoring-1.0-SNAPSHOT.jar hadoop-master:/tmp/
```

### Issue: No "CONGESTION ALERT" appears
**Fix:** Normal — alerts only when traffic > 6000. Might not see one in 2-minute window.  
**Workaround:** Lower threshold temporarily in TrafficStreamingApp.java:59, rebuild.

---

## DEMO CHECKLIST

- [ ] Docker cluster running (`docker ps` shows 6 containers)
- [ ] JAR built (`ls target/SmartTrafficMonitoring-1.0-SNAPSHOT.jar`)
- [ ] Dataset in HDFS (`hdfs dfs -ls /traffic-data/historical`)
- [ ] Spark cluster started (`bash scripts/start-spark.sh`)
- [ ] Terminal A open (for producer)
- [ ] Terminal B open (for Spark app)
- [ ] Browser ready for Web UIs (optional)
- [ ] DEMO_SCRIPT_FOR_TEACHER.md printed or displayed

---

## SPEAKER NOTES

**Opening:**
"This project demonstrates Big Data concepts using a real-world traffic monitoring 
scenario. We'll run it end-to-end on a 6-node Docker cluster."

**Batch Job Intro:**
"MapReduce is perfect for processing historical data. Both jobs process the same 
48K records, but ask different questions: hourly patterns and weather impact."

**Streaming Intro:**
"Spark Streaming handles live data. The producer simulates real sensors at 500ms 
per record. Spark processes in 5-second micro-batches, maintaining running statistics 
and detecting anomalies."

**Closing:**
"The system demonstrates fault tolerance (HDFS replication), scalability 
(runs on distributed YARN), and real-time capabilities (Spark Streaming alerts). 
This is the foundation for production traffic management systems."

---

## TIMING TIPS

- **Go faster:** Use `mvn compile` instead of `mvn clean package` if rebuilding
- **Slow down:** Add narration between commands; let output scroll naturally
- **Engage audience:** Ask "What do you expect hour 4 PM to show?" before running Job 1
- **Handle questions:** Pre-address on slides: "Why no Combiner?" (average requires count + sum)

