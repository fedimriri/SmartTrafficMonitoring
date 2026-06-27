# Live Defense Demo Guide — Smart Traffic Monitoring

> This guide is designed for a **live defense** or live demonstration.  
> Follow it top to bottom. Every command is exact. Every expected output is shown.

---

## Pre-Defense Checklist

Run these checks **before** the evaluators arrive:

```bash
# 1. All 6 containers are running
docker ps --format "table {{.Names}}\t{{.Status}}"

# 2. Hadoop daemons
docker exec hadoop-master jps    # NameNode, SecondaryNameNode, ResourceManager
docker exec hadoop-worker1 jps   # DataNode, NodeManager

# 3. Dataset exists in HDFS
docker exec hadoop-master hdfs dfs -ls /traffic-data/historical/

# 4. Spark cluster is running
docker exec spark-master jps     # Master
docker exec spark-slave1 jps     # Worker
docker exec spark-slave2 jps     # Worker

# 5. JAR is built
ls -lh target/SmartTrafficMonitoring-1.0-SNAPSHOT.jar
```

If anything is missing, use the **Quick Recovery** steps at the bottom of this guide.

---

## Demo Sequence

### Phase 1 — Introduce the System (2 min)

**Say:**
> "This project implements a two-layer Big Data pipeline for monitoring metropolitan traffic on Interstate 94. The batch layer uses Hadoop MapReduce running on YARN to compute historical averages stored in HDFS. The streaming layer uses Apache Spark Streaming to process live traffic events in real time and detect congestion."

Open http://localhost:9870 to show the HDFS NameNode UI.
Open http://localhost:8088 to show the YARN ResourceManager.
Open http://localhost:8080 to show the Spark Master (if cluster is already running).

---

### Phase 2 — Show the Dataset (1 min)

```bash
# Show the first 5 rows of the dataset
head -5 dataset/Metro_Interstate_Traffic_Volume.csv
```

Expected:
```
holiday,temp,rain_1h,snow_1h,clouds_all,weather_main,weather_description,date_time,traffic_volume
None,288.28,0.0,0.0,40,Clouds,scattered clouds,2012-10-02 09:00:00,5545
None,289.36,0.0,0.0,75,Clouds,broken clouds,2012-10-02 10:00:00,4516
None,289.58,0.0,0.0,90,Clouds,overcast clouds,2012-10-02 11:00:00,4767
None,290.13,0.0,0.0,90,Clouds,overcast clouds,2012-10-02 12:00:00,5026
```

**Say:**
> "The dataset contains 48,204 hourly records. The fields we use are: `weather_main` at index 5, `date_time` at index 7, and `traffic_volume` at index 8."

```bash
# Show dataset size in HDFS
docker exec hadoop-master hdfs dfs -du -h /traffic-data/historical/
```

---

### Phase 3 — MapReduce Job 1: Traffic by Hour (3 min)

**Say:**
> "The first MapReduce job computes the average traffic volume for each hour of the day and identifies the peak hour and the lowest hour."

```bash
docker exec hadoop-master hdfs dfs -rm -r /traffic-data/output/hourly 2>/dev/null || true

docker exec hadoop-master hadoop jar /tmp/SmartTrafficMonitoring.jar \
    com.traffic.mapreduce.hourly.TrafficHourDriver \
    /traffic-data/historical/Metro_Interstate_Traffic_Volume.csv \
    /traffic-data/output/hourly
```

While the job runs, open http://localhost:8088 and show the application running.

After completion:

```bash
docker exec hadoop-master hdfs dfs -cat /traffic-data/output/hourly/part-r-00000
```

Expected output:
```
00      834
01      516
02      370
03      371
...
16      5663
...
23      1469
PEAK_HOUR       16  (avg = 5663 vehicles/hour)
LOWEST_HOUR     03  (avg = 371 vehicles/hour)
```

**Say:**
> "The Mapper extracts the hour from the date_time field and emits the pair (hour, volume). During Shuffle and Sort, Hadoop groups all volumes by hour. The Reducer computes the average per hour and, in the cleanup phase, identifies the peak hour which is 16:00 — the evening rush hour — and the lowest at 03:00."

---

### Phase 4 — MapReduce Job 2: Traffic by Weather (2 min)

**Say:**
> "The second MapReduce job computes the average traffic volume for each weather condition."

```bash
docker exec hadoop-master hdfs dfs -rm -r /traffic-data/output/weather 2>/dev/null || true

docker exec hadoop-master hadoop jar /tmp/SmartTrafficMonitoring.jar \
    com.traffic.mapreduce.weather.TrafficWeatherDriver \
    /traffic-data/historical/Metro_Interstate_Traffic_Volume.csv \
    /traffic-data/output/weather
```

```bash
docker exec hadoop-master hdfs dfs -cat /traffic-data/output/weather/part-r-00000
```

Expected output:
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

**Say:**
> "The Mapper emits (weather_main, volume). The Reducer computes the average per condition. Cloudy conditions have the highest traffic volume — cloud cover correlates with peak commuting hours. Squalls have the lowest, as drivers avoid the road in severe weather."

Show YARN history:

```bash
docker exec hadoop-master yarn application -list -appStates ALL
```

---

### Phase 5 — Spark Streaming Demo (5 min)

There are two modes. Choose **Manual Mode** for the most impactful live demonstration.

---

#### Option A — Manual Demo (Recommended for Defense)

**Say:**
> "Now I'll demonstrate the Spark Streaming pipeline. I'll type traffic records manually so you can see each event being processed in real time."

**Open two terminals side by side.**

**Terminal 1** — Start the Spark Streaming application:
```bash
bash scripts/start-streaming.sh
```

Wait for output like:
```
=== Submitting TrafficStreamingApp to spark://spark-master:7077 ===
```
Then wait ~10 seconds for the streaming context to initialize.

**Terminal 2** — Start the interactive producer:
```bash
bash scripts/start-manual-demo.sh
```

This starts `TrafficDataProducer --interactive` inside `spark-master` via `docker exec -it`. No `nc` or netcat is needed. Wait until Terminal 2 shows:
```
  Waiting for Spark Streaming to connect...
```

And Terminal 1 shows the streaming context is running. Now type records in Terminal 2:

**Type this (congestion alert):**
```
2026-01-01 08:00:00,Rain,8500
```

Watch Terminal 1. Within 5 seconds:
```
+--------------------------------------+
|       ** CONGESTION ALERT **         |
+--------------------------------------+
|  Time    : 2026-01-01 08:00:00      |
|  Weather : Rain                      |
|  Volume  : 8500                      |
+--------------------------------------+

[STATS] Running Average Traffic : 8500 vehicles/hour  (total processed: 1 records)
```

**Type this (normal traffic):**
```
2026-01-01 09:00:00,Clear,4200
```

Terminal 1 shows:
```
[STATS] Running Average Traffic : 6350 vehicles/hour  (total processed: 2 records)

[WEATHER] Traffic by condition (last 30 s):
  Rain               -> 8500 vehicles
  Clear              -> 4200 vehicles
```

**Type more records:**
```
2026-01-01 10:00:00,Clouds,6100
2026-01-01 11:00:00,Snow,3800
2026-01-01 12:00:00,Fog,7200
```

**Say during typing:**
> "Each line I type is a simulated sensor reading. Spark picks it up in the next 5-second micro-batch. Notice the congestion alert fires when volume exceeds 6000. The running average updates across all batches because we use `updateStateByKey`. The weather aggregation uses a 30-second window with `reduceByKeyAndWindow`."

---

#### Option B — Automated Mode

**Terminal 1 — Start Spark Streaming:**
```bash
bash scripts/start-streaming.sh
```

**Terminal 2 — Start the producer:**
```bash
bash scripts/start-producer.sh
```

Monitor the producer:
```bash
docker exec spark-master tail -f /tmp/producer.log
```

Results appear in Terminal 1 every 5 seconds. The producer sends ~2 records/second from the 48,204-record dataset.

---

### Phase 6 — Spark Web UI (1 min)

Open http://localhost:8080

**Say:**
> "The Spark Master shows one running application — our StreamingApp — and two workers: spark-slave1 and spark-slave2. The application currently has two executor cores allocated."

Point to the Workers table and explain that the ReceiverTask (socket reader) runs on one executor while processing tasks are distributed across both workers.

---

## Common Evaluator Questions and Answers

**Q: What is the difference between Hadoop MapReduce and Spark Streaming in this project?**

> MapReduce runs a batch job on the entire historical dataset (48,204 records) and produces a static result stored in HDFS. It answers questions like "what was the average volume at 16:00 over 5 years?" Spark Streaming processes each new event in real time (5-second micro-batches) and answers "is traffic congested right now?" and "what is the running average so far?"

**Q: Is this a Lambda Architecture?**

> No. Lambda Architecture requires the speed layer to process the same events as the batch layer and serve them jointly from a serving layer. In this project, the two pipelines are independent: Hadoop reads from HDFS, Spark reads from a TCP socket. They answer different questions about the same underlying dataset.

**Q: Why does Spark read the original CSV instead of Hadoop's output?**

> The streaming layer simulates a live sensor feed by replaying the historical dataset. Its purpose is real-time monitoring, not re-analysing batch results. Hadoop's output (average per hour, average per weather) would not be useful input for a streaming congestion detector that needs individual events.

**Q: What is `updateStateByKey` and why is it needed?**

> `updateStateByKey` is a Spark Streaming operator that maintains a persistent key-value state across micro-batches. We use it to accumulate a running sum and count across all batches, which lets us compute the true cumulative average rather than just a per-batch average. It requires a checkpoint directory to survive driver restarts.

**Q: What is `reduceByKeyAndWindow`?**

> It is a windowed aggregation. With a 30-second window and 5-second slide, it sums traffic volumes by weather condition over the last 30 seconds, recomputing every 5 seconds. This shows which weather conditions are generating the most traffic right now, based on a rolling 30-second observation.

**Q: What is checkpointing and why is it required?**

> Checkpointing periodically saves the streaming context's state (DStream graph + stateful operator state) to a reliable storage location. It is required by `updateStateByKey` because that operator's state grows across batches and must survive failures. Without checkpointing, a driver crash would lose all accumulated state.

**Q: How does the socket receiver work?**

> `TrafficDataProducer` acts as a TCP server (ServerSocket on port 9999) inside `spark-master`. Spark's socket receiver runs as a task on one of the executors (spark-slave1 or spark-slave2). That executor connects to `spark-master:9999` as a client and buffers incoming lines. Every 5 seconds, the StreamingContext creates an RDD from the buffered data and submits processing tasks to both workers.

**Q: Why do the MapReduce jobs not use a Combiner?**

> Because the Reducer computes an average. A Combiner would need to aggregate partial results from multiple Mappers on the same node. If the Combiner ran `partial_sum / partial_count`, it would produce incorrect averages when the Reducer combined results from multiple Combiners. To use a Combiner correctly, the Mapper would need to emit `(key, "sum:count")` strings, which would complicate the design for marginal gain on a 5 MB dataset.

**Q: What congestion threshold is used and why?**

> The threshold is 6000 vehicles/hour, as specified in the project requirements. In real-world traffic engineering, Level of Service (LOS) degrades significantly around this volume on a typical highway, and the dataset contains frequent readings above this level during peak hours.

---

## Verification Steps

### Verify HDFS is healthy

```bash
docker exec hadoop-master hdfs dfsadmin -report
# Look for: Live datanodes (2), Configured Capacity
```

### Verify Spark workers are connected

```bash
docker exec spark-master jps   # Master
docker exec spark-slave1 jps   # Worker
docker exec spark-slave2 jps   # Worker
```

Open http://localhost:8080 → "Workers" section should show 2 workers.

### Verify HDFS output after MapReduce

```bash
docker exec hadoop-master hdfs dfs -ls -R /traffic-data/output/
# Should show: /output/hourly/part-r-00000 and /output/weather/part-r-00000
```

### Verify producer is sending data

```bash
docker exec spark-master tail -20 /tmp/producer.log
# Should show: [PRODUCER] Sent XXXXX records | Last: timestamp,weather,volume
```

---

## Quick Recovery

### Containers stopped / crashed

```bash
docker-compose down
docker-compose up -d
# Wait 15 seconds for HDFS/YARN to initialize
```

### Re-upload dataset to HDFS

```bash
docker cp dataset/Metro_Interstate_Traffic_Volume.csv hadoop-master:/tmp/
docker exec hadoop-master hdfs dfs -mkdir -p /traffic-data/historical/
docker exec hadoop-master hdfs dfs -put -f \
    /tmp/Metro_Interstate_Traffic_Volume.csv \
    /traffic-data/historical/
```

### Restart Spark cluster

```bash
bash scripts/start-spark.sh
```

### Rebuild JAR

```bash
mvn clean package -DskipTests
```

### Kill stuck producer

```bash
docker exec spark-master bash -c "fuser -k 9999/tcp" 2>/dev/null || true
```

### Clear streaming checkpoint (if Spark refuses to start after crash)

```bash
docker exec spark-master rm -rf /tmp/spark-checkpoint-traffic
```

---

## Common Mistakes to Avoid

| Mistake | Fix |
|---|---|
| Starting Spark Streaming **before** the producer | Producer is the TCP server — start it first (automatic: `start-producer.sh`; manual: `start-manual-demo.sh`) |
| Running MapReduce when the output directory already exists | Script handles this; if manual: `hdfs dfs -rm -r /traffic-data/output/hourly` |
| Typing records without the correct format | Must be exactly `timestamp,weather,volume` — three comma-separated fields |
| Spark app shows no output | Wait at least 10 seconds; check producer for "[CONNECTED] Spark connected" |
| Images not found locally | Run `docker compose pull` to download all images from Docker Hub |
| Stale Spark PID files after restart | Run `bash scripts/start-spark.sh` — it clears them automatically |
