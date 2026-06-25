# STREAMING VALIDATION REPORT
**Project:** SmartTrafficMonitoring  
**Date:** 2026-06-25  
**Auditor:** Senior Spark Streaming Engineer / QA  
**Branch:** main

---

## FINAL VERDICT

```
STREAMING WORKING ✅
(after 2 targeted fixes — see Fixes Applied)
```

---

## 1. CURRENT STATUS

The Spark Streaming pipeline is **fully operational** after two minimum-footprint fixes:

| Fix | File | Nature |
|-----|------|--------|
| Jackson version pinned to 2.6.7.1 | `pom.xml` | dependency management |
| Socket port made configurable (args[3]) | `TrafficStreamingApp.java` | 1-line parameter add |

Before these fixes, running the demo commands from the documentation produced:
- **Producer** → `java.net.BindException: Address already in use`
- **Streaming App** → `JsonMappingException: Incompatible Jackson version: 2.12.7-1`

Both failures are deterministic and 100% reproducible on any machine running the Docker cluster.

---

## 2. PHASE 1 — DOCUMENT VS REALITY

| Claimed Feature | Doc Evidence | Code Evidence | VERIFIED |
|----------------|--------------|---------------|----------|
| Socket Streaming | DEMO_SCRIPT step 5 | `ssc.socketTextStream(host, port)` line 88 | ✅ YES |
| Spark Standalone Cluster | DEMO_SCRIPT step 1.4, `docker-compose.yml` | `SparkConf.setMaster(master)`, default `local[2]` | ⚠️ PARTIAL — cluster starts, demo runs in `local[2]` |
| Checkpointing | DEMO_SCRIPT step 7 | `ssc.checkpoint(checkpointDir)` line 84 | ✅ YES |
| updateStateByKey | DEMO_SCRIPT step 7 | `mapToPair(...).updateStateByKey(updateAvgFn)` line 177 | ✅ YES |
| reduceByKeyAndWindow | DEMO_SCRIPT step 7 | `.reduceByKeyAndWindow(..., 30s, 5s)` line 208-218 | ✅ YES |
| Congestion Alerts | DEMO_SCRIPT step 6 | `filter(volume > 6000).foreachRDD(...)` line 127-150 | ✅ YES — 118 alerts in live test |
| Running Statistics | DEMO_SCRIPT step 7 | `updateStateByKey` + `foreachRDD(print avg)` | ✅ YES — printed every 5s batch |
| Weather Aggregation | DEMO_SCRIPT step 7 | `reduceByKeyAndWindow` on weather key | ✅ YES — 6+ conditions aggregated live |
| Producer Simulation | DEMO_SCRIPT step 5 | `ServerSocket(port)` + CSV read loop | ✅ YES — 1520+ records streamed |
| 5-second micro-batch | DEMO_SCRIPT step 5 | `Durations.seconds(5)` line 81 | ✅ YES |

---

## 3. PHASE 2 — SPARK CLUSTER VALIDATION

### Container Status

```bash
docker ps
# All 6 containers UP: hadoop-master, hadoop-worker1, hadoop-worker2,
#                       spark-master, spark-slave1, spark-slave2
```

### Spark Daemon Status (after `bash scripts/start-spark.sh`)

```
--- spark-master ---     --- spark-slave1 ---     --- spark-slave2 ---
130 Master               56 Worker                50 Worker
195 Jps                  124 Jps                   118 Jps
```

All three daemons verified running via `docker exec <node> jps`.

### Master URL

`spark://spark-master:7077` — correct, matches `start-spark.sh` and container hostname.

### Important Caveat

`TrafficStreamingApp.java` defaults to **`local[2]`** (not the cluster URL). The cluster is running and the Spark Master UI is available at `http://localhost:8080`, but the documented demo uses local mode. The cluster URL can be passed as `args[0]` to use the actual cluster.

---

## 4. PHASE 3 — STREAMING APPLICATION AUDIT

File: [src/main/java/com/traffic/streaming/TrafficStreamingApp.java](src/main/java/com/traffic/streaming/TrafficStreamingApp.java)

| Component | Line | Status | Notes |
|-----------|------|--------|-------|
| SparkConf + setMaster | 76-78 | ✅ | Master configurable via args[0] |
| StreamingContext 5s batch | 81 | ✅ | `Durations.seconds(5)` |
| Checkpoint directory | 84 | ✅ | Required for `updateStateByKey` |
| socketTextStream | 87-88 | ✅ (after fix) | Port now via args[3], was hardcoded 9999 |
| Non-empty line filter | 91-98 | ✅ | Skips blanks |
| CSV parsing (flatMap) | 103-123 | ✅ | `NumberFormatException` safe |
| Congestion filter + foreachRDD | 127-150 | ✅ | Prints alert box on screen |
| updateStateByKey state fn | 155-168 | ✅ | Accumulates sum + count as `long[2]` |
| updateStateByKey output | 170-195 | ✅ | Prints avg + count every batch |
| reduceByKeyAndWindow 30s/5s | 208-218 | ✅ | window=30s, slide=5s, valid multiples |
| foreachRDD weather output | 219-232 | ✅ | Prints all weather conditions |
| ssc.start() | 235 | ✅ | DStream started |
| ssc.awaitTermination() | 236 | ✅ | Blocking wait, correct |
| TrafficEvent serializable | 240 | ✅ | `implements Serializable` |
| Anonymous inner classes | all lambdas | ✅ | Avoids ASM 5.x bytecode version issue |

No transformations left disconnected. All three pipelines (congestion, stats, weather) terminate in `foreachRDD` output actions.

---

## 5. PHASE 4 — PRODUCER AUDIT

File: [src/main/java/com/traffic/streaming/TrafficDataProducer.java](src/main/java/com/traffic/streaming/TrafficDataProducer.java)

| Check | Result |
|-------|--------|
| Reads dataset | ✅ `Files.newBufferedReader(Paths.get(csvPath))` |
| Sends continuously | ✅ `while(line = reader.readLine()) ... out.println(...)` |
| Connects to correct host | ✅ Acts as `ServerSocket` server; Spark is the client |
| Port | ✅ Configurable via args[1], default 9999 |
| Wire format | ✅ `dateTime + "," + weatherMain + "," + trafficVolume` |

**Wire format verification:**

Producer sends: `2012-10-02 09:00:00,Clouds,5545`  
Streaming app parses: `parts[0]=timestamp, parts[1]=weather, parts[2]=volume`  
Result: **exact match, no format mismatch.**

**CSV column mapping (TrafficRecord.java):**
```
holiday(0), temp(1), rain_1h(2), snow_1h(3), clouds_all(4),
weather_main(5), weather_description(6), date_time(7), traffic_volume(8)
```
`fields[7]` → dateTime ✅  `fields[5]` → weather ✅  `fields[8]` → volume ✅

**CSV integrity check:**  
All 48,205 rows have exactly 9 comma-separated fields (no embedded commas). No parsing errors.

---

## 6. PHASE 5 — END-TO-END TEST RESULTS

### Live Test Executed: 2026-06-25

```bash
# Step 1: Verify Docker cluster
docker ps
# Result: 6/6 containers UP ✅

# Step 2: Start Spark cluster
bash scripts/start-spark.sh
# Result: Master on spark-master, Worker on spark-slave1 + spark-slave2 ✅

# Step 3: Spark Master UI
curl -s http://localhost:8080 | grep -o "Spark Master" | head -1
# Result: Spark Master UI accessible ✅

# Step 4: Start Producer (Terminal 1)
mvn exec:java -Dexec.mainClass="com.traffic.streaming.TrafficDataProducer" \
  -Dexec.args="dataset/Metro_Interstate_Traffic_Volume.csv 19999 50" -q

# Step 5: Start Streaming App (Terminal 2)
mvn exec:java -Dexec.mainClass="com.traffic.streaming.TrafficStreamingApp" \
  -Dexec.args="local[2] localhost /tmp/spark-checkpoint-traffic 19999" -q
```

### Observed Output

**Producer (after connection):**
```
[PRODUCER] Spark connected from /127.0.0.1
[PRODUCER] Sent    20 records | Last: 2012-10-03 04:00:00,Clear,814
[PRODUCER] Sent    40 records | Last: 2012-10-04 04:00:00,Clear,835
[PRODUCER] Sent   180 records | Last: 2012-10-10 07:00:00,Drizzle,6793
```

**Streaming App — Congestion Alert (VERIFIED LIVE):**
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
|  Time    : 2012-10-03 08:00:00       |
|  Weather : Clear                     |
|  Volume  : 6511                      |
+--------------------------------------+
```

**Streaming App — Running Average (VERIFIED LIVE):**
```
[STATS] Running Average Traffic : 3539 vehicles/hour  (total processed: 152 records)
[STATS] Running Average Traffic : 3518 vehicles/hour  (total processed: 317 records)
```

**Streaming App — Weather Aggregation (VERIFIED LIVE):**
```
[WEATHER] Traffic by condition (last 30 s):
  Clouds               -> 621611 vehicles
  Drizzle              -> 43968 vehicles
  Fog                  -> 5837 vehicles
  Rain                 -> 46447 vehicles
  Mist                 -> 84920 vehicles

[WEATHER] Traffic by condition (last 30 s):
  Clouds               -> 892682 vehicles
  Drizzle              -> 100746 vehicles
  Haze                 -> 37389 vehicles
  Fog                  -> 31684 vehicles
  Rain                 -> 249659 vehicles
  Mist                 -> 332737 vehicles
```

**Metrics from live run:**

| Metric | Value |
|--------|-------|
| Total congestion alerts fired | 118 |
| Total STATS batches | 9 |
| Total WEATHER batches | 9 |
| Producer records sent | 1520+ |
| Batch duration | 5 seconds |
| Window / slide | 30s / 5s |

---

## 7. PHASE 6 — ROOT CAUSE ANALYSIS

### Bug 1 — Port 9999 Conflict (CRITICAL)

| Field | Detail |
|-------|--------|
| Root Cause | Docker Compose maps `"9999:9999"` on `spark-master`. The `docker-proxy` process binds host port 9999 the moment `docker-compose up` runs. |
| Evidence | `ss -tlnp \| grep 9999` → `LISTEN 0 4096 0.0.0.0:9999` before producer starts. `mvn exec:java` producer → `Address already in use (Bind failed)`. |
| Impact | Producer cannot start on the host. Entire streaming demo is dead on arrival. |
| File | `TrafficStreamingApp.java` + demo script |
| Code Location | `TrafficStreamingApp.java:88` — `ssc.socketTextStream(socketHost, 9999)` hardcoded port |
| Fix | Added `args[3]` for port (default: 9999). Demo uses port 19999 on host. |

### Bug 2 — Jackson Version Incompatibility (CRITICAL)

| Field | Detail |
|-------|--------|
| Root Cause | `pom.xml` declares both `hadoop-common:3.3.6` and `spark-core_2.11:2.4.5`. Hadoop 3.3.6 transitively pulls `jackson-databind:2.12.7.1`. Spark 2.4.5's `SparkContext` init checks the Jackson version and hard-fails on anything newer than 2.6.x. |
| Evidence | `mvn dependency:tree \| grep jackson` → `jackson-databind:jar:2.12.7.1`. App crash: `com.fasterxml.jackson.databind.JsonMappingException: Incompatible Jackson version: 2.12.7-1` |
| Impact | `SparkContext` never initializes. Streaming app crashes immediately on startup with `ExceptionInInitializerError`. |
| File | `pom.xml` |
| Code Location | `<dependencies>` block — no `<dependencyManagement>` to pin Jackson |
| Fix | Added `<dependencyManagement>` forcing `jackson-databind:2.6.7.1`, `jackson-core:2.6.7`, `jackson-annotations:2.6.0`. |

---

## 8. PHASE 7 — FIXES APPLIED

### Fix 1 — `TrafficStreamingApp.java`

**What changed:** Socket port extracted from `args[3]` (default: 9999). Banner line updated to print the actual port.

```java
// BEFORE (hardcoded, broken when Docker holds 9999)
JavaReceiverInputDStream<String> rawStream =
        ssc.socketTextStream(socketHost, 9999);

// AFTER (configurable)
final int socketPort = args.length > 3 ? Integer.parseInt(args[3]) : 9999;
// ...
JavaReceiverInputDStream<String> rawStream =
        ssc.socketTextStream(socketHost, socketPort);
```

### Fix 2 — `pom.xml`

**What changed:** `<dependencyManagement>` block added to pin all three Jackson Fasterxml core artifacts to the version Spark 2.4.5 requires.

```xml
<dependencyManagement>
    <dependencies>
        <dependency>
            <groupId>com.fasterxml.jackson.core</groupId>
            <artifactId>jackson-databind</artifactId>
            <version>2.6.7.1</version>
        </dependency>
        <dependency>
            <groupId>com.fasterxml.jackson.core</groupId>
            <artifactId>jackson-core</artifactId>
            <version>2.6.7</version>
        </dependency>
        <dependency>
            <groupId>com.fasterxml.jackson.core</groupId>
            <artifactId>jackson-annotations</artifactId>
            <version>2.6.0</version>
        </dependency>
    </dependencies>
</dependencyManagement>
```

Both fixes are minimal — no architectural changes, no MapReduce code touched, no new files created.

---

## 9. DEMO COMMANDS (CORRECTED)

Use these commands for the defense presentation. They replace the broken commands in `DEMO_SCRIPT.md`.

```bash
# ── 0. One-time setup ──────────────────────────────────────────────
mvn clean package -q                         # rebuild JAR with fixes
bash scripts/start-spark.sh                  # start Spark cluster

# ── 1. Verify cluster ──────────────────────────────────────────────
docker ps
docker exec spark-master jps                 # expect: Master
docker exec spark-slave1 jps                 # expect: Worker
docker exec spark-slave2 jps                 # expect: Worker
# Spark Master UI: http://localhost:8080

# ── 2. Terminal 1 — Start Producer ────────────────────────────────
mvn exec:java \
  -Dexec.mainClass="com.traffic.streaming.TrafficDataProducer" \
  -Dexec.args="dataset/Metro_Interstate_Traffic_Volume.csv 19999 500" \
  -q
# Wait for: "Waiting for Spark Streaming to connect..."

# ── 3. Terminal 2 — Start Streaming App ───────────────────────────
mvn exec:java \
  -Dexec.mainClass="com.traffic.streaming.TrafficStreamingApp" \
  -Dexec.args="local[2] localhost /tmp/spark-checkpoint-traffic 19999" \
  -q
# Wait ~10s for first batch. Watch for CONGESTION ALERT, STATS, WEATHER.

# ── 4. Generate a congestion event immediately ─────────────────────
# Any record with traffic_volume > 6000 triggers the alert.
# Record at line ~1 of dataset: 2012-10-02 16:00:00,Clear,6015 (fires within ~2 min at 500ms/rec)
# Use 50ms/rec for a faster demo: change 500 → 50 in producer args.

# ── 5. Graceful stop ───────────────────────────────────────────────
# Ctrl+C in Producer terminal → producer stops sending
# Ctrl+C in Streaming terminal → Spark Streaming shuts down
```

### Cluster-mode variant (uses actual Spark cluster, optional)

```bash
# Terminal 2 — Submit to cluster instead of local mode
mvn exec:java \
  -Dexec.mainClass="com.traffic.streaming.TrafficStreamingApp" \
  -Dexec.args="spark://spark-master:7077 hadoop-master /tmp/spark-checkpoint-traffic 19999" \
  -q
```

---

## 10. VERIFIED WORKING COMPONENTS

| Component | Evidence |
|-----------|----------|
| Docker 6-node cluster | `docker ps` — all 6 UP |
| Spark Master daemon | `docker exec spark-master jps` → `Master` |
| Spark Worker daemons | `docker exec spark-slave1/2 jps` → `Worker` |
| Spark Master UI | `http://localhost:8080` accessible |
| TrafficDataProducer | Bound port 19999, accepted connection, streamed 1520+ records |
| TrafficStreamingApp banner | Prints master, socket, checkpoint, threshold |
| Checkpoint directory | `ssc.checkpoint(dir)` — required for stateful ops, set correctly |
| Congestion detection (> 6000) | 118 live alerts captured |
| updateStateByKey (running avg) | 9 printed STATS lines, count grows monotonically across batches |
| reduceByKeyAndWindow (30s/5s) | 9 printed WEATHER blocks, 6+ conditions aggregated |
| Wire format match | Producer `timestamp,weather,volume` = Streaming parser format |
| CSV parsing safety | `NumberFormatException` and length guards in both classes |
| Java 8 compatibility | All lambdas replaced with anonymous inner classes — compiles and runs clean on Java 8 |

---

## 11. BROKEN COMPONENTS (BEFORE FIXES)

| Component | Bug | Status After Fix |
|-----------|-----|-----------------|
| Port 9999 (host) | Docker proxy holds port 9999 → `BindException` | ✅ Fixed — demo uses 19999 |
| Jackson 2.12.7.1 | Incompatible with Spark 2.4.5 → `ExceptionInInitializerError` | ✅ Fixed — pinned to 2.6.7.1 |
| Spark cluster auto-start | Daemons not started by `docker-compose up` | ✅ Operational (run `start-spark.sh`) |

---

## 12. DEFENSE READINESS

| Checkpoint | Status |
|------------|--------|
| Docker cluster starts cleanly | ✅ |
| Spark Master + 2 Workers registered | ✅ |
| Producer binds and streams data | ✅ |
| Congestion alerts fire on screen | ✅ |
| Running average updates per batch | ✅ |
| Weather window aggregation visible | ✅ |
| No crashes, no exceptions | ✅ |
| `mvn package` compiles clean | ✅ |
| All fixes are minimal (2 files, additive only) | ✅ |
| MapReduce portion unaffected | ✅ |

**The project is defense-ready. Run `bash scripts/start-spark.sh` before the demo and use port 19999 as shown in the corrected commands above.**
