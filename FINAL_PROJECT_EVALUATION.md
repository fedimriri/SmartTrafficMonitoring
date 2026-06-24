# Final Project Evaluation — SmartTrafficMonitoring

**Role:** Lead Evaluator  
**Date:** 2026-06-24  
**Sources:**
- `HADOOP_INFRASTRUCTURE_REPORT.md` — Senior Hadoop Architect (2026-06-21)
- `MAPREDUCE_AUDIT_REPORT.md` — Hadoop MapReduce Expert (2026-06-21)
- `SPARK_STREAMING_AUDIT_REPORT.md` — Senior Spark Streaming Engineer (2026-06-24)
- `DEPLOYMENT_AUDIT_REPORT.md` — DevOps Architect (2026-06-24)
- `SPECIFICATION_COMPLIANCE_REPORT.md` — Big Data Project Reviewer (2026-06-24)

---

## 1. Executive Summary

The SmartTrafficMonitoring project demonstrates a **functional but incomplete** Big Data implementation. The Hadoop infrastructure is the strongest element — the full 3-node cluster is operational, both MapReduce jobs ran via YARN, and their outputs persist in HDFS. However, the computation logic stops at raw summation without averages, peak identification, or formatted reports. The Spark Streaming component has a working pipeline skeleton but is missing every stateful, windowed, and cluster-mode requirement. Deployment is entirely non-reproducible from the repository — no Docker artifacts, no scripts, no startup procedure exists in version control.

The project achieves **53% overall specification compliance** across 106 audited requirements. Of 14 specification categories, only Hadoop cluster infrastructure reaches 100%. Six categories score below 30%, with Real-Time Monitoring and HDFS Organization at 0%.

The project can be defended for its Hadoop component today. The Spark defense is weak and will expose several critical gaps under questioning.

---

## 2. Completed Requirements

The following requirements are **fully implemented** with runtime evidence.

### Hadoop Infrastructure
- 3-node Hadoop cluster fully operational (`hadoop-master`, `hadoop-worker1`, `hadoop-worker2`)
- NameNode (PID 387), SecondaryNameNode (PID 632), ResourceManager (PID 875) on `hadoop-master`
- DataNode (PID 101) on `hadoop-worker1` (172.18.0.3:9866) and `hadoop-worker2` (172.18.0.4:9866)
- YARN NodeManager (PID 227 and 226) on both workers
- 2 live DataNodes confirmed via `hdfs dfsadmin -report`; 934.69 GB total cluster capacity
- `mapred-site.xml`: `mapreduce.framework.name = yarn` — jobs run through YARN
- Replication factor = 2; both DataNodes hold identical block copies

### Dataset
- `Metro_Interstate_Traffic_Volume.csv` (3.1 MB) in repository at `dataset/`
- Uploaded to HDFS at `/traffic/input/Metro_Interstate_Traffic_Volume.csv`; replicated: 6.2 MB total
- CSV schema exactly matches specification: `holiday,temp,rain_1h,snow_1h,clouds_all,weather_main,weather_description,date_time,traffic_volume`
- Header row confirmed at runtime via `hdfs dfs -cat`

### MapReduce Job 1 — Hourly
- Hour extraction: `dateTime.substring(11, 13)` on `fields[7]` produces zero-padded keys `00`–`23`
- Traffic volume summation: `sum += val.get()` — 24 correct hourly totals in HDFS
- Header row skipped: `line.startsWith("holiday")`
- Malformed row handling: silent `catch(Exception)` prevents job failure
- Driver wired correctly: `TrafficHourMapper`, `TrafficHourReducer`, `Text`/`IntWritable` types, paths from `args[]`, `waitForCompletion(true)`
- Job completed via YARN: `_SUCCESS` file at `/traffic/output/hourly/` (2026-06-15)

### MapReduce Job 2 — Weather
- Weather condition extraction: `fields[5]` → 11 weather categories (`Clear`, `Clouds`, `Drizzle`, `Fog`, `Haze`, `Mist`, `Rain`, `Smoke`, `Snow`, `Squall`, `Thunderstorm`)
- Traffic volume summation: correct totals, peak derivable as `Clouds` (54,870,172), lowest as `Squall` (8,247)
- Same header skip and error handling as Job 1
- Driver wired correctly; job completed via YARN: `_SUCCESS` at `/traffic/output/weather/` (2026-06-15)

### Spark Streaming — Core Pipeline
- `JavaStreamingContext` with 5-second batch interval (`Durations.seconds(5)`)
- `socketTextStream("localhost", 9999)` — real-time ingestion channel established
- Multi-stage filtering: null check → empty check → field count validation (reject if `parts.length != 3`)
- `TrafficEvent` POJO created from parsed fields (timestamp, weather, trafficVolume)
- `HIGH TRAFFIC ALERT` message printed via `foreachRDD` (alert mechanism exists)
- Per-batch weather grouping: `mapToPair` → `reduceByKey` — sum per weather condition each 5 seconds
- Weather summary printed each batch: `===== WEATHER TRAFFIC SUMMARY =====`
- `ssc.start()` + `ssc.awaitTermination()` — application lifecycle correct

### Delivery Artifacts
- 8 Java source files committed to `src/main/java/com/traffic/`
- `pom.xml` with Hadoop 3.3.6 + Spark 2.4.5 dependencies
- Built JAR: `target/SmartTrafficMonitoring-1.0-SNAPSHOT.jar` (14 KB, 2026-06-18)
- `README.md` — 324 lines with technology stack, usage instructions, architecture
- Git repository with 5 commits on `main` branch

---

## 3. Partially Implemented Requirements

These items have a working foundation but contain a specific, fixable gap.

| Item | What Works | What Is Wrong | Fix Size |
|---|---|---|---|
| Congestion detection | Filter + alert message exists; `foreachRDD` pattern is correct | Threshold is `8000` — specification requires `6000`; 33% of the required range is never alerted | 1 character change (`8000` → `6000`) |
| Spark event parsing | Null/empty/field-count validation is correct | `Integer.parseInt(parts[2].trim())` is unguarded — `NumberFormatException` fails the entire micro-batch | ~10 lines (switch to `flatMap` with try-catch) |
| Container networking | Both cluster networks exist and internal DNS resolves | `spark-network` (172.19.0.0/16) and `hadoop` (172.18.0.0/16) are isolated — Spark cannot reach HDFS | `docker network connect hadoop spark-master` |
| HDFS path convention | Dataset in HDFS, MapReduce outputs in HDFS | Paths use `/traffic/input` and `/traffic/output` instead of spec-required `/traffic-data/historical` and `/traffic-data/streaming` | `hdfs dfs -mkdir -p` + copy commands |
| Spark cluster containers | `spark-master`, `spark-slave1`, `spark-slave2` containers running; Spark 2.4.5 binary installed | Spark standalone daemons (Master/Worker JVM) not started; `jps` shows only `Jps` on all three containers | `docker exec spark-master /opt/spark/sbin/start-all.sh` |
| Batch report output | Files exist in HDFS with correct data | Raw `hour TAB sum` format — no average column, no PEAK/LOWEST labels, no report header | Reducer `cleanup()` rewrite |
| README documentation | Project goals, tech stack, dataset schema documented | Uses `mvn exec:java` instead of `hadoop jar` and `spark-submit`; no Docker commands | Update README with correct commands |

---

## 4. Missing Requirements

These items have **no implementation** — zero lines of code address them.

### MapReduce Layer
- **Average computation** — both reducers emit `SUM` only; no `count` variable exists; no division occurs anywhere
- **Peak hour identification** — no cross-key comparison in Job 1; `cleanup()` never overridden; hour 16 (11,259,548 total) is never labeled as peak
- **Lowest traffic hour** — same absence; hour 03 (751,459 total) is never labeled as lowest
- **Highest traffic weather** — no cross-key comparison in Job 2; `Clouds` (54,870,172) is never labeled
- **Lowest traffic weather** — `Squall` (8,247) is never labeled
- **Combiner** — `setCombinerClass` not called in either driver; shuffle traffic is unnecessarily large

### Spark Streaming Layer
- **`ssc.checkpoint()`** — no call anywhere in 102 lines; required for `updateStateByKey` and `reduceByKeyAndWindow`
- **`updateStateByKey`** — no stateful DStream operation; cumulative totals reset every 5-second batch
- **Running average traffic** — no cumulative average; no counter maintained across batches
- **Per-batch average** — not even per-batch average computed; no division, no `count()` call
- **`reduceByKeyAndWindow(30s, 5s)`** — no window operation of any kind; spec requires 30-second window with 5-second slide
- **Cumulative weather average** — streaming weather aggregation is per-batch sum only; session-wide totals never tracked
- **`TrafficEvent implements Serializable`** — missing; triggers `SparkException: Task not serializable` in any cluster-mode deployment
- **Cluster-mode master** — `setMaster("local[*]")` hardcoded; `spark://spark-master:7077` never set
- **`JavaStreamingContext.getOrCreate()`** — crash recovery pattern not used; driver restarts start from scratch
- **HDFS output for streaming** — all results written only to `System.out.println`; no `saveAsTextFiles`, no durable persistence
- **`getOrCreate` factory** — context is always created fresh, no recovery path

### Infrastructure and Deployment Layer
- **`docker-compose.yml`** — not present; `find . -name "docker-compose*.yml"` returns empty
- **`Dockerfile`** — not present; neither Hadoop nor Spark image build is documented
- **Deployment scripts** — zero `.sh` files in repository; no cluster startup, no dataset upload, no job submission scripts
- **`/traffic-data/historical`** — HDFS path does not exist; `hdfs dfs -ls /` shows `/traffic`, not `/traffic-data`
- **`/traffic-data/streaming`** — no streaming output directory of any kind in HDFS
- **Test suite** — `src/test/` directory does not exist; zero unit or integration tests

---

## 5. Technical Debt

These are code quality issues that do not block execution but create maintenance risk and exam risk under code review.

| ID | Issue | Severity | Location |
|---|---|---|---|
| TD-01 | `TrafficEvent` not `Serializable` — silent in local mode, fatal crash in cluster mode | **CRITICAL** | `TrafficStreamingApp.java:66` |
| TD-02 | `Integer.parseInt` unguarded — `NumberFormatException` kills the micro-batch | **HIGH** | `TrafficStreamingApp.java:42` |
| TD-03 | Congestion threshold `8000` vs specification `6000` — wrong result produced | **HIGH** | `TrafficStreamingApp.java:46` |
| TD-04 | `setMaster("local[*]")` hardcoded — app cannot be submitted to Spark cluster without source change | **HIGH** | `TrafficStreamingApp.java:16` |
| TD-05 | `private final static IntWritable trafficValue` in both Mappers — Hadoop may reuse JVM across task attempts; static mutable field is a data corruption risk | **MEDIUM** | `TrafficHourMapper.java:8`, `TrafficWeatherMapper.java:8` |
| TD-06 | `System.out.println` inside `.filter()` lambda — executes on executor JVM, not visible in driver console; produces enormous log noise at scale | **MEDIUM** | `TrafficStreamingApp.java:24-27` |
| TD-07 | Unused imports `Function2` and `List` | **LOW** | `TrafficStreamingApp.java:6-8` |
| TD-08 | `socketTextStream` host hardcoded to `"localhost"` — breaks in any multi-node deployment | **LOW** | `TrafficStreamingApp.java:23` |
| TD-09 | No `spark-defaults.conf` configuration — `spark-env.sh` is a template with no values | **LOW** | Spark container at `/opt/spark/conf/` |
| TD-10 | README references `mvn exec:java` for MapReduce — this runs drivers as a local JVM process, bypassing YARN entirely | **MEDIUM** | `README.md` |
| TD-11 | MapReduce output value type is `IntWritable` but would need to change to `Text` after adding averages — Driver change required simultaneously with Reducer change | **MEDIUM** | `TrafficHourDriver.java:24`, `TrafficWeatherDriver.java:24` |

---

## 6. Defense Readiness

### Verdict: ⚠️ CONDITIONAL — Defensible for Hadoop, Weak for Spark

#### What CAN be demonstrated today

| Demonstration | Status |
|---|---|
| Show 3 Hadoop containers running via `docker ps` | ✅ Ready |
| Show HDFS Web UI at `localhost:9870` | ✅ Ready |
| Show YARN Web UI at `localhost:8088` | ✅ Ready |
| Show dataset in HDFS via `hdfs dfs -ls /traffic/input` | ✅ Ready |
| Show MapReduce outputs via `hdfs dfs -cat /traffic/output/hourly/part-r-00000` | ✅ Ready |
| Run Spark streaming app in local mode with `nc -lk 9999` feeding data | ✅ Ready (local mode) |
| Show congestion alerts triggered (at wrong threshold 8000) | ⚠️ Works but wrong value |
| Show per-batch weather summary | ✅ Ready |

#### What CANNOT be demonstrated today

| Demonstration | Blocker |
|---|---|
| Spark cluster submission (`spark-submit --master spark://...`) | Spark daemons not running; network isolation blocks HDFS access |
| Correct congestion alerts (threshold > 6000) | Code uses 8000; requires recompile |
| Sliding window analytics (30s / 5s) | Not implemented — cannot be shown |
| Running average / stateful tracking | Not implemented — cannot be shown |
| Reproducible cluster deployment | Zero Docker artifacts in repository |
| Average per hour / per weather in MapReduce output | Not implemented — only SUM exists in HDFS |
| Peak/lowest identification in reports | Not implemented — no annotations in HDFS output |

#### Likely exam questions and risk assessment

| Question | Risk Level | Current Answer |
|---|---|---|
| "What does your reducer output per hour?" | **HIGH** — will expose missing average | "The total sum. We did not implement average." |
| "Where is the peak hour identified?" | **HIGH** — exposes complete omission | "It is not labeled in the output." |
| "Show me the Spark cluster running" | **HIGH** — Spark daemons not started | Cannot demonstrate cluster mode |
| "What happens with > 6000 volume events?" | **MEDIUM** — threshold is wrong | "We filter at > 8000 — different from spec." |
| "What is your HDFS directory structure?" | **MEDIUM** — path names wrong | "We use /traffic instead of /traffic-data." |
| "Can another developer recreate your environment?" | **HIGH** — no artifacts in repo | "No docker-compose.yml exists in our repository." |

---

## 7. Completion Percentage

### By Component

| Component | Metric | Score |
|---|---|---|
| **Infrastructure** | Runtime: Hadoop 100%, Spark 50%, HDFS paths 0%, repo artifacts 0% | **62%** |
| **MapReduce** | Job 1: 45%, Job 2: 40%, Reporting: 25% — weighted average | **43%** |
| **Spark Streaming** | Implementation: 28%, Production readiness: 24% | **26%** |
| **Deployment** | Runtime: 66%, Repository: 0% — blended | **50%** |
| **Documentation** | Source + README + dataset: 75%; no infra docs, no tests | **65%** |

### Overall

| Method | Score |
|---|---|
| Specification compliance (55 complete + 1.5 partial / 106) | **53%** |
| Requirements by count (55 complete / 106 total) | **52%** |
| Component average (weighted: Infra 25%, MR 25%, Spark 30%, Deploy 10%, Docs 10%) | **45%** |
| **Conservative overall estimate** | **~50%** |

---

## 8. Academic Evaluation

### Estimated Score: **11 / 20**

### Justification

| Criterion | Max | Awarded | Rationale |
|---|---|---|---|
| Hadoop cluster operational | 4 | 4.0 | Full 3-node cluster, all daemons confirmed, YARN scheduling, dataset in HDFS — complete evidence |
| MapReduce correctness | 4 | 1.5 | Both jobs run and produce output; aggregation logic correct; but no averages, no peak/lowest, no formatted report — 3 of 5 reducer requirements missing |
| Spark Streaming correctness | 5 | 1.5 | Socket ingestion, parsing pipeline, weather grouping present; missing checkpointing, stateful processing, windows, correct threshold, cluster mode — only the skeleton exists |
| Deployment and reproducibility | 3 | 0.5 | Cluster runs at runtime; zero Docker artifacts in repository; a grader cloning the repo cannot recreate anything |
| Code quality and documentation | 4 | 3.5 | Source is readable, README present, Maven build works; penalized for serialization bug, wrong threshold, debug anti-patterns, missing tests |
| **Total** | **20** | **11** | |

### Why not higher (12–15 range)
- MapReduce jobs produce **raw sums only**. The specification explicitly requires averages, peak identification, and lowest identification. These are central analytics requirements — not presentation extras. A grader running `hdfs dfs -cat` on the output will immediately see the absence of any of these values.
- The Spark Streaming component **has no stateful operations at all**. Real-time monitoring requirements (7.1–7.5) score 0/5. This is the most valued Spark-specific capability in any Big Data course.
- The wrong congestion threshold (8000 vs 6000) is a correctness error that will be caught during any live demonstration.

### Why not lower (8–10 range)
- The Hadoop infrastructure is **genuinely impressive** — a live 6-container cluster with confirmed YARN job execution and HDFS-persisted outputs demonstrates real operational knowledge.
- Both MapReduce jobs **did run** and their outputs are **numerically correct** for the sum dimension. The data pipeline is wired correctly end-to-end.
- The Spark app is structurally correct for local mode — the parsing pipeline is idiomatic Spark code.
- The project compiles, runs, and produces verifiable output — it is not a non-functional submission.

---

## 9. Exact Action Plan

### P0 — Must Finish Before Defense (hours of work)

These are small, high-impact changes that close critical demonstration gaps.

**P0-1 — Fix Spark congestion threshold (1 line)**

```java
// TrafficStreamingApp.java, line 46
// Change:
events.filter(e -> e.trafficVolume > 8000)
// To:
events.filter(e -> e.trafficVolume > 6000)
```

Impact: Closes the 33% gap in congestion detection. Immediately demonstrable.

---

**P0-2 — Add `implements Serializable` to TrafficEvent (1 line)**

```java
// TrafficStreamingApp.java, line 66
public static class TrafficEvent implements java.io.Serializable {
    private static final long serialVersionUID = 1L;
    // ... existing fields unchanged
}
```

Impact: Required for any cluster-mode deployment. Currently crashes with `SparkException: Task not serializable`.

---

**P0-3 — Wrap `parseInt` in try-catch (10 lines)**

```java
// TrafficStreamingApp.java — replace .map() with .flatMap()
.flatMap(line -> {
    try {
        String[] parts = line.split(",");
        return Collections.singletonList(
            new TrafficEvent(parts[0].trim(), parts[1].trim(),
                             Integer.parseInt(parts[2].trim()))
        ).iterator();
    } catch (NumberFormatException e) {
        return Collections.<TrafficEvent>emptyList().iterator();
    }
})
```

Impact: Prevents entire micro-batch failure on a single bad line.

---

**P0-4 — Start Spark daemons (3 commands)**

```bash
docker exec spark-master /opt/spark/sbin/start-master.sh
docker exec spark-slave1 /opt/spark/sbin/start-slave.sh spark://spark-master:7077
docker exec spark-slave2 /opt/spark/sbin/start-slave.sh spark://spark-master:7077
```

Impact: Makes `spark-submit --master spark://spark-master:7077` usable during demo.

---

**P0-5 — Connect Spark to Hadoop network (3 commands)**

```bash
docker network connect hadoop spark-master
docker network connect hadoop spark-slave1
docker network connect hadoop spark-slave2
```

Impact: Allows Spark to resolve `hdfs://hadoop-master:9000/` for reading/writing.

---

**P0-6 — Create correct HDFS paths (4 commands)**

```bash
docker exec hadoop-master hdfs dfs -mkdir -p /traffic-data/historical
docker exec hadoop-master hdfs dfs -mkdir -p /traffic-data/streaming
docker exec hadoop-master hdfs dfs -cp /traffic/input/Metro_Interstate_Traffic_Volume.csv \
    /traffic-data/historical/
docker exec hadoop-master hdfs dfs -ls /traffic-data
```

Impact: Closes HDFS Organization 0% score. Demonstrable in defense.

---

**P0-7 — Commit a `docker-compose.yml` to repository (30 lines)**

```yaml
version: "3"
services:
  hadoop-master:
    image: liliasfaxi/hadoop-cluster:latest
    container_name: hadoop-master
    networks: [hadoop]
    ports: ["9870:9870", "8088:8088", "9000:9000"]
  hadoop-worker1:
    image: liliasfaxi/hadoop-cluster:latest
    container_name: hadoop-worker1
    networks: [hadoop]
  hadoop-worker2:
    image: liliasfaxi/hadoop-cluster:latest
    container_name: hadoop-worker2
    networks: [hadoop]
  spark-master:
    image: spark-image:latest
    container_name: spark-master
    networks: [hadoop, spark-network]
    ports: ["8080:8080", "7077:7077", "9999:9999"]
  spark-slave1:
    image: spark-image:latest
    container_name: spark-slave1
    networks: [spark-network, hadoop]
  spark-slave2:
    image: spark-image:latest
    container_name: spark-slave2
    networks: [spark-network, hadoop]
networks:
  hadoop:
    driver: bridge
  spark-network:
    driver: bridge
```

Impact: Makes the deployment reproducible from the repository. Addresses the single biggest gap in the DevOps audit (FIND-01 CRITICAL).

---

### P1 — Should Finish (1–3 hours of work)

These are analytically significant but require more code changes.

**P1-1 — Add average + peak/lowest to both MapReduce Reducers**

Replace `TrafficHourReducer` and `TrafficWeatherReducer` with the `cleanup()`-based pattern provided in `MAPREDUCE_AUDIT_REPORT.md` (Modifications 1 and 2). Fixes DEF-01 and DEF-02 simultaneously. Also update both Drivers to use `setOutputValueClass(Text.class)`.

Expected impact: Moves MapReduce score from 43% to ~70%.

---

**P1-2 — Fix static mutable Writable in both Mappers (DEF-03)**

```java
// In both TrafficHourMapper and TrafficWeatherMapper:
// Change:
private final static IntWritable trafficValue = new IntWritable();
// To:
private IntWritable trafficValue = new IntWritable();
```

---

**P1-3 — Add checkpointing to Spark app (1 line, but prerequisite for P1-4 and P1-5)**

```java
// Add immediately after JavaStreamingContext creation:
ssc.checkpoint("/tmp/spark-checkpoint-traffic");
```

---

**P1-4 — Add `updateStateByKey` for cumulative weather average (~30 lines)**

Implement the state update function from `SPARK_STREAMING_AUDIT_REPORT.md`, Section F. Tracks `(sum, count)` per weather key across all batches. Prints cumulative averages each batch. Requires P1-3.

---

**P1-5 — Add `reduceByKeyAndWindow(30s, 5s)` (~15 lines)**

Implement the windowed weather aggregation from `SPARK_STREAMING_AUDIT_REPORT.md`, Section E. Requires P1-3 (checkpointing required for inverse reduce function).

---

**P1-6 — Remove `setMaster("local[*]")` hardcoding**

```java
// Accept master from args[0], default to local:
String master = (args.length > 0) ? args[0] : "local[*]";
SparkConf conf = new SparkConf().setAppName("SmartTrafficStreaming").setMaster(master);
```

---

### P2 — Nice to Have (low-priority polish)

| Task | Effort | Impact |
|---|---|---|
| Add Combiner to both MapReduce Drivers (`setCombinerClass`) | 2 lines | Reduces shuffle traffic; academic bonus |
| Add HDFS output for streaming alerts and summaries (`saveAsTextFiles`) | ~10 lines | Addresses HDFS streaming persistence requirement |
| Replace debug `println` filter with `ssc.sparkContext().setLogLevel()` | 5 lines | Code quality; misleading in cluster mode |
| Add `JavaStreamingContext.getOrCreate()` recovery pattern | ~20 lines | Fault tolerance; production practice |
| Create `scripts/setup-hdfs.sh`, `scripts/run-mapreduce.sh`, `scripts/run-spark.sh` | ~60 lines total | Reproducibility documentation |
| Add unit tests for Mapper and Reducer logic | ~50 lines | Addresses TD-11; expected in academic submissions |
| Update README with `hadoop jar` and `spark-submit` commands | ~20 lines | Replaces misleading `mvn exec:java` references |

---

## 10. Final Verdict

### Does the project satisfy the Hadoop MapReduce requirement?

**⚠️ PARTIAL — Core mechanics work; analytics are incomplete**

The Hadoop MapReduce requirement is **partially satisfied**. Both jobs compile, deploy to a live YARN cluster, and produce output in HDFS. The data pipeline is correctly wired: CSV parsing → key extraction → shuffle/sort → reduction → HDFS persistence. The cluster infrastructure is fully operational.

However, the specification requires averages, peak identification, and lowest identification as explicit outputs. None of these are present in either job's output. The HDFS files contain raw sums in tab-separated format with no labels and no aggregate statistics. A grader reading `part-r-00000` will not see the peak hour labeled, the average per hour, or the lowest-traffic hour — these are the analytical deliverables of the MapReduce layer.

**Evidence:** `/traffic/output/hourly/part-r-00000` contains 24 lines of `hour\tsum` format. `/traffic/output/weather/part-r-00000` contains 11 lines of `weather\tsum` format. No annotations, no averages, no extrema.

---

### Does the project satisfy the Spark Streaming requirement?

**❌ NOT SATISFIED — Skeleton only; all advanced requirements missing**

The Spark Streaming requirement is **not satisfied** at specification level. The application has a working ingestion pipeline and a functioning per-batch weather sum, but every advanced streaming requirement is absent:

- No checkpointing
- No stateful processing (`updateStateByKey`)
- No sliding window analytics (`reduceByKeyAndWindow`)
- No running average
- No cumulative statistics of any kind
- Wrong congestion threshold (8000 vs required 6000)
- No cluster deployment (daemons not running; `local[*]` hardcoded)
- `TrafficEvent` is not `Serializable` — crashes on cluster submission

The application cannot demonstrate the defining capabilities of a Spark Streaming system. It operates as a stateless filter-and-print pipeline with no temporal memory, which is the antithesis of stream processing as a paradigm.

**Evidence:** Full scan of `TrafficStreamingApp.java` (102 lines) — no `checkpoint`, no `updateStateByKey`, no `window`, no `reduceByKeyAndWindow`, no `mapWithState`. `docker exec spark-master jps` → `120 Jps` (no Master process).

---

### Does the project satisfy the Big Data course requirement?

**⚠️ MARGINAL — Infrastructure quality partially compensates for code gaps**

The project demonstrates real Big Data infrastructure knowledge. A 6-container Docker cluster with HDFS, YARN, and confirmed distributed job execution is a meaningful operational achievement. The pipeline from raw data → HDFS ingestion → distributed MapReduce → HDFS output is end-to-end functional.

What prevents full satisfaction of the course requirement:
1. The analytical layer (averages, extrema) is entirely absent from batch processing
2. The streaming layer (state, windows, checkpointing) is entirely absent from Spark
3. No deployment reproducibility — the environment cannot be recreated from the repository
4. The HDFS naming convention does not match the specification

The project demonstrates **that the student knows how to set up and operate a Big Data cluster**, which is a prerequisite skill. It does not fully demonstrate **that the student can write correct distributed analytics**, which is the primary learning objective.

**Estimated academic score: 11/20.** Defensible if the student can clearly explain the Hadoop architecture, the MapReduce paradigm, and acknowledge the Spark gaps during questioning. Likely to lose marks during live code review when the absence of averages and stateful streaming is probed.

---

## Quick Reference — Fix Priority Matrix

| Fix | P-Level | Files | Lines Changed | Score Impact |
|---|---|---|---|---|
| Fix threshold `8000` → `6000` | P0 | `TrafficStreamingApp.java` | 1 | +2% |
| Add `implements Serializable` | P0 | `TrafficStreamingApp.java` | 1 | +3% |
| Wrap `parseInt` in flatMap | P0 | `TrafficStreamingApp.java` | ~10 | +2% |
| Start Spark daemons | P0 | CLI | 3 commands | +4% |
| Connect Spark to Hadoop network | P0 | CLI | 3 commands | +3% |
| Create `/traffic-data/` HDFS paths | P0 | CLI | 4 commands | +5% |
| Commit `docker-compose.yml` | P0 | new file | ~30 | +6% |
| Add average + cleanup to Reducers | P1 | 2 Reducer + 2 Driver files | ~50 | +10% |
| Fix static Writable fields | P1 | 2 Mapper files | 2 | +1% |
| Add `ssc.checkpoint()` | P1 | `TrafficStreamingApp.java` | 1 | +1% (prerequisite) |
| Add `updateStateByKey` | P1 | `TrafficStreamingApp.java` | ~30 | +7% |
| Add `reduceByKeyAndWindow` | P1 | `TrafficStreamingApp.java` | ~15 | +5% |
| Remove `setMaster` hardcoding | P1 | `TrafficStreamingApp.java` | ~5 | +2% |
| **P0 total** | | | | **+25%** |
| **P0 + P1 total** | | | | **+51%** |
| **Projected final score after P0+P1** | | | | **~78%** |
