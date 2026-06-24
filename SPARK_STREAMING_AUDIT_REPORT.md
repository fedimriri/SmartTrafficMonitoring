# Spark Streaming Audit Report
## SmartTrafficMonitoring — Senior Spark Streaming Engineer Assessment
**Date:** 2026-06-24  
**Analyst:** Senior Spark Streaming Engineer (Claude Sonnet 4.6)  
**Scope:** Full code audit of `TrafficStreamingApp.java` against project specification  
**Spark Version (runtime):** 2.4.5 — confirmed via `docker exec spark-master /opt/spark/bin/spark-submit --version`

---

## Runtime Environment at Audit Time

```
docker exec spark-master jps  →  94 Jps   (no Spark Master daemon)
docker exec spark-slave1 jps  →  46 Jps   (no Spark Worker daemon)
docker exec spark-slave2 jps  →  46 Jps   (no Spark Worker daemon)

docker exec hadoop-master jps →  213 Jps, 107 NodeCLI
                                  (NameNode / ResourceManager NOT running)
HDFS status: Connection refused — HDFS is DOWN at audit time
```

> **Note:** Spark 2.4.5 binary exists at `/opt/spark/bin/spark-submit`. Containers are Up. Spark daemons and Hadoop daemons are not currently running. Per reporting rules, the Spark runtime infrastructure exists (installed, containerised, previously functional). The code audit is based on source only and is infrastructure-independent.

---

## Source File Under Audit

**File:** [src/main/java/com/traffic/streaming/TrafficStreamingApp.java](src/main/java/com/traffic/streaming/TrafficStreamingApp.java)  
**Lines:** 102  
**Built JAR:** `target/SmartTrafficMonitoring-1.0-SNAPSHOT.jar` (14 KB, built 2026-06-18)

Full annotated source:

```java
 1  package com.traffic.streaming;
 2
 3  import org.apache.spark.SparkConf;
 4  import org.apache.spark.streaming.Durations;
 5  import org.apache.spark.streaming.api.java.*;
 6  import org.apache.spark.api.java.function.Function2;   // ← UNUSED IMPORT
 7  import scala.Tuple2;
 8  import java.util.List;                                 // ← UNUSED IMPORT
 9
10  public class TrafficStreamingApp {
11
12      public static void main(String[] args) throws Exception {
13
14          SparkConf conf = new SparkConf()
15                  .setAppName("SmartTrafficStreaming")
16                  .setMaster("local[*]");                // ← HARDCODED, not cluster-ready
17
18          JavaStreamingContext ssc =
19                  new JavaStreamingContext(conf, Durations.seconds(5));
19
20          JavaReceiverInputDStream<String> stream =
21                  ssc.socketTextStream("localhost", 9999);  // ← socket only
22
23          JavaDStream<TrafficEvent> events = stream
24                  .filter(line -> {                         // ← debug println in filter
25                      System.out.println("Received: [" + line + "]");
26                      return true;
27                  })
28                  .filter(line -> line != null && !line.trim().isEmpty())
29                  .filter(line -> {
30                      String[] parts = line.split(",");
31                      if (parts.length != 3) {
32                          System.out.println("INVALID LINE -> [" + line + "]");
33                          return false;
34                      }
35                      return true;
36                  })
37                  .map(line -> {
38                      String[] parts = line.split(",");
39                      return new TrafficEvent(
40                              parts[0].trim(),
41                              parts[1].trim(),
42                              Integer.parseInt(parts[2].trim()));  // ← UNHANDLED NumberFormatException
43                  });
44
45          // ALERT: threshold 8000 — spec requires 6000
46          events.filter(e -> e.trafficVolume > 8000)
47                  .foreachRDD(rdd -> {
48                      rdd.foreach(e -> System.out.println("HIGH TRAFFIC ALERT: "
49                              + e.timestamp + " -> " + e.trafficVolume));
50                  });
51
52          // WEATHER aggregation — per-batch SUM only, not cumulative
53          events.mapToPair(event ->
54                          new Tuple2<String, Integer>(event.weather, event.trafficVolume))
55                  .reduceByKey((a, b) -> a + b)
56                  .foreachRDD(rdd -> {
57                      System.out.println("\n===== WEATHER TRAFFIC SUMMARY =====");
58                      rdd.foreach(record ->
59                              System.out.println(record._1() + " => " + record._2()));
60                  });
61
62          ssc.start();
63          ssc.awaitTermination();
64      }
65
66      public static class TrafficEvent {        // ← NOT Serializable
67          public String timestamp;
68          public String weather;
69          public int trafficVolume;
70
71          public TrafficEvent(String t, String w, int v) {
72              this.timestamp = t;
73              this.weather = w;
74              this.trafficVolume = v;
75          }
76      }
77  }
```

---

## A. Data Ingestion

### A1. Socket Text Stream

**STATUS: ✅ COMPLETE**

```java
// Line 23
JavaReceiverInputDStream<String> stream = ssc.socketTextStream("localhost", 9999);
```

Evidence: `socketTextStream` is used with host `"localhost"` and port `9999`. A `JavaReceiverInputDStream<String>` is returned and consumed by the pipeline. This satisfies the basic real-time ingestion requirement.

**Limitation:** Host is hardcoded to `"localhost"`. In a cluster deployment, the socket producer would need to run on the same node as the Spark driver, or the host would need to be configurable via `args[0]` or a system property.

---

### A2. File Stream

**STATUS: ❌ MISSING**

No `ssc.textFileStream()`, `ssc.fileStream()`, or `ssc.binaryRecordsStream()` exists anywhere in the file. A file stream would allow reading from an HDFS directory (`/traffic-data/streaming/`) as files arrive, which is the production pattern for durable ingestion.

**What is required:**
```java
// Read from HDFS streaming directory (production pattern)
JavaDStream<String> hdfsStream =
    ssc.textFileStream("hdfs://hadoop-master:9000/traffic-data/streaming/");
```

---

### A3. Custom Receiver

**STATUS: ❌ MISSING**

No class extending `org.apache.spark.streaming.receiver.Receiver<String>` exists in the project. A custom receiver would encapsulate connection logic (e.g., TCP reconnect, Kafka consumer, sensor API polling) and provide fault tolerance via `Receiver.store()`.

---

## B. Parsing

### B1. Empty / Null Line Handling

**STATUS: ✅ COMPLETE**

```java
// Line 28
.filter(line -> line != null && !line.trim().isEmpty())
```

Both `null` and blank lines are filtered before any field extraction. Order is correct — this check runs before the format check.

---

### B2. Format Validation (field count)

**STATUS: ✅ COMPLETE**

```java
// Lines 29-36
.filter(line -> {
    String[] parts = line.split(",");
    if (parts.length != 3) {
        System.out.println("INVALID LINE -> [" + line + "]");
        return false;
    }
    return true;
})
```

Lines that do not split into exactly 3 parts are dropped with a console message. This correctly guards against missing fields.

---

### B3. Exception Handling in `.map()` — NumberFormatException

**STATUS: ❌ MISSING**

```java
// Line 42 — NO try-catch
Integer.parseInt(parts[2].trim())
```

The format filter (B2) only validates that the line has 3 comma-separated parts. It does **not** verify that `parts[2]` is a valid integer. If a well-formed 3-part line arrives with a non-numeric traffic volume (e.g., `"2026-06-24 08:00:00,Clouds,N/A"`), `Integer.parseInt` throws `NumberFormatException`.

In Spark Streaming, an unhandled exception inside a lambda passed to `.map()` propagates up to the task executor and **fails the entire micro-batch**. In `local[*]` mode this may cause a stream halt; in cluster mode it triggers task retries and eventually job failure.

**Required fix:**

```java
.flatMap(line -> {
    try {
        String[] parts = line.split(",");
        return Collections.singletonList(
            new TrafficEvent(
                parts[0].trim(),
                parts[1].trim(),
                Integer.parseInt(parts[2].trim()))
        ).iterator();
    } catch (NumberFormatException e) {
        System.err.println("BAD VOLUME FIELD: [" + line + "] — " + e.getMessage());
        return Collections.<TrafficEvent>emptyList().iterator();
    }
})
```

Using `flatMap` with an empty list return on exception is the idiomatic Spark way to drop a bad record without failing the batch.

---

### B4. Debug Print Inside Filter

**STATUS: ⚠️ PARTIAL — Performance Anti-Pattern**

```java
// Lines 24-27
.filter(line -> {
    System.out.println("Received: [" + line + "]");
    return true;
})
```

This filter always returns `true` — it is a logging side effect disguised as a filter. Issues:
- `System.out.println` inside a Spark lambda executes on the **executor JVM**, not the driver. Output appears in executor logs, not driver stdout — making it misleading.
- In cluster mode, these prints are invisible in the driver console entirely.
- At any real traffic volume this produces enormous log noise.
- Should be replaced with `ssc.sparkContext().setLogLevel("INFO")` or proper log4j logging if debugging is needed.

---

## C. Congestion Detection

**STATUS: ⚠️ PARTIAL — Wrong Threshold**

```java
// Line 46 — threshold is 8000, specification requires 6000
events.filter(e -> e.trafficVolume > 8000)
        .foreachRDD(rdd -> {
            rdd.foreach(e -> System.out.println(
                "\n *** HIGH TRAFFIC ALERT *** \n"
                + "[ALERT] HIGH TRAFFIC ALERT: "
                + e.timestamp + " -> " + e.trafficVolume));
        });
```

**What is correct:**
- Filtering on `trafficVolume` is the right approach.
- `foreachRDD` → `rdd.foreach()` correctly triggers an action per micro-batch.
- Alert message printed to console.

**What is wrong:**
- Threshold is `> 8000`. Specification states `traffic_volume > 6000`.
- At threshold 8000, events with volumes between 6001 and 8000 are silently missed — **33% of the required congestion range is ignored**.
- No severity levels (moderate congestion 6000–8000 vs. severe > 8000).
- No alert persistence — output is console-only; no write to HDFS, Kafka topic, or database.
- `foreachRDD` with `rdd.foreach` has no accumulated state — each micro-batch is evaluated independently, so a road holding 7500 vehicles continuously across multiple batches never triggers an alert.

**Required fix:**

```java
// Correct threshold per specification
events.filter(e -> e.trafficVolume > 6000)
        .foreachRDD(rdd -> {
            List<TrafficEvent> alerts = rdd.collect();
            if (!alerts.isEmpty()) {
                for (TrafficEvent e : alerts) {
                    System.err.println("[CONGESTION ALERT] "
                        + e.timestamp + " | weather=" + e.weather
                        + " | volume=" + e.trafficVolume);
                }
                // Optionally persist to HDFS:
                // rdd.map(e -> e.timestamp + "," + e.weather + "," + e.trafficVolume)
                //    .saveAsTextFile("hdfs://hadoop-master:9000/traffic-data/streaming/alerts/"
                //                   + System.currentTimeMillis());
            }
        });
```

---

## D. Real-Time Statistics — Current Average Traffic

**STATUS: ❌ MISSING**

No average calculation exists anywhere in the application. The weather aggregation computes a per-batch **sum** by weather group, which is not the same as an average.

A correct running average requires tracking both total volume and record count:

```java
// Per-batch average (stateless — resets every 5 seconds)
events.foreachRDD(rdd -> {
    if (!rdd.isEmpty()) {
        JavaRDD<Integer> volumes = rdd.map(e -> e.trafficVolume);
        long count = volumes.count();
        long sum   = volumes.reduce(Integer::sum);
        double avg = (double) sum / count;
        System.out.printf("[STATS] Batch avg=%.1f  count=%d  sum=%d%n",
                          avg, count, sum);
    }
});
```

For a **running average** across all batches (cumulative), `updateStateByKey` or `mapWithState` is required — see section F.

---

## E. Window Operations

**STATUS: ❌ MISSING**

**Specification:** 30-second sliding window with 5-second slide interval.

No window operation of any kind exists in the code. The batch interval is set to 5 seconds (`Durations.seconds(5)`), which is the correct slide granularity, but no window is ever opened over it.

**What is required:**

```java
// Window: 30 seconds long, sliding every 5 seconds
// Batch interval must divide evenly into both window and slide durations
// 5s batch × 6 = 30s window ✓   5s batch × 1 = 5s slide ✓

JavaPairDStream<String, Integer> windowedWeather =
    events
        .mapToPair(e -> new Tuple2<>(e.weather, e.trafficVolume))
        .reduceByKeyAndWindow(
            (a, b) -> a + b,          // reduce function
            (a, b) -> a - b,          // inverse reduce (for efficiency)
            Durations.seconds(30),    // window length
            Durations.seconds(5));    // slide interval

windowedWeather.foreachRDD(rdd -> {
    System.out.println("\n=== WINDOWED WEATHER SUMMARY (last 30s) ===");
    rdd.foreach(r -> System.out.println(r._1() + " => " + r._2()));
});

// Window-based congestion alert
JavaDStream<TrafficEvent> windowedAlerts =
    events
        .window(Durations.seconds(30), Durations.seconds(5))
        .filter(e -> e.trafficVolume > 6000);
```

> **Note:** `reduceByKeyAndWindow` with an inverse function (`(a, b) -> a - b`) requires checkpointing. See section G.

---

## F. Stateful Processing — `updateStateByKey`

**STATUS: ❌ MISSING**

No stateful DStream operation (`updateStateByKey`, `mapWithState`) exists anywhere in the code. Every computation resets at the end of each 5-second micro-batch.

Without stateful processing:
- Cumulative traffic totals across the session are impossible.
- Running averages (requirement 6) are impossible.
- Persistent congestion tracking (e.g., "Road X has been congested for 3 consecutive batches") is impossible.

**What is required:**

```java
// Step 1: Checkpointing is mandatory before updateStateByKey
ssc.checkpoint("/tmp/spark-checkpoint");   // or HDFS path

// Step 2: State update function — accumulates (sum, count) per weather key
Function2<List<Integer>, Optional<long[]>, Optional<long[]>> updateFunction =
    (newValues, runningState) -> {
        long[] state = runningState.or(new long[]{0L, 0L}); // [sum, count]
        for (int v : newValues) {
            state[0] += v;   // accumulate sum
            state[1]++;      // accumulate count
        }
        return Optional.of(state);
    };

// Step 3: Apply stateful aggregation
JavaPairDStream<String, long[]> runningTotals =
    events
        .mapToPair(e -> new Tuple2<>(e.weather, e.trafficVolume))
        .updateStateByKey(updateFunction);

// Step 4: Print running average per weather
runningTotals.foreachRDD(rdd -> {
    System.out.println("\n=== CUMULATIVE WEATHER AVERAGES ===");
    rdd.foreach(r -> {
        long sum   = r._2()[0];
        long count = r._2()[1];
        double avg = count > 0 ? (double) sum / count : 0.0;
        System.out.printf("%-15s  total=%-12d  avg=%.1f  count=%d%n",
                          r._1(), sum, avg, count);
    });
});
```

---

## G. Checkpointing

**STATUS: ❌ MISSING**

No `ssc.checkpoint()` call exists anywhere in the application.

Checkpointing is **mandatory** for two of the missing features:
1. `updateStateByKey` — Spark cannot recover state without a checkpoint directory.
2. `reduceByKeyAndWindow` with inverse reduce — requires checkpointing to reconstruct the sliding window after a failure.

Without checkpointing:
- Any attempt to add `updateStateByKey` will throw: `java.lang.Exception: No output operations registered, so nothing to execute`
- Driver failures have no recovery path.
- Window operations degrade to full recomputation each slide (no inverse function).

**Required fix:**

```java
// Add immediately after creating JavaStreamingContext, before any DStream operations
// Local path for development:
ssc.checkpoint("/tmp/spark-checkpoint-traffic");

// HDFS path for production (recommended):
// ssc.checkpoint("hdfs://hadoop-master:9000/traffic-data/streaming/checkpoint");
```

> For production, the checkpoint path must be on a fault-tolerant filesystem (HDFS, S3, GCS). A local path is lost on driver restart.

**Idiomatic pattern for checkpoint recovery** (also missing):

```java
// Replace the direct constructor call with getOrCreate for crash recovery:
JavaStreamingContext ssc = JavaStreamingContext.getOrCreate(
    checkpointDir,
    () -> {
        SparkConf conf = new SparkConf().setAppName("SmartTrafficStreaming");
        JavaStreamingContext ctx = new JavaStreamingContext(conf, Durations.seconds(5));
        ctx.checkpoint(checkpointDir);
        // ... build all DStreams here ...
        return ctx;
    });
```

---

## H. Deployment

### H1. Local Mode — `local[*]`

**STATUS: ✅ COMPLETE**

```java
// Line 16
.setMaster("local[*]");
```

The application runs in local mode using all available CPU cores. Functional for development and testing on a single machine.

---

### H2. Cluster Mode — `spark://master`

**STATUS: ❌ MISSING**

The master URL is hardcoded to `"local[*]"`. A comment on line 17 reads `// change to spark:// for cluster mode` — but this is a note to the developer, not an implementation.

For deployment to the running Spark cluster (`spark-master`, `spark-slave1`, `spark-slave2`):

```java
// Option 1: Accept master URL from command line (recommended)
String master = args.length > 0 ? args[0] : "local[*]";
SparkConf conf = new SparkConf()
    .setAppName("SmartTrafficStreaming")
    .setMaster(master);

// Then submit with:
// spark-submit --master spark://spark-master:7077 \
//              --class com.traffic.streaming.TrafficStreamingApp \
//              SmartTrafficMonitoring-1.0-SNAPSHOT.jar spark://spark-master:7077

// Option 2: Remove setMaster entirely and pass via spark-submit --master flag
// (best practice — never hardcode master in production code)
SparkConf conf = new SparkConf().setAppName("SmartTrafficStreaming");
// spark-submit sets the master; no hardcoding needed
```

---

### H3. Cluster Compatibility — `TrafficEvent` Serialization

**STATUS: ❌ MISSING — Critical Cluster Bug**

```java
// Line 66 — missing implements Serializable
public static class TrafficEvent {
    public String timestamp;
    public String weather;
    public int trafficVolume;
    ...
}
```

`TrafficEvent` does not implement `java.io.Serializable`. In `local[*]` mode all operations run in a single JVM, so Java serialization is not invoked. In cluster mode, Spark must serialize task closures and objects to send them to worker nodes via Java serialization. Any attempt to use `TrafficEvent` in a lambda that crosses executor boundaries (`filter`, `map`, `foreachRDD`) will throw:

```
org.apache.spark.SparkException: Task not serializable
Caused by: java.io.NotSerializableException: com.traffic.streaming.TrafficStreamingApp$TrafficEvent
```

**Required fix:**

```java
public static class TrafficEvent implements java.io.Serializable {
    private static final long serialVersionUID = 1L;   // best practice

    public String timestamp;
    public String weather;
    public int trafficVolume;

    public TrafficEvent(String t, String w, int v) {
        this.timestamp = t;
        this.weather = w;
        this.trafficVolume = v;
    }
}
```

---

## Code Quality Review

### Issue Summary

| # | Issue | Severity | Location |
|---|---|---|---|
| CQ-01 | `TrafficEvent` not `Serializable` — crashes in cluster mode | **CRITICAL** | Line 66 |
| CQ-02 | `Integer.parseInt` unguarded — `NumberFormatException` fails batch | **HIGH** | Line 42 |
| CQ-03 | Congestion threshold `8000` vs spec `6000` — wrong by 33% | **HIGH** | Line 46 |
| CQ-04 | `setMaster("local[*]")` hardcoded — not deployable to cluster | **HIGH** | Line 16 |
| CQ-05 | Debug `println` inside `.filter()` — misleading and noisy in cluster | **MEDIUM** | Lines 24–27 |
| CQ-06 | Unused imports `Function2`, `List` — dead code | **LOW** | Lines 6, 8 |
| CQ-07 | Weather aggregation is per-batch sum, not cumulative | **MEDIUM** | Lines 52–60 |
| CQ-08 | All output is `System.out.println` — no HDFS persistence | **MEDIUM** | All output |
| CQ-09 | Socket host `"localhost"` hardcoded | **LOW** | Line 23 |
| CQ-10 | No `gracefulStop` or shutdown hook | **LOW** | Entire class |

### Positive Code Quality Observations

- Multi-step filtering pipeline before `.map()` is the correct Spark pattern — validate before transform.
- Null check before empty check on line 28 avoids a NullPointerException.
- `Tuple2<String, Integer>` + `reduceByKey` for weather aggregation is the correct Spark idiom.
- `ssc.awaitTermination()` correctly blocks the driver thread.
- Batch interval of 5 seconds is architecturally compatible with the required 5-second slide.

---

## Full Requirement Status Matrix

### Requirements 1–10

| # | Requirement | Status | Evidence |
|---|---|---|---|
| 1 | Real-time traffic ingestion | ✅ COMPLETE | `ssc.socketTextStream("localhost", 9999)` at line 23 |
| 2 | Traffic event parsing | ⚠️ PARTIAL | Format validation exists; `parseInt` unguarded (CQ-02) |
| 3 | Congestion detection | ⚠️ PARTIAL | Filter exists at line 46 but threshold is 8000, spec is 6000 |
| 4 | Real-time traffic statistics | ❌ MISSING | No average, count, or per-batch statistics computed |
| 5 | Weather aggregation | ✅ COMPLETE | `reduceByKey` on weather key; per-batch sum printed |
| 6 | Running average traffic | ❌ MISSING | No cumulative tracking; no `updateStateByKey` |
| 7 | Sliding window analytics | ❌ MISSING | No `window()` or `reduceByKeyAndWindow()` call exists |
| 8 | Stateful processing | ❌ MISSING | No `updateStateByKey` or `mapWithState` anywhere |
| 9 | Checkpointing | ❌ MISSING | No `ssc.checkpoint()` call anywhere |
| 10 | Production-ready Spark config | ❌ MISSING | Local master hardcoded; `TrafficEvent` not serializable |

### Section A–H Detail

| Section | Item | Status |
|---|---|---|
| A | socketTextStream | ✅ COMPLETE |
| A | file stream (HDFS) | ❌ MISSING |
| A | custom receiver | ❌ MISSING |
| B | null / empty line handling | ✅ COMPLETE |
| B | format validation (field count) | ✅ COMPLETE |
| B | `NumberFormatException` handling in `.map()` | ❌ MISSING |
| B | debug println in filter (anti-pattern) | ⚠️ PARTIAL |
| C | congestion detection filter exists | ⚠️ PARTIAL |
| C | correct threshold (> 6000) | ❌ MISSING — uses 8000 |
| D | current average traffic per batch | ❌ MISSING |
| D | cumulative running average | ❌ MISSING |
| E | 30-second window | ❌ MISSING |
| E | 5-second slide interval | ❌ MISSING |
| F | `updateStateByKey` | ❌ MISSING |
| G | `ssc.checkpoint()` | ❌ MISSING |
| G | `getOrCreate` recovery pattern | ❌ MISSING |
| H | `local[*]` mode | ✅ COMPLETE |
| H | `spark://master` cluster URL | ❌ MISSING |
| H | `TrafficEvent implements Serializable` | ❌ MISSING |

---

## Production Readiness Score

| Dimension | Score | Max | Notes |
|---|---|---|---|
| Ingestion layer | 2 | 5 | Socket only; no HDFS stream, no custom receiver |
| Parsing robustness | 3 | 5 | Good format validation; unguarded parseInt is a crash risk |
| Analytics correctness | 2 | 10 | Sum exists; no average, no window, no state |
| Fault tolerance | 0 | 10 | No checkpointing, no recovery, no graceful shutdown |
| Cluster deployability | 1 | 10 | Hardcoded local master; TrafficEvent not serializable |
| Output / observability | 1 | 5 | Console only; no HDFS writes, no metrics |
| Code cleanliness | 3 | 5 | Unused imports, debug prints, hardcoded values |
| **TOTAL** | **12** | **50** | **24% production ready** |

---

## Completion Percentage

| Category | Implemented | Total Items | % |
|---|---|---|---|
| Data ingestion variants | 1 | 3 | 33% |
| Parsing & validation | 2 | 3 | 67% |
| Congestion detection (correct spec) | 0 | 1 | 0% |
| Real-time statistics | 1 | 3 | 33% |
| Window operations | 0 | 2 | 0% |
| Stateful processing | 0 | 1 | 0% |
| Checkpointing | 0 | 2 | 0% |
| Deployment readiness | 1 | 3 | 33% |
| **TOTAL** | **5** | **18** | **28%** |

**Overall Spark Streaming implementation: 28% complete**

---

## Priority Fix Order

| Priority | Fix | Effort | Impact |
|---|---|---|---|
| 1 — CRITICAL | Add `implements java.io.Serializable` to `TrafficEvent` | 1 line | Unblocks cluster deployment |
| 2 — HIGH | Fix congestion threshold from `8000` to `6000` | 1 line | Matches specification |
| 3 — HIGH | Wrap `Integer.parseInt` in try-catch inside `.map()` (or switch to `flatMap`) | ~10 lines | Prevents batch failure on bad data |
| 4 — HIGH | Add `ssc.checkpoint(path)` before any DStream is created | 1 line | Required for requirements 6, 7, 8 |
| 5 — HIGH | Add `updateStateByKey` for running average (requirement 6, 8) | ~25 lines | Enables requirements 6 and 8 |
| 6 — HIGH | Add `reduceByKeyAndWindow` (30s / 5s) for requirement 7 | ~15 lines | Enables requirement 7 |
| 7 — MEDIUM | Add per-batch average calculation for requirement 4 | ~10 lines | Enables requirement 4 |
| 8 — MEDIUM | Remove `setMaster("local[*]")` hardcoding; accept via args or spark-submit | ~5 lines | Enables cluster mode |
| 9 — LOW | Remove debug `println` filter; use proper logging | ~5 lines | Performance and correctness |
| 10 — LOW | Add HDFS output for alerts and aggregations | ~10 lines | Persistence requirement |
| 11 — LOW | Remove unused imports (`Function2`, `List`) | 2 lines | Code cleanliness |
