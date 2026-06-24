# MapReduce Audit Report
## SmartTrafficMonitoring — Hadoop MapReduce Expert Assessment
**Date:** 2026-06-21  
**Analyst:** Hadoop MapReduce Expert (Claude Sonnet 4.6)  
**Scope:** Full code audit of both MapReduce jobs against project specification

---

## Executive Summary

Both MapReduce jobs **compile, deploy, and execute successfully** on the live Hadoop cluster. The core mechanics (CSV parsing, key extraction, shuffle/sort, summing) are correctly implemented. However, **every reducer-level analytical requirement beyond raw summation is missing**: no averages are computed, no peak/lowest values are identified, and no formatted report is generated. The jobs are functionally incomplete against the specification.

**Job 1 (Hourly) implementation: 40%**  
**Job 2 (Weather) implementation: 40%**  
**Overall MapReduce completion: 40%**

---

## Runtime Environment (Verified)

### Cluster Status

```
docker exec hadoop-master jps
176  NameNode
420  SecondaryNameNode
663  ResourceManager

docker exec hadoop-worker1 jps
76   DataNode
201  NodeManager

docker exec hadoop-worker2 jps
76   DataNode
201  NodeManager
```

**Hadoop cluster: RUNNING** — 1 master (NameNode + ResourceManager) + 2 workers (DataNode + NodeManager each).

```
yarn node -list
Total Nodes: 2
hadoop-worker1:39465  RUNNING
hadoop-worker2:40715  RUNNING
```

**YARN: RUNNING** — 2 NodeManagers active and accepting containers.

### Dataset in HDFS (Runtime — path differs from spec)

```
hdfs dfs -ls /traffic/input
-rw-r--r--  2  root  supergroup  3237208  2026-06-15 12:36
    /traffic/input/Metro_Interstate_Traffic_Volume.csv

hdfs dfs -du -h /traffic/input
3.1 M   6.2 M   /traffic/input/Metro_Interstate_Traffic_Volume.csv
```

Dataset is present in HDFS, 3.1 MB, replication factor 2. Header row:

```
holiday,temp,rain_1h,snow_1h,clouds_all,weather_main,weather_description,date_time,traffic_volume
  [0]   [1]   [2]    [3]     [4]          [5]           [6]               [7]         [8]
```

### Existing HDFS Output (Jobs Already Ran)

```
hdfs dfs -ls /traffic/output/hourly
-rw-r--r--  2  root  supergroup  0    2026-06-15 12:42  _SUCCESS
-rw-r--r--  2  root  supergroup  265  2026-06-15 12:42  part-r-00000

hdfs dfs -ls /traffic/output/weather
-rw-r--r--  2  root  supergroup  0    2026-06-15 13:47  _SUCCESS
-rw-r--r--  2  root  supergroup  158  2026-06-15 13:47  part-r-00000
```

Both `_SUCCESS` files confirm jobs completed through YARN without errors.

---

## Job 1 — Traffic Volume by Hour

### 1.1 TrafficHourMapper.java — Audit

**File:** [src/main/java/com/traffic/mapreduce/hourly/TrafficHourMapper.java](src/main/java/com/traffic/mapreduce/hourly/TrafficHourMapper.java)

```java
public class TrafficHourMapper
        extends Mapper<Object, Text, Text, IntWritable> {

    private final static IntWritable trafficValue = new IntWritable();  // ← BUG
    private Text hour = new Text();

    public void map(Object key, Text value, Context context) {
        String line = value.toString();
        if (line.startsWith("holiday")) { return; }      // header skip
        try {
            String[] fields = line.split(",");
            String dateTime = fields[7];                  // date_time column
            int traffic = Integer.parseInt(fields[8]);    // traffic_volume column
            String extractedHour = dateTime.substring(11, 13);  // "HH" from "yyyy-MM-dd HH:mm:ss"
            hour.set(extractedHour);
            trafficValue.set(traffic);
            context.write(hour, trafficValue);
        } catch (Exception e) { /* silent skip */ }
    }
}
```

#### Requirement Checklist

| Requirement | Status | Finding |
|---|---|---|
| Header row skip | ✅ COMPLETE | `line.startsWith("holiday")` correctly skips the CSV header |
| Field index for `date_time` | ✅ COMPLETE | `fields[7]` matches column index 7 in the CSV schema |
| Field index for `traffic_volume` | ✅ COMPLETE | `fields[8]` matches column index 8 |
| Hour extraction | ✅ COMPLETE | `dateTime.substring(11, 13)` correctly extracts `HH` from `yyyy-MM-dd HH:mm:ss`; produces zero-padded strings `"00"`–`"23"` |
| Emit `(hour, volume)` | ✅ COMPLETE | `context.write(hour, trafficValue)` is correct |
| Malformed line handling | ✅ COMPLETE | Silent catch prevents job failure on bad rows |
| Emit count for average | ❌ MISSING | No count value (e.g., `1`) is emitted alongside volume; average cannot be computed downstream |
| Static mutable Writable | ⚠️ PARTIAL | `private final static IntWritable trafficValue` is a shared mutable instance. In Hadoop, each Mapper runs single-threaded in its JVM, so this is not a threading crash risk in practice. However, if the JVM reuses the same Mapper instance across input splits (which Hadoop is allowed to do), the `static` field is shared across logical mapper invocations. The safe and idiomatic pattern is an **instance field**. |

**Evidence from actual output:** Hour field extraction produces correctly zero-padded keys `00`–`23`, visible in HDFS output:

```
00  1700449
01  1058204
...
23  2997036
```

---

### 1.2 TrafficHourReducer.java — Audit

**File:** [src/main/java/com/traffic/mapreduce/hourly/TrafficHourReducer.java](src/main/java/com/traffic/mapreduce/hourly/TrafficHourReducer.java)

```java
public class TrafficHourReducer
        extends Reducer<Text, IntWritable, Text, IntWritable> {

    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values, Context context) {
        int sum = 0;
        for (IntWritable val : values) {
            sum += val.get();
        }
        result.set(sum);
        context.write(key, result);   // emits only: hour → total_sum
    }
}
```

#### Requirement Checklist

| Requirement | Status | Finding |
|---|---|---|
| Aggregate traffic volume (SUM) | ✅ COMPLETE | Correctly sums all volume values for each hour |
| Compute average traffic per hour | ❌ MISSING | No count variable; divides nothing; only SUM is emitted |
| Identify peak hour | ❌ MISSING | No cross-key comparison; no `cleanup()` to track maximum |
| Identify lowest traffic hour | ❌ MISSING | No cross-key comparison; no `cleanup()` to track minimum |
| Output format | ⚠️ PARTIAL | Emits raw `hour\tsum` — no labeled report, no average column, no annotation |

**Evidence from actual HDFS output:**

```
hdfs dfs -cat /traffic/output/hourly/part-r-00000

00  1700449    ← sum only, no average, no annotation
01  1058204
02   784086
03   751459    ← real lowest (751,459 total), but unreported as such
04  1469036
05  4321105
06  8641231
07  9854837
08  9541994
09  8849490
10  8695735
11  8717393
12  9224263
13  8981962
14  9710889
15  10135174
16  11259548   ← real peak (11,259,548 total), but unreported as such
17  10264377
18  8467745
19  6425009
20  5609807
21  5289840
22  4385615
23  2997036
```

The peak hour (16:00) and lowest hour (03:00) are computable from this output but **are not identified or annotated** anywhere in the job output.

---

### 1.3 TrafficHourDriver.java — Audit

**File:** [src/main/java/com/traffic/mapreduce/hourly/TrafficHourDriver.java](src/main/java/com/traffic/mapreduce/hourly/TrafficHourDriver.java)

```java
Configuration conf = new Configuration();
Job job = Job.getInstance(conf, "Traffic Volume By Hour");
job.setJarByClass(TrafficHourDriver.class);
job.setMapperClass(TrafficHourMapper.class);
job.setReducerClass(TrafficHourReducer.class);
job.setOutputKeyClass(Text.class);
job.setOutputValueClass(IntWritable.class);
FileInputFormat.addInputPath(job, new Path(args[0]));
FileOutputFormat.setOutputPath(job, new Path(args[1]));
System.exit(job.waitForCompletion(true) ? 0 : 1);
```

#### Requirement Checklist

| Requirement | Status | Finding |
|---|---|---|
| Argument validation | ✅ COMPLETE | Exits with usage message if `args.length != 2` |
| Job name | ✅ COMPLETE | `"Traffic Volume By Hour"` |
| `setJarByClass` | ✅ COMPLETE | Correctly identifies the JAR for YARN distribution |
| Mapper assignment | ✅ COMPLETE | `TrafficHourMapper.class` |
| Reducer assignment | ✅ COMPLETE | `TrafficHourReducer.class` |
| Output key/value types | ✅ COMPLETE | `Text.class`, `IntWritable.class` match reducer output |
| Input/output path | ✅ COMPLETE | Both taken from `args[0]`/`args[1]` at runtime |
| `waitForCompletion(true)` | ✅ COMPLETE | Verbose mode, blocks until YARN completes |
| Combiner | ❌ MISSING | No `job.setCombinerClass()` — the Reducer could be reused as a Combiner to reduce shuffle data; for a pure SUM it is always correct to do so |
| Map output types | ⚠️ PARTIAL | `setMapOutputKeyClass` / `setMapOutputValueClass` not set explicitly; they default to the output types (`Text`, `IntWritable`), which are the same here, so no runtime error — but not explicit |
| Report post-processing | ❌ MISSING | No second job or `cleanup()` phase to identify and annotate peak/lowest hour |

---

## Job 2 — Traffic Volume by Weather

### 2.1 TrafficWeatherMapper.java — Audit

**File:** [src/main/java/com/traffic/mapreduce/weather/TrafficWeatherMapper.java](src/main/java/com/traffic/mapreduce/weather/TrafficWeatherMapper.java)

```java
public class TrafficWeatherMapper
        extends Mapper<Object, Text, Text, IntWritable> {

    private final static IntWritable trafficValue = new IntWritable();  // ← BUG (same as hourly)
    private Text weather = new Text();

    public void map(Object key, Text value, Context context) {
        String line = value.toString();
        if (line.startsWith("holiday")) { return; }
        try {
            String[] fields = line.split(",");
            String weatherMain = fields[5];              // weather_main column
            int traffic = Integer.parseInt(fields[8]);   // traffic_volume column
            weather.set(weatherMain);
            trafficValue.set(traffic);
            context.write(weather, trafficValue);
        } catch (Exception e) { /* silent skip */ }
    }
}
```

#### Requirement Checklist

| Requirement | Status | Finding |
|---|---|---|
| Header row skip | ✅ COMPLETE | `line.startsWith("holiday")` correctly skips the CSV header |
| Field index for `weather_main` | ✅ COMPLETE | `fields[5]` matches column index 5 in the CSV schema |
| Field index for `traffic_volume` | ✅ COMPLETE | `fields[8]` matches column index 8 |
| Emit `(weather, volume)` | ✅ COMPLETE | `context.write(weather, trafficValue)` is correct |
| Malformed line handling | ✅ COMPLETE | Silent catch prevents job failure on bad rows |
| CSV split risk for `weather_description` | ⚠️ PARTIAL | `line.split(",")` is used on a CSV where `fields[6]` is `weather_description` (e.g., `"scattered clouds"`, `"sky is clear"`). These descriptions do **not** contain commas in the observed dataset, so `fields[5]` and `fields[8]` are safe. However, if any description contains a comma, `fields[8]` would shift and produce a `NumberFormatException` (silently caught). A split limit of `split(",", -1)` or a proper CSV parser would be safer. |
| Emit count for average | ❌ MISSING | No count value emitted; average cannot be computed downstream |
| Static mutable Writable | ⚠️ PARTIAL | Same issue as `TrafficHourMapper` — should be an instance field |

---

### 2.2 TrafficWeatherReducer.java — Audit

**File:** [src/main/java/com/traffic/mapreduce/weather/TrafficWeatherReducer.java](src/main/java/com/traffic/mapreduce/weather/TrafficWeatherReducer.java)

```java
public class TrafficWeatherReducer
        extends Reducer<Text, IntWritable, Text, IntWritable> {

    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values, Context context) {
        int sum = 0;
        for (IntWritable value : values) {
            sum += value.get();
        }
        result.set(sum);
        context.write(key, result);   // emits only: weather → total_sum
    }
}
```

#### Requirement Checklist

| Requirement | Status | Finding |
|---|---|---|
| Aggregate by weather (SUM) | ✅ COMPLETE | Correctly sums all volume values per weather category |
| Compute average traffic by weather | ❌ MISSING | No count; only SUM emitted |
| Identify highest traffic weather | ❌ MISSING | No cross-key max tracking; no `cleanup()` |
| Identify lowest traffic weather | ❌ MISSING | No cross-key min tracking; no `cleanup()` |
| Output format | ⚠️ PARTIAL | Emits raw `weather\tsum` — no average column, no peak/lowest annotation |

**Evidence from actual HDFS output:**

```
hdfs dfs -cat /traffic/output/weather/part-r-00000

Clear        40921675    ← real 2nd highest, but unreported as such
Clouds       54870172    ← real highest, but unreported as such
Drizzle       5992414
Fog           2465793
Haze          4762858
Mist         17451092
Rain         18819160
Smoke           64753
Snow          8676444
Squall           8247   ← real lowest, but unreported as such
Thunderstorm  3103676
```

Peak weather (`Clouds` — 54,870,172 total volume) and lowest weather (`Squall` — 8,247 total) are computable from this output but **are not identified or annotated** by the job.

---

### 2.3 TrafficWeatherDriver.java — Audit

**File:** [src/main/java/com/traffic/mapreduce/weather/TrafficWeatherDriver.java](src/main/java/com/traffic/mapreduce/weather/TrafficWeatherDriver.java)

Identical structure to `TrafficHourDriver`. Same findings apply:

| Requirement | Status | Finding |
|---|---|---|
| Argument validation | ✅ COMPLETE | Exits with usage message if `args.length != 2` |
| Job name | ✅ COMPLETE | `"Traffic Volume By Weather"` |
| All class assignments | ✅ COMPLETE | Mapper, Reducer, key/value types, input/output paths all correct |
| `waitForCompletion(true)` | ✅ COMPLETE | Blocking execution with verbose output |
| Combiner | ❌ MISSING | `TrafficWeatherReducer` is combinable (SUM is associative) but no `setCombinerClass` call |
| Report post-processing | ❌ MISSING | No second job or `cleanup()` to identify highest/lowest weather |

---

## Defect Register

### DEF-01 — No Average Computation (CRITICAL — Both Jobs)

**Affects:** `TrafficHourReducer`, `TrafficWeatherReducer`  
**Specification:** "Compute average traffic per hour" / "Compute average traffic by weather"  
**Current behaviour:** Reducer emits `SUM` only. `count` of records per key is never tracked.

To compute average, the mapper must emit a count alongside the value, OR the reducer must track count during the iteration:

```java
// Fix in reducer — track count during the single iteration
public void reduce(Text key, Iterable<IntWritable> values, Context context) {
    long sum = 0;
    int count = 0;
    for (IntWritable val : values) {
        sum += val.get();
        count++;
    }
    long average = (count > 0) ? sum / count : 0;
    // emit both: "sum=X avg=Y count=Z"
    context.write(key, new Text(
        String.format("sum=%d  avg=%d  count=%d", sum, average, count)));
}
```

Note: if a Combiner is added, count-based averaging breaks because the Combiner would sum partial counts and the reducer would see a pre-aggregated sum with count=1. The correct solution when using a Combiner is to emit a custom `SumCount` Writable:

```java
// Custom Writable (recommended for production)
public class SumCount implements Writable {
    public long sum;
    public int count;
    // write() / readFields() ...
}
// Mapper emits: (key, SumCount(volume, 1))
// Combiner and Reducer both merge: sum += other.sum; count += other.count;
// Final reducer: average = sum / count;
```

---

### DEF-02 — No Peak/Lowest Identification (CRITICAL — Both Jobs)

**Affects:** `TrafficHourReducer`, `TrafficWeatherReducer`, both Drivers  
**Specification:** "Identify peak hour", "Identify lowest traffic hour", "Identify highest/lowest traffic weather"  
**Current behaviour:** No cross-key comparison occurs anywhere in the pipeline.

**Option A — `cleanup()` in Reducer (single-pass, recommended):**

```java
public class TrafficHourReducer
        extends Reducer<Text, IntWritable, Text, Text> {

    private String peakHour = null;
    private String lowestHour = null;
    private long peakSum = Long.MIN_VALUE;
    private long lowestSum = Long.MAX_VALUE;

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        long sum = 0; int count = 0;
        for (IntWritable v : values) { sum += v.get(); count++; }
        long avg = count > 0 ? sum / count : 0;

        context.write(key, new Text(
            String.format("total=%-12d  avg=%-8d  count=%d", sum, avg, count)));

        if (sum > peakSum)   { peakSum = sum;   peakHour   = key.toString(); }
        if (sum < lowestSum) { lowestSum = sum;  lowestHour = key.toString(); }
    }

    @Override
    protected void cleanup(Context context)
            throws IOException, InterruptedException {
        context.write(new Text("PEAK_HOUR"),
            new Text(peakHour + "  total=" + peakSum));
        context.write(new Text("LOWEST_HOUR"),
            new Text(lowestHour + "  total=" + lowestSum));
    }
}
```

**Option B — Second MapReduce Job (more scalable, required for truly large datasets):**  
Run a second job whose mapper reads the hourly-sum output and emits a single fixed key (e.g., `"ALL"`) with `(hour, sum)` pairs, and whose reducer tracks the global maximum and minimum across all 24 hours.

---

### DEF-03 — Static Mutable Writable Field (CODE QUALITY — Both Mappers)

**Affects:** `TrafficHourMapper`, `TrafficWeatherMapper`

```java
// WRONG — shared across Mapper instances if JVM is reused
private final static IntWritable trafficValue = new IntWritable();

// CORRECT — instance field, one per Mapper instance
private IntWritable trafficValue = new IntWritable();
```

Hadoop is permitted to reuse the same JVM for multiple Mapper task attempts. A `static` mutable field is shared across all Mapper instances in that JVM, which creates a race condition in that scenario. The Hadoop programming guide explicitly recommends instance fields for output Writables.

---

### DEF-04 — Missing Combiner (PERFORMANCE — Both Drivers)

The `TrafficHourReducer` and `TrafficWeatherReducer` perform associative summation (`sum += val`). This makes them safe to use as Combiners — pre-aggregating map output on each node before the shuffle, significantly reducing network traffic for large datasets.

```java
// Add to both Drivers:
job.setCombinerClass(TrafficHourReducer.class);   // or TrafficWeatherReducer.class
```

> **Caveat:** Once DEF-01 is fixed and count-tracking is introduced, the Combiner must use a `SumCount` Writable so partial sums and counts merge correctly. A plain `IntWritable` Combiner cannot be added at that point without switching to a custom Writable.

---

### DEF-05 — No Formatted Final Report (PRESENTATION — Both Jobs)

**Specification:** "Final report generation"  
**Current behaviour:** Output is a raw tab-separated text file with no labels, no units, no averages, no peak/lowest annotations.

The final `part-r-00000` output should be a human-readable report. Example of expected output for the hourly job:

```
=== Traffic Volume by Hour Report ===
Hour  Total Volume    Avg Volume   Record Count
00    1,700,449       ...          ...
01    1,058,204       ...          ...
...
16    11,259,548      ...          ...   ← PEAK HOUR
...
03    751,459         ...          ...   ← LOWEST HOUR
=====================================
PEAK HOUR:   16:00  (11,259,548 total)
LOWEST HOUR: 03:00  (751,459 total)
```

This can be produced either by:
1. Writing the report header/footer from `setup()` / `cleanup()` in the Reducer
2. Adding a post-processing step using `MultipleOutputs` or a second job
3. Adding a Driver-level post-processing method that reads the output and formats it

---

## Required Code Modifications

### Modification 1 — TrafficHourReducer (Fixes DEF-01, DEF-02, DEF-05)

Replace the current reducer with:

```java
package com.traffic.mapreduce.hourly;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TrafficHourReducer
        extends Reducer<Text, IntWritable, Text, Text> {

    private String peakHour   = null;
    private String lowestHour = null;
    private long   peakSum    = Long.MIN_VALUE;
    private long   lowestSum  = Long.MAX_VALUE;

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {

        long sum = 0;
        int  count = 0;

        for (IntWritable val : values) {
            sum += val.get();
            count++;
        }

        long average = (count > 0) ? sum / count : 0;

        context.write(key, new Text(
            String.format("total=%-12d  avg=%-8d  count=%d",
                          sum, average, count)));

        if (sum > peakSum)   { peakSum   = sum;  peakHour   = key.toString(); }
        if (sum < lowestSum) { lowestSum = sum;  lowestHour = key.toString(); }
    }

    @Override
    protected void cleanup(Context context)
            throws IOException, InterruptedException {
        context.write(new Text("--- PEAK_HOUR ---"),
            new Text(peakHour   + "  total=" + peakSum));
        context.write(new Text("--- LOWEST_HOUR ---"),
            new Text(lowestHour + "  total=" + lowestSum));
    }
}
```

Also update the Driver output value type to `Text`:

```java
// In TrafficHourDriver.java:
job.setOutputKeyClass(Text.class);
job.setOutputValueClass(Text.class);       // changed from IntWritable
```

---

### Modification 2 — TrafficWeatherReducer (Fixes DEF-01, DEF-02, DEF-05)

Apply the same pattern:

```java
package com.traffic.mapreduce.weather;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TrafficWeatherReducer
        extends Reducer<Text, IntWritable, Text, Text> {

    private String peakWeather   = null;
    private String lowestWeather = null;
    private long   peakSum       = Long.MIN_VALUE;
    private long   lowestSum     = Long.MAX_VALUE;

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {

        long sum = 0;
        int  count = 0;

        for (IntWritable val : values) {
            sum += val.get();
            count++;
        }

        long average = (count > 0) ? sum / count : 0;

        context.write(key, new Text(
            String.format("total=%-12d  avg=%-8d  count=%d",
                          sum, average, count)));

        if (sum > peakSum)   { peakSum   = sum;  peakWeather   = key.toString(); }
        if (sum < lowestSum) { lowestSum = sum;  lowestWeather = key.toString(); }
    }

    @Override
    protected void cleanup(Context context)
            throws IOException, InterruptedException {
        context.write(new Text("--- HIGHEST_TRAFFIC_WEATHER ---"),
            new Text(peakWeather   + "  total=" + peakSum));
        context.write(new Text("--- LOWEST_TRAFFIC_WEATHER ---"),
            new Text(lowestWeather + "  total=" + lowestSum));
    }
}
```

Also update the Driver:

```java
// In TrafficWeatherDriver.java:
job.setOutputKeyClass(Text.class);
job.setOutputValueClass(Text.class);       // changed from IntWritable
```

---

### Modification 3 — Fix Static Writable Field (DEF-03 — Both Mappers)

In `TrafficHourMapper.java` and `TrafficWeatherMapper.java`:

```java
// Remove:
private final static IntWritable trafficValue = new IntWritable();

// Replace with:
private IntWritable trafficValue = new IntWritable();
```

---

### Modification 4 — Add Combiner for Pure SUM Phase (DEF-04)

> Only apply this if the reducers remain in `SUM`-only mode. Once average/count tracking is added (DEF-01 fix), skip this and use a `SumCount` Writable approach instead.

```java
// In TrafficHourDriver.java (SUM-only mode only):
job.setCombinerClass(TrafficHourReducer.class);

// In TrafficWeatherDriver.java (SUM-only mode only):
job.setCombinerClass(TrafficWeatherReducer.class);
```

---

## Complete Requirement Status Matrix

### Job 1 — Traffic Volume by Hour

| Requirement | Component | Status | Evidence |
|---|---|---|---|
| Extract hour from `date_time` | TrafficHourMapper | ✅ COMPLETE | `dateTime.substring(11, 13)` on `fields[7]`; HDFS output shows keys `00`–`23` |
| Aggregate (sum) traffic volume | TrafficHourReducer | ✅ COMPLETE | `sum += val.get()` loop; HDFS output shows correct totals |
| Compute average traffic per hour | TrafficHourReducer | ❌ MISSING | No `count` variable; no division; only `sum` is written |
| Identify peak hour | TrafficHourReducer | ❌ MISSING | No cross-key max tracking; no `cleanup()` |
| Identify lowest traffic hour | TrafficHourReducer | ❌ MISSING | No cross-key min tracking; no `cleanup()` |
| Skip CSV header row | TrafficHourMapper | ✅ COMPLETE | `line.startsWith("holiday")` guard |
| Handle malformed rows | TrafficHourMapper | ✅ COMPLETE | Silent `catch(Exception)` |
| Correct column indices | TrafficHourMapper | ✅ COMPLETE | `fields[7]` = `date_time`, `fields[8]` = `traffic_volume` |
| Job wired correctly | TrafficHourDriver | ✅ COMPLETE | Mapper, Reducer, types, paths all set; `waitForCompletion(true)` |
| Combiner configured | TrafficHourDriver | ❌ MISSING | No `setCombinerClass` call |
| Final formatted report | TrafficHourReducer | ❌ MISSING | Raw `hour\tsum` output only; no labels, no average, no peak annotation |

**Job 1 Score: 5 / 11 requirements met = 45%**

---

### Job 2 — Traffic Volume by Weather

| Requirement | Component | Status | Evidence |
|---|---|---|---|
| Aggregate by weather condition | TrafficWeatherReducer | ✅ COMPLETE | `sum += value.get()` loop; HDFS output shows 11 weather categories |
| Compute average traffic by weather | TrafficWeatherReducer | ❌ MISSING | No `count` variable; no division; only `sum` is written |
| Identify highest traffic weather | TrafficWeatherReducer | ❌ MISSING | No cross-key max; no `cleanup()` |
| Identify lowest traffic weather | TrafficWeatherReducer | ❌ MISSING | No cross-key min; no `cleanup()` |
| Skip CSV header row | TrafficWeatherMapper | ✅ COMPLETE | `line.startsWith("holiday")` guard |
| Handle malformed rows | TrafficWeatherMapper | ✅ COMPLETE | Silent `catch(Exception)` |
| Correct column indices | TrafficWeatherMapper | ✅ COMPLETE | `fields[5]` = `weather_main`, `fields[8]` = `traffic_volume` |
| Job wired correctly | TrafficWeatherDriver | ✅ COMPLETE | Mapper, Reducer, types, paths all set; `waitForCompletion(true)` |
| Combiner configured | TrafficWeatherDriver | ❌ MISSING | No `setCombinerClass` call |
| Final formatted report | TrafficWeatherReducer | ❌ MISSING | Raw `weather\tsum` output only; no average, no highest/lowest annotation |

**Job 2 Score: 4 / 10 requirements met = 40%**

---

## What the Current Output Actually Tells Us

From the HDFS output, the correct answers CAN be derived manually — proving the sum logic is right:

**Hourly Analysis — Derivable from existing output:**

| Metric | Value | Hour |
|---|---|---|
| Peak Hour (by total volume) | 11,259,548 | **16:00** |
| Lowest Traffic Hour | 751,459 | **03:00** |
| Average (sum / 24 hours) | ~6,019,640 | — |
| *Note: true per-record average needs record count, not available* | | |

**Weather Analysis — Derivable from existing output:**

| Metric | Value | Weather |
|---|---|---|
| Highest Traffic Weather | 54,870,172 | **Clouds** |
| Lowest Traffic Weather | 8,247 | **Squall** |
| *Note: true per-record average needs record count, not available* | | |

The values are correct — the computation logic is sound. Only the **reporting and annotation layer** is missing.

---

## Summary

| Category | Implemented | Missing | % |
|---|---|---|---|
| CSV parsing (both jobs) | ✅ Both correct | — | 100% |
| Hour extraction | ✅ Correct | — | 100% |
| Weather extraction | ✅ Correct | — | 100% |
| Traffic volume summation (both) | ✅ Both correct | — | 100% |
| Average calculation (both) | — | ❌ Both missing | 0% |
| Peak identification (hourly) | — | ❌ Missing | 0% |
| Lowest identification (hourly) | — | ❌ Missing | 0% |
| Highest identification (weather) | — | ❌ Missing | 0% |
| Lowest identification (weather) | — | ❌ Missing | 0% |
| Formatted final report (both) | — | ❌ Both missing | 0% |
| Driver job wiring (both) | ✅ Both correct | — | 100% |
| Combiner optimization (both) | — | ❌ Both missing | 0% |
| Static field bug (both mappers) | — | ⚠️ Both affected | 0% |
| **TOTAL** | **7 / 17** | **10 / 17** | **41%** |

### Priority Fix Order

1. **[HIGH]** Add `count` tracking to both Reducers and compute average — DEF-01  
2. **[HIGH]** Add `cleanup()` to both Reducers for peak/lowest identification — DEF-02  
3. **[HIGH]** Change `setOutputValueClass` to `Text.class` in both Drivers after Reducer changes  
4. **[MEDIUM]** Fix static mutable Writable to instance field in both Mappers — DEF-03  
5. **[LOW]** Add Combiner to both Drivers (only in SUM-only mode, not after DEF-01 fix without SumCount Writable) — DEF-04  
6. **[LOW]** Add report header/footer via `setup()`/`cleanup()` in Reducers — DEF-05
