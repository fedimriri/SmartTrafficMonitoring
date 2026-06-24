# SmartTrafficMonitoring — Architecture Diagram

## System Architecture

```mermaid
flowchart TD
    DS[(Metro Interstate\nTraffic Volume\nCSV Dataset\n48 204 records)]

    subgraph HDFS ["HDFS — Distributed Storage"]
        direction TB
        HI[/traffic-data/historical/\nMetro_Interstate_Traffic_Volume.csv]
        HO_H[/traffic-data/output/hourly/\npart-r-00000]
        HO_W[/traffic-data/output/weather/\npart-r-00000]
        HS[/traffic-data/streaming/\nincoming-data/]
    end

    subgraph HADOOP ["Hadoop Cluster — YARN + MapReduce"]
        direction TB
        subgraph JOB1 ["Job 1 — Traffic by Hour"]
            M1[TrafficHourMapper\nkey: hour HH\nvalue: traffic_volume]
            R1[TrafficHourReducer\navg per hour\n+ PEAK_HOUR\n+ LOWEST_HOUR]
        end
        subgraph JOB2 ["Job 2 — Traffic by Weather"]
            M2[TrafficWeatherMapper\nkey: weather_main\nvalue: traffic_volume]
            R2[TrafficWeatherReducer\navg per condition\n+ HIGHEST_WEATHER\n+ LOWEST_WEATHER]
        end
    end

    subgraph SPARK ["Spark Cluster — Standalone spark://spark-master:7077"]
        direction TB
        PROD[TrafficDataProducer\nServerSocket :9999\nCSV → stream records]
        subgraph APP ["TrafficStreamingApp — 5 s micro-batches"]
            REC[socketTextStream\nReceiver]
            ALT[Congestion Filter\nvolume > 6000]
            AVG[updateStateByKey\nRunning Average]
            WIN[reduceByKeyAndWindow\n30 s / 5 s Weather Agg]
        end
    end

    subgraph REPORTS ["Reports & Alerts"]
        R_H[Hourly Traffic Report\n00: 834  vehicles/h\n...\nPEAK: 16h  5663 v/h\nLOWEST: 03h  371 v/h]
        R_W[Weather Traffic Report\nClouds: 3618 v/h\n...\nHIGHEST: Clouds\nLOWEST: Squall]
        R_A[Real-time Alerts\n⚠ CONGESTION ALERT\nRunning Avg Stats\nWeather Window Stats]
    end

    DS -->|"hdfs dfs -put"| HI
    HI --> M1
    HI --> M2
    M1 --> R1
    M2 --> R2
    R1 --> HO_H
    R2 --> HO_W
    HO_H --> R_H
    HO_W --> R_W

    DS -->|"local CSV read"| PROD
    PROD -->|TCP :9999| REC
    REC --> ALT
    REC --> AVG
    REC --> WIN
    ALT --> R_A
    AVG --> R_A
    WIN --> R_A
```

---

## Data Flow — Batch Pipeline

```
CSV Dataset (48 204 rows)
        │
        ▼  hdfs dfs -put
┌─────────────────────────────────┐
│  HDFS /traffic-data/historical/ │
└─────────────────────────────────┘
        │
        ▼  hadoop jar → YARN ResourceManager → 2 NodeManagers
┌───────────────────────────────────────────────────────────────┐
│  MapReduce Job 1: TrafficHourDriver                           │
│   Mapper:  (date_time col 7)  → emit (HH, traffic_volume)    │
│   Reducer: group by hour      → avg; track peak/lowest        │
└───────────────────────────────────────────────────────────────┘
        │
        ▼
HDFS /traffic-data/output/hourly/part-r-00000
        │  (24 hour averages + PEAK_HOUR + LOWEST_HOUR)
        ▼

┌───────────────────────────────────────────────────────────────┐
│  MapReduce Job 2: TrafficWeatherDriver                        │
│   Mapper:  (weather_main col 5) → emit (weather, volume)     │
│   Reducer: group by condition  → avg; track highest/lowest   │
└───────────────────────────────────────────────────────────────┘
        │
        ▼
HDFS /traffic-data/output/weather/part-r-00000
        (11 weather categories + HIGHEST_WEATHER + LOWEST_WEATHER)
```

---

## Data Flow — Streaming Pipeline

```
CSV Dataset (local file)
        │
        ▼  reads row by row, 500 ms/record
┌─────────────────────────────────────┐
│  TrafficDataProducer                │
│  ServerSocket on :9999              │
│  Converts CSV → "datetime,wx,vol"  │
└─────────────────────────────────────┘
        │ TCP socket
        ▼
┌─────────────────────────────────────────────────────────────────┐
│  TrafficStreamingApp   (Spark Standalone spark://spark-master)  │
│  Batch interval: 5 seconds                                      │
│                                                                 │
│  1. Congestion Detection                                        │
│     filter(volume > 6000) → print CONGESTION ALERT             │
│                                                                 │
│  2. Running Global Average                                      │
│     mapToPair("GLOBAL", volume)                                 │
│     → updateStateByKey(sum, count)                              │
│     → print running avg every batch                             │
│                                                                 │
│  3. Weather Window Aggregation                                  │
│     mapToPair(weather, volume)                                  │
│     → reduceByKeyAndWindow(sum, 30s window, 5s slide)           │
│     → print weather totals for last 30 s                        │
└─────────────────────────────────────────────────────────────────┘
        │
        ▼
Console Alerts & Statistics (stdout)
```

---

## Docker Network Topology

```
Docker Network: hadoop (172.18.0.0/16)
┌──────────────────────────────────────────────────────────┐
│                                                          │
│  hadoop-master   hadoop-worker1   hadoop-worker2         │
│  (NameNode       (DataNode        (DataNode              │
│   SecondaryNN     NodeManager)     NodeManager)          │
│   ResManager                                             │
│   NodeManager)                                           │
│                                                          │
│  spark-master    spark-slave1     spark-slave2           │
│  (Spark Master)  (Spark Worker)   (Spark Worker)         │
│  port 7077                                               │
│  port 8080                                               │
│  port 9999 ◄── TrafficDataProducer socket               │
│                                                          │
└──────────────────────────────────────────────────────────┘
```

---

## Component Summary

| Component | Role | Technology |
|---|---|---|
| `TrafficHourMapper` | Extract `(hour, volume)` pairs | Hadoop MapReduce |
| `TrafficHourReducer` | Average per hour + peak/lowest | Hadoop MapReduce |
| `TrafficWeatherMapper` | Extract `(weather, volume)` pairs | Hadoop MapReduce |
| `TrafficWeatherReducer` | Average per condition + extremes | Hadoop MapReduce |
| `TrafficDataProducer` | Simulate live stream from CSV | Java ServerSocket |
| `TrafficStreamingApp` | Real-time analysis + alerts | Spark Streaming 2.4.5 |
| HDFS | Distributed storage for input/output | Hadoop 3.3.6 |
| YARN | Cluster resource management | Hadoop 3.3.6 |
