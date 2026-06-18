# SmartTrafficMonitoring

[![Java](https://img.shields.io/badge/Java-8-blue.svg)](https://www.oracle.com/java/)
[![Hadoop](https://img.shields.io/badge/Hadoop-3.3.6-orange.svg)](https://hadoop.apache.org/)
[![Spark Streaming](https://img.shields.io/badge/Spark%20Streaming-2.4.5-red.svg)](https://spark.apache.org/streaming/)
[![Maven](https://img.shields.io/badge/Maven-Build-C71A36.svg)](https://maven.apache.org/)

SmartTrafficMonitoring is a university big data project that analyzes road traffic data using two complementary approaches:

- **Batch processing with Hadoop MapReduce** for historical traffic analysis
- **Real-time processing with Apache Spark Streaming** for live traffic monitoring

The project uses the Metro Interstate Traffic Volume dataset for batch analysis and a socket-based input stream for real-time traffic events.

## Quick Start

1. Build the project with `mvn clean package`.
2. Run one of the Hadoop MapReduce drivers with an input path and output path.
3. Start a socket source on port `9999`.
4. Launch `TrafficStreamingApp` and send comma-separated traffic events into the socket.
5. Review the console output for high-traffic alerts and weather summaries.

## Project Goals

- Analyze historical traffic volume stored in HDFS or local filesystem input.
- Aggregate traffic by **hour**.
- Aggregate traffic by **weather condition**.
- Detect high traffic events in a streaming pipeline.
- Provide live traffic summaries from incoming data.

## Technologies Used

- Java 8
- Apache Hadoop MapReduce 3.3.6
- Apache Spark Streaming 2.4.5
- Maven

## Repository Structure

```text
src/main/java/com/traffic/
├── App.java
├── mapreduce/
│   ├── hourly/
│   │   ├── TrafficHourDriver.java
│   │   ├── TrafficHourMapper.java
│   │   └── TrafficHourReducer.java
│   └── weather/
│       ├── TrafficWeatherDriver.java
│       ├── TrafficWeatherMapper.java
│       └── TrafficWeatherReducer.java
└── streaming/
		└── TrafficStreamingApp.java

dataset/
└── Metro_Interstate_Traffic_Volume.csv
```

## Dataset

The batch jobs use `dataset/Metro_Interstate_Traffic_Volume.csv`.

### CSV Schema

The file contains these columns:

```text
holiday,temp,rain_1h,snow_1h,clouds_all,weather_main,weather_description,date_time,traffic_volume
```

### Fields Used by the MapReduce Jobs

- **Hourly analysis** uses:
	- `date_time` at column index 7
	- `traffic_volume` at column index 8
- **Weather analysis** uses:
	- `weather_main` at column index 5
	- `traffic_volume` at column index 8

## Features Implemented

### 1. Batch Analysis with Hadoop MapReduce

#### Traffic by Hour

- `TrafficHourMapper` extracts the hour from `date_time`.
- `TrafficHourReducer` sums traffic volume per hour.
- `TrafficHourDriver` runs the job and writes output to the path provided at runtime.

#### Traffic by Weather

- `TrafficWeatherMapper` extracts the weather category from `weather_main`.
- `TrafficWeatherReducer` sums traffic volume per weather condition.
- `TrafficWeatherDriver` runs the job and writes output to the path provided at runtime.

### 2. Real-Time Analysis with Spark Streaming

- `TrafficStreamingApp` creates a `JavaStreamingContext`.
- Reads incoming traffic lines from a socket at `localhost:9999`.
- Parses streaming events into a simple `TrafficEvent` object.
- Detects high traffic when `trafficVolume > 8000`.
- Produces a live summary grouped by weather condition.

## Real-Time Input Format

The streaming app expects each line to contain **three comma-separated values**:

```text
timestamp,weather,trafficVolume
```

### Example

```text
2026-06-18 08:00:00,Clouds,9100
2026-06-18 08:05:00,Clear,4300
2026-06-18 08:10:00,Rain,12050
```

You can generate test data with `nc` or another socket producer.

## Build Requirements

- JDK 8
- Maven
- Hadoop libraries available at compile/run time for MapReduce jobs
- Spark compatible runtime for the streaming application

## Screenshots

Add your project screenshots here to make the report more presentation-ready:

- Batch analysis output
- Weather summary output
- High traffic alert output

Suggested folder structure:

```text
docs/screenshots/
├── hourly-output.png
├── weather-output.png
└── streaming-alerts.png
```

## Build the Project

```bash
mvn clean package
```

This generates a runnable JAR in `target/`.

## Command-by-Command Execution

### Step 1: Compile

```bash
mvn clean package
```

### Step 2: Run Hourly Batch Analysis

```bash
mvn exec:java \
	-Dexec.mainClass="com.traffic.mapreduce.hourly.TrafficHourDriver" \
	-Dexec.args="dataset/Metro_Interstate_Traffic_Volume.csv output/hourly"
```

### Step 3: Run Weather Batch Analysis

```bash
mvn exec:java \
	-Dexec.mainClass="com.traffic.mapreduce.weather.TrafficWeatherDriver" \
	-Dexec.args="dataset/Metro_Interstate_Traffic_Volume.csv output/weather"
```

### Step 4: Start a Socket Producer

```bash
nc -lk 9999
```

### Step 5: Run Spark Streaming

```bash
mvn exec:java -Dexec.mainClass="com.traffic.streaming.TrafficStreamingApp"
```

### Step 6: Send Live Events

```text
2026-06-18 08:00:00,Clouds,9100
2026-06-18 08:05:00,Clear,4300
2026-06-18 08:10:00,Rain,12050
```

## Run the Hadoop MapReduce Jobs

### 1. Traffic Volume by Hour

```bash
mvn exec:java \
	-Dexec.mainClass="com.traffic.mapreduce.hourly.TrafficHourDriver" \
	-Dexec.args="<input> <output>"
```

Example:

```bash
mvn exec:java \
	-Dexec.mainClass="com.traffic.mapreduce.hourly.TrafficHourDriver" \
	-Dexec.args="dataset/Metro_Interstate_Traffic_Volume.csv output/hourly"
```

### 2. Traffic Volume by Weather

```bash
mvn exec:java \
	-Dexec.mainClass="com.traffic.mapreduce.weather.TrafficWeatherDriver" \
	-Dexec.args="<input> <output>"
```

Example:

```bash
mvn exec:java \
	-Dexec.mainClass="com.traffic.mapreduce.weather.TrafficWeatherDriver" \
	-Dexec.args="dataset/Metro_Interstate_Traffic_Volume.csv output/weather"
```

### MapReduce Output

The reducers emit key-value pairs such as:

```text
08    125340
09    143220
Clear 982340
Clouds 1142200
```

## Run the Spark Streaming App

### Start a socket source

Use `nc` to simulate a stream:

```bash
nc -lk 9999
```

### Launch the streaming application

```bash
mvn exec:java -Dexec.mainClass="com.traffic.streaming.TrafficStreamingApp"
```

Then type streaming records into the socket terminal.

## Project Architecture

```text
Historical traffic CSV
				↓
MapReduce batch processing
				↓
Hourly and weather traffic summaries

Live traffic events from socket
				↓
Spark Streaming processing
				↓
High traffic alerts and weather-based aggregation
```

## Current Scope

This project currently includes:

- Historical traffic aggregation by hour
- Historical traffic aggregation by weather condition
- Streaming ingestion from a socket
- High traffic alert detection
- Live weather-based traffic aggregation

## Limitations

- The streaming app uses a socket source, not Kafka.
- The streaming input format is simplified and does not reuse the full CSV schema.
- There is no persistence layer for processed streaming results.
- There is no dashboard or web UI.

## Future Improvements

- Add Kafka ingestion for the streaming pipeline.
- Persist MapReduce outputs into HDFS or a warehouse layer.
- Add windowed and stateful Spark Streaming analytics.
- Create a dashboard for visualizing traffic trends and alerts.
- Support richer traffic event schemas and sensor simulation.

## Author

Fadi Mriri | Cloud & DevOps Engineer

- Email: fmriri2@gmail.com
- Location: Tunis, Tunis
- Portfolio: fadimriri-portfolio.vercel.app

