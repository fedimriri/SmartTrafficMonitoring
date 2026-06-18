package com.traffic.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;
import java.util.List;

public class TrafficStreamingApp {

        public static void main(String[] args) throws Exception {

                // 1. Spark configuration
                SparkConf conf = new SparkConf()
                                .setAppName("SmartTrafficStreaming")
                                .setMaster("local[*]"); // change to spark:// for cluster mode

                // 2. Streaming context (batch every 5 seconds)
                JavaStreamingContext ssc = new JavaStreamingContext(conf, Durations.seconds(5));

                // 3. Input stream (Socket)
                JavaReceiverInputDStream<String> stream = ssc.socketTextStream("localhost", 9999);

                // 4. Parse input safely
                JavaDStream<TrafficEvent> events = stream

                                // Debug: show every received line
                                .filter(line -> {
                                        System.out.println("Received: [" + line + "]");
                                        return true;
                                })

                                // Ignore null or empty lines
                                .filter(line -> line != null && !line.trim().isEmpty())

                                // Validate format
                                .filter(line -> {
                                        String[] parts = line.split(",");

                                        if (parts.length != 3) {
                                                System.out.println(
                                                                "INVALID LINE -> [" + line + "]");
                                                return false;
                                        }

                                        return true;
                                })

                                // Convert to TrafficEvent
                                .map(line -> {
                                        String[] parts = line.split(",");

                                        return new TrafficEvent(
                                                        parts[0].trim(),
                                                        parts[1].trim(),
                                                        Integer.parseInt(parts[2].trim()));
                                });

                // 5. ALERT: high traffic detection
                events.filter(e -> e.trafficVolume > 8000)
                                .foreachRDD(rdd -> {
                                        rdd.foreach(e -> System.out.println(
                                                        "\n ************************************************************************************ HIGH TRAFFIC ALERT ************************************************************************************ \n [ALERT] HIGH TRAFFIC ALERT: "
                                                                        + e.timestamp
                                                                        + " -> "
                                                                        + e.trafficVolume));
                                });

                // 6. WEATHER-based aggregation

                events.mapToPair(event -> new Tuple2<String, Integer>(
                                event.weather,
                                event.trafficVolume))
                                .reduceByKey((a, b) -> a + b)
                                .foreachRDD(rdd -> {

                                        System.out.println("\n===== WEATHER TRAFFIC SUMMARY =====");

                                        rdd.foreach(record -> System.out.println(
                                                        record._1() + " => " + record._2()));
                                });

                // 7. Start streaming
                ssc.start();
                ssc.awaitTermination();
        }

        // Simple POJO
        public static class TrafficEvent {

                public String timestamp;
                public String weather;
                public int trafficVolume;

                public TrafficEvent(String t, String w, int v) {
                        this.timestamp = t;
                        this.weather = w;
                        this.trafficVolume = v;
                }
        }
}