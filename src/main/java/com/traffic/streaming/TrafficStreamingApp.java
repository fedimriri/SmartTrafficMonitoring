package com.traffic.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * Smart Traffic Monitoring — Real-Time Processing Module
 *
 * Connects to a socket stream produced by TrafficDataProducer and performs:
 *   1. Congestion detection       (threshold: traffic_volume > 6000)
 *   2. Running traffic average    (stateful — updateStateByKey)
 *   3. Weather-based aggregation  (windowed — reduceByKeyAndWindow 30s/5s)
 *
 * Usage (local mode — run from inside spark-master container):
 *   spark-submit --master local[2] \
 *     --class com.traffic.streaming.TrafficStreamingApp \
 *     /tmp/SmartTrafficMonitoring.jar
 *
 * Usage (cluster mode):
 *   spark-submit --master spark://spark-master:7077 \
 *     --class com.traffic.streaming.TrafficStreamingApp \
 *     /tmp/SmartTrafficMonitoring.jar \
 *     spark://spark-master:7077  hadoop-master  /tmp/spark-checkpoint-traffic
 *
 * Arguments (all optional):
 *   args[0]  Spark master URL       (default: local[2])
 *   args[1]  Socket host            (default: localhost)
 *   args[2]  Checkpoint directory   (default: /tmp/spark-checkpoint-traffic)
 *
 * NOTE: All stream transformations use explicit anonymous inner classes instead of
 * Java lambda expressions. This is required for compatibility with Spark 2.4.5 when
 * running on a JVM newer than Java 8: Java 9+ generates lambda proxy classes at
 * runtime with the host JVM's class file major version (e.g. 55 for Java 11), which
 * Spark 2.4.5's bundled ASM 5.x library cannot parse. Anonymous inner classes are
 * compiled at build time to Java 8 bytecode (major version 52), which ASM 5.x
 * fully supports.
 */
public class TrafficStreamingApp {

    // Congestion threshold per project specification
    private static final int CONGESTION_THRESHOLD = 6000;

    public static void main(String[] args) throws Exception {

        final String master        = args.length > 0 ? args[0] : "local[2]";
        final String socketHost    = args.length > 1 ? args[1] : "localhost";
        final String checkpointDir = args.length > 2 ? args[2] : "/tmp/spark-checkpoint-traffic";

        System.out.println("========================================");
        System.out.println("  Smart Traffic Monitoring - Streaming");
        System.out.println("========================================");
        System.out.printf("Master       : %s%n", master);
        System.out.printf("Socket       : %s:9999%n", socketHost);
        System.out.printf("Checkpoint   : %s%n", checkpointDir);
        System.out.printf("Alert threshold: traffic_volume > %d%n", CONGESTION_THRESHOLD);
        System.out.println("========================================");

        SparkConf conf = new SparkConf()
                .setAppName("SmartTrafficMonitoring")
                .setMaster(master);

        // 5-second micro-batch interval
        JavaStreamingContext ssc = new JavaStreamingContext(conf, Durations.seconds(5));

        // Checkpoint is required for updateStateByKey (stateful processing)
        ssc.checkpoint(checkpointDir);

        // ── Input stream ──────────────────────────────────────────────────
        JavaReceiverInputDStream<String> rawStream =
                ssc.socketTextStream(socketHost, 9999);

        // ── Non-empty line filter ─────────────────────────────────────────
        JavaDStream<String> validLines = rawStream.filter(
                new Function<String, Boolean>() {
                    private static final long serialVersionUID = 1L;
                    @Override
                    public Boolean call(String line) {
                        return line != null && !line.trim().isEmpty();
                    }
                });

        // ── Event parsing (fault-tolerant) ────────────────────────────────
        // flatMap so a malformed record skips the entry rather than failing
        // the entire micro-batch (NumberFormatException safe).
        JavaDStream<TrafficEvent> events = validLines.flatMap(
                new FlatMapFunction<String, TrafficEvent>() {
                    private static final long serialVersionUID = 1L;
                    @Override
                    public Iterator<TrafficEvent> call(String line) {
                        try {
                            String[] parts = line.split(",");
                            if (parts.length < 3) {
                                return Collections.<TrafficEvent>emptyIterator();
                            }
                            int volume = Integer.parseInt(parts[2].trim());
                            TrafficEvent e = new TrafficEvent(
                                    parts[0].trim(),
                                    parts[1].trim(),
                                    volume);
                            return Arrays.asList(e).iterator();
                        } catch (NumberFormatException ex) {
                            return Collections.<TrafficEvent>emptyIterator();
                        }
                    }
                });

        // ── 1. Congestion Detection ───────────────────────────────────────
        // Spec: traffic_volume > 6000 triggers an alert
        events.filter(new Function<TrafficEvent, Boolean>() {
                    private static final long serialVersionUID = 1L;
                    @Override
                    public Boolean call(TrafficEvent e) {
                        return e.trafficVolume > CONGESTION_THRESHOLD;
                    }
                })
                .foreachRDD(new VoidFunction<JavaRDD<TrafficEvent>>() {
                    private static final long serialVersionUID = 1L;
                    @Override
                    public void call(JavaRDD<TrafficEvent> rdd) {
                        List<TrafficEvent> alerts = rdd.collect();
                        for (TrafficEvent e : alerts) {
                            System.out.println();
                            System.out.println("+--------------------------------------+");
                            System.out.println("|       ** CONGESTION ALERT **         |");
                            System.out.println("+--------------------------------------+");
                            System.out.printf( "|  Time    : %-26s|%n", e.timestamp);
                            System.out.printf( "|  Weather : %-26s|%n", e.weather);
                            System.out.printf( "|  Volume  : %-26d|%n", e.trafficVolume);
                            System.out.println("+--------------------------------------+");
                        }
                    }
                });

        // ── 2. Running Traffic Average (stateful) ─────────────────────────
        // State: long[]{cumulativeSum, cumulativeCount}
        // Key is fixed to "GLOBAL" so we maintain one global running average.
        Function2<List<Integer>, Optional<long[]>, Optional<long[]>> updateAvgFn =
                new Function2<List<Integer>, Optional<long[]>, Optional<long[]>>() {
                    private static final long serialVersionUID = 1L;
                    @Override
                    public Optional<long[]> call(List<Integer> newValues, Optional<long[]> state) {
                        long sum   = state.isPresent() ? state.get()[0] : 0L;
                        long count = state.isPresent() ? state.get()[1] : 0L;
                        for (Integer v : newValues) {
                            sum   += v;
                            count += 1;
                        }
                        return Optional.of(new long[]{sum, count});
                    }
                };

        events.mapToPair(new PairFunction<TrafficEvent, String, Integer>() {
                    private static final long serialVersionUID = 1L;
                    @Override
                    public Tuple2<String, Integer> call(TrafficEvent e) {
                        return new Tuple2<String, Integer>("GLOBAL", e.trafficVolume);
                    }
                })
                .updateStateByKey(updateAvgFn)
                .foreachRDD(new VoidFunction<JavaPairRDD<String, long[]>>() {
                    private static final long serialVersionUID = 1L;
                    @Override
                    public void call(JavaPairRDD<String, long[]> rdd) {
                        List<Tuple2<String, long[]>> stats = rdd.collect();
                        for (Tuple2<String, long[]> pair : stats) {
                            long sum   = pair._2()[0];
                            long count = pair._2()[1];
                            if (count > 0) {
                                long avg = sum / count;
                                System.out.printf(
                                    "[STATS] Running Average Traffic : %d vehicles/hour" +
                                    "  (total processed: %d records)%n",
                                    avg, count);
                            }
                        }
                    }
                });

        // ── 3. Weather-Based Aggregation (windowed) ───────────────────────
        // 30-second window, 5-second slide — matches the 5s batch interval.
        JavaPairDStream<String, Integer> weatherVolume =
                events.mapToPair(new PairFunction<TrafficEvent, String, Integer>() {
                    private static final long serialVersionUID = 1L;
                    @Override
                    public Tuple2<String, Integer> call(TrafficEvent e) {
                        return new Tuple2<String, Integer>(e.weather, e.trafficVolume);
                    }
                });

        weatherVolume
                .reduceByKeyAndWindow(
                        new Function2<Integer, Integer, Integer>() {
                            private static final long serialVersionUID = 1L;
                            @Override
                            public Integer call(Integer a, Integer b) {
                                return a + b;
                            }
                        },
                        Durations.seconds(30),
                        Durations.seconds(5))
                .foreachRDD(new VoidFunction<JavaPairRDD<String, Integer>>() {
                    private static final long serialVersionUID = 1L;
                    @Override
                    public void call(JavaPairRDD<String, Integer> rdd) {
                        List<Tuple2<String, Integer>> results = rdd.collect();
                        if (!results.isEmpty()) {
                            System.out.println("[WEATHER] Traffic by condition (last 30 s):");
                            for (Tuple2<String, Integer> r : results) {
                                System.out.printf("  %-20s -> %d vehicles%n",
                                        r._1(), r._2());
                            }
                        }
                    }
                });

        // ── Start ─────────────────────────────────────────────────────────
        ssc.start();
        ssc.awaitTermination();
    }

    // ── Inner POJO ────────────────────────────────────────────────────────
    public static class TrafficEvent implements java.io.Serializable {

        private static final long serialVersionUID = 1L;

        public final String timestamp;
        public final String weather;
        public final int    trafficVolume;

        public TrafficEvent(String timestamp, String weather, int trafficVolume) {
            this.timestamp     = timestamp;
            this.weather       = weather;
            this.trafficVolume = trafficVolume;
        }

        @Override
        public String toString() {
            return timestamp + "," + weather + "," + trafficVolume;
        }
    }
}
