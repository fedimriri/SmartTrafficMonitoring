package com.traffic.streaming;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;

/**
 * Simulates a traffic sensor network by streaming records over a TCP socket.
 *
 * Two modes:
 *
 *   Automatic mode (default): reads the historical CSV row by row and sends
 *   each record at a configurable rate. Used by start-producer.sh.
 *
 *     args[0]  path to CSV dataset  (default: dataset/Metro_Interstate_Traffic_Volume.csv)
 *     args[1]  TCP port             (default: 9999)
 *     args[2]  delay between records in ms  (default: 500)
 *
 *   Interactive mode: reads records from stdin (one per line) so the user can
 *   type them manually during a live demo. Used by start-manual-demo.sh.
 *
 *     args[0]  --interactive
 *     args[1]  TCP port  (default: 9999)
 *
 * In both modes this class is the TCP SERVER. The Spark Streaming application
 * acts as the TCP CLIENT via socketTextStream / ReceiverTask.
 *
 * Wire format per line:
 *   timestamp,weather,trafficVolume
 *   Example: 2012-10-02 09:00:00,Clouds,5545
 */
public class TrafficDataProducer {

    public static void main(String[] args) throws Exception {
        if (args.length > 0 && "--interactive".equals(args[0])) {
            int port = args.length > 1 ? Integer.parseInt(args[1]) : 9999;
            runInteractive(port);
        } else {
            runAutomatic(args);
        }
    }

    // ── Interactive mode ──────────────────────────────────────────────────────
    // Reads wire-format records from stdin and forwards them to the Spark client.
    private static void runInteractive(int port) throws Exception {
        System.out.println("========================================");
        System.out.println("  Smart Traffic Monitoring");
        System.out.println("  Manual Demo Mode");
        System.out.println("========================================");
        System.out.println();
        System.out.println("  Port    : " + port);
        System.out.println("  Format  : timestamp,weather,traffic_volume");
        System.out.println("  Example : 2026-01-01 08:00:00,Rain,8500");
        System.out.println();
        System.out.println("  Waiting for Spark Streaming to connect...");
        System.out.println("========================================");

        try (ServerSocket serverSocket = new ServerSocket(port)) {
            Socket clientSocket = serverSocket.accept();
            System.out.println();
            System.out.println("[CONNECTED] Spark connected from "
                    + clientSocket.getInetAddress());
            System.out.println();
            System.out.println("  Type a record and press Enter.");
            System.out.println("  Spark processes each record within 5 seconds.");
            System.out.println("  Volume > 6000 triggers a CONGESTION ALERT.");
            System.out.println("  Press Ctrl-C to stop.");
            System.out.println("----------------------------------------");
            System.out.println();

            try (PrintWriter out = new PrintWriter(
                         new OutputStreamWriter(
                                 clientSocket.getOutputStream(),
                                 StandardCharsets.UTF_8), true);
                 BufferedReader stdin = new BufferedReader(
                         new InputStreamReader(System.in, StandardCharsets.UTF_8))) {

                String line;
                while ((line = stdin.readLine()) != null) {
                    line = line.trim();
                    if (line.isEmpty() || line.startsWith("#")) {
                        continue;
                    }
                    String error = validateWireFormat(line);
                    if (error != null) {
                        System.out.println("[ERROR] " + error);
                        System.out.println("        Format  : timestamp,weather,traffic_volume");
                        System.out.println("        Example : 2026-01-01 08:00:00,Rain,8500");
                        continue;
                    }
                    out.println(line);
                    System.out.println("[SENT]  " + line
                            + "  -> Spark will process it within 5 s");
                }

            } catch (IOException e) {
                System.out.println("[ERROR] Connection to Spark lost: " + e.getMessage());
                System.out.println("        Restart Spark Streaming and run this script again.");
            }
        }

        System.out.println();
        System.out.println("[DONE] Manual demo session ended.");
    }

    // Validate the 3-field wire format sent by automatic mode and accepted by StreamingApp.
    // Returns null on success, an error message on failure.
    private static String validateWireFormat(String line) {
        String[] parts = line.split(",");
        if (parts.length < 3) {
            return "Expected 3 comma-separated fields, got " + parts.length + " in: " + line;
        }
        try {
            Integer.parseInt(parts[2].trim());
        } catch (NumberFormatException e) {
            return "traffic_volume must be an integer, got: \"" + parts[2].trim() + "\"";
        }
        return null;
    }

    // ── Automatic mode (original CSV replay) ─────────────────────────────────
    private static void runAutomatic(String[] args) throws Exception {
        String csvPath = args.length > 0
                ? args[0]
                : "dataset/Metro_Interstate_Traffic_Volume.csv";

        int port = args.length > 1
                ? Integer.parseInt(args[1])
                : 9999;

        long delayMs = args.length > 2
                ? Long.parseLong(args[2])
                : 500L;

        System.out.println("========================================");
        System.out.println("  Smart Traffic Monitoring — Producer");
        System.out.println("========================================");
        System.out.println("Dataset : " + csvPath);
        System.out.println("Port    : " + port);
        System.out.println("Delay   : " + delayMs + " ms / record");
        System.out.println("Waiting for Spark Streaming to connect...");
        System.out.println("========================================");

        try (ServerSocket serverSocket = new ServerSocket(port)) {

            // Loop so that if Spark reconnects (e.g. after restart), the producer
            // keeps serving without requiring a manual restart.
            while (true) {
                Socket clientSocket = serverSocket.accept();
                System.out.println("[PRODUCER] Spark connected from "
                        + clientSocket.getInetAddress());

                try (PrintWriter out = new PrintWriter(
                             new OutputStreamWriter(
                                     clientSocket.getOutputStream(),
                                     StandardCharsets.UTF_8), true);
                     BufferedReader reader = Files.newBufferedReader(
                             Paths.get(csvPath), StandardCharsets.UTF_8)) {

                    String line;
                    int sent = 0;

                    while ((line = reader.readLine()) != null) {
                        TrafficRecord record = TrafficRecord.fromCsvLine(line);
                        if (record == null) {
                            continue; // skip header and malformed rows
                        }
                        out.println(record.toStreamFormat());
                        sent++;
                        if (sent % 20 == 0) {
                            System.out.printf("[PRODUCER] Sent %5d records | Last: %s%n",
                                    sent, record.toStreamFormat());
                        }
                        Thread.sleep(delayMs);
                    }

                    System.out.println("[PRODUCER] Dataset exhausted. Total records sent: " + sent);
                    System.out.println("[PRODUCER] Waiting for next connection...");

                } catch (IOException e) {
                    System.out.println("[PRODUCER] Client disconnected: " + e.getMessage());
                }
            }
        }
    }
}
