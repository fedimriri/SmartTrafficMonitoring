package com.traffic.streaming;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;

/**
 * Simulates a traffic sensor network by reading the historical dataset
 * row by row and streaming records over a TCP socket.
 *
 * The Spark Streaming application acts as the client (socketTextStream).
 * This producer acts as the server (ServerSocket on port 9999).
 *
 * Wire format per line:
 *   timestamp,weather,trafficVolume
 *   Example: 2012-10-02 09:00:00,Clouds,5545
 *
 * Usage:
 *   mvn exec:java -Dexec.mainClass="com.traffic.streaming.TrafficDataProducer" \
 *                 -Dexec.args="dataset/Metro_Interstate_Traffic_Volume.csv 9999 500"
 *
 * Arguments (all optional):
 *   args[0]  path to CSV dataset  (default: dataset/Metro_Interstate_Traffic_Volume.csv)
 *   args[1]  TCP port             (default: 9999)
 *   args[2]  delay between records in ms  (default: 500)
 */
public class TrafficDataProducer {

    public static void main(String[] args) throws Exception {

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

            // Loop so that if Spark reconnects (e.g. restart), the producer
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
