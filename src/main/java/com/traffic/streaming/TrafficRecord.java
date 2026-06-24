package com.traffic.streaming;

import java.io.Serializable;

/**
 * Represents one traffic observation record.
 * Used by the streaming producer and the Spark Streaming application.
 *
 * Dataset schema:
 *   holiday,temp,rain_1h,snow_1h,clouds_all,weather_main,
 *   weather_description,date_time,traffic_volume
 *   index: 0      1    2        3       4          5
 *                 6               7           8
 */
public class TrafficRecord implements Serializable {

    private static final long serialVersionUID = 1L;

    public final String dateTime;
    public final String weatherMain;
    public final int trafficVolume;

    public TrafficRecord(String dateTime, String weatherMain, int trafficVolume) {
        this.dateTime = dateTime;
        this.weatherMain = weatherMain;
        this.trafficVolume = trafficVolume;
    }

    /**
     * Parse a full CSV row from Metro_Interstate_Traffic_Volume.csv.
     * Returns null when the line is malformed or is the header.
     */
    public static TrafficRecord fromCsvLine(String line) {
        if (line == null || line.startsWith("holiday")) {
            return null;
        }
        String[] fields = line.split(",");
        if (fields.length < 9) {
            return null;
        }
        try {
            String dateTime = fields[7].trim();
            String weather  = fields[5].trim();
            int volume      = Integer.parseInt(fields[8].trim());
            return new TrafficRecord(dateTime, weather, volume);
        } catch (NumberFormatException e) {
            return null;
        }
    }

    /**
     * Produces the wire format consumed by TrafficStreamingApp:
     *   timestamp,weather,trafficVolume
     */
    public String toStreamFormat() {
        return dateTime + "," + weatherMain + "," + trafficVolume;
    }

    @Override
    public String toString() {
        return toStreamFormat();
    }
}
