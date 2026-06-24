package com.traffic.utils;

/**
 * Utility methods for parsing the Metro Interstate Traffic Volume CSV dataset.
 *
 * CSV column layout:
 *   0: holiday
 *   1: temp
 *   2: rain_1h
 *   3: snow_1h
 *   4: clouds_all
 *   5: weather_main
 *   6: weather_description
 *   7: date_time
 *   8: traffic_volume
 */
public class CsvParser {

    public static final int COL_WEATHER_MAIN  = 5;
    public static final int COL_DATE_TIME     = 7;
    public static final int COL_TRAFFIC_VOL   = 8;
    public static final int MIN_FIELDS        = 9;

    private CsvParser() {}

    /** Split a raw CSV line into fields. */
    public static String[] parse(String line) {
        return line.split(",");
    }

    /** Returns true when the line is the header row. */
    public static boolean isHeader(String line) {
        return line != null && line.startsWith("holiday");
    }

    /**
     * Extract the two-digit hour string from a date_time field.
     * Example: "2012-10-02 09:00:00" -> "09"
     */
    public static String extractHour(String dateTime) {
        return dateTime.substring(11, 13);
    }

    /**
     * Parse traffic_volume as an integer.
     * Throws NumberFormatException for malformed values.
     */
    public static int parseVolume(String[] fields) {
        return Integer.parseInt(fields[COL_TRAFFIC_VOL].trim());
    }
}
