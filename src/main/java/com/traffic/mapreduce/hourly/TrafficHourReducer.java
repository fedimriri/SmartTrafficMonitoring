package com.traffic.mapreduce.hourly;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TrafficHourReducer
        extends Reducer<Text, IntWritable, Text, Text> {

    private int peakHour = -1;
    private long peakAvg = Long.MIN_VALUE;
    private int lowestHour = -1;
    private long lowestAvg = Long.MAX_VALUE;

    @Override
    public void reduce(Text key,
                       Iterable<IntWritable> values,
                       Context context)
            throws IOException, InterruptedException {

        long sum = 0;
        int count = 0;

        for (IntWritable val : values) {
            sum += val.get();
            count++;
        }

        long avg = count > 0 ? sum / count : 0;
        context.write(key, new Text(String.valueOf(avg)));

        int hour = Integer.parseInt(key.toString());
        if (avg > peakAvg) {
            peakAvg = avg;
            peakHour = hour;
        }
        if (avg < lowestAvg) {
            lowestAvg = avg;
            lowestHour = hour;
        }
    }

    @Override
    protected void cleanup(Context context)
            throws IOException, InterruptedException {

        if (peakHour >= 0) {
            context.write(
                new Text("PEAK_HOUR"),
                new Text(String.format("%02d  (avg = %d vehicles/hour)",
                        peakHour, peakAvg)));
            context.write(
                new Text("LOWEST_HOUR"),
                new Text(String.format("%02d  (avg = %d vehicles/hour)",
                        lowestHour, lowestAvg)));
        }
    }
}
