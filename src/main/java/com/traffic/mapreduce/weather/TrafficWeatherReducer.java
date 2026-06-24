package com.traffic.mapreduce.weather;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TrafficWeatherReducer
        extends Reducer<Text, IntWritable, Text, Text> {

    private String highestWeather = null;
    private long highestAvg = Long.MIN_VALUE;
    private String lowestWeather = null;
    private long lowestAvg = Long.MAX_VALUE;

    @Override
    public void reduce(Text key,
                       Iterable<IntWritable> values,
                       Context context)
            throws IOException, InterruptedException {

        long sum = 0;
        int count = 0;

        for (IntWritable value : values) {
            sum += value.get();
            count++;
        }

        long avg = count > 0 ? sum / count : 0;
        context.write(key, new Text(String.valueOf(avg)));

        String weather = key.toString();
        if (avg > highestAvg) {
            highestAvg = avg;
            highestWeather = weather;
        }
        if (avg < lowestAvg) {
            lowestAvg = avg;
            lowestWeather = weather;
        }
    }

    @Override
    protected void cleanup(Context context)
            throws IOException, InterruptedException {

        if (highestWeather != null) {
            context.write(
                new Text("HIGHEST_WEATHER"),
                new Text(highestWeather + "  (avg = " + highestAvg + " vehicles/hour)"));
            context.write(
                new Text("LOWEST_WEATHER"),
                new Text(lowestWeather + "  (avg = " + lowestAvg + " vehicles/hour)"));
        }
    }
}
