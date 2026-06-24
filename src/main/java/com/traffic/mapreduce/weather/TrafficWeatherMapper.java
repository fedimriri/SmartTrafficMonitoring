package com.traffic.mapreduce.weather;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TrafficWeatherMapper
        extends Mapper<Object, Text, Text, IntWritable> {

    private IntWritable trafficValue = new IntWritable();

    private Text weather = new Text();

    @Override
    public void map(Object key,
                    Text value,
                    Context context)
            throws IOException, InterruptedException {

        String line = value.toString();

        if (line.startsWith("holiday")) {
            return;
        }

        try {

            String[] fields = line.split(",");

            String weatherMain = fields[5];
            int traffic =
                    Integer.parseInt(fields[8]);

            weather.set(weatherMain);
            trafficValue.set(traffic);

            context.write(weather, trafficValue);

        } catch (Exception e) {
            // Ignore malformed lines
        }
    }
}