package com.traffic.mapreduce.hourly;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TrafficHourMapper
        extends Mapper<Object, Text, Text, IntWritable> {

    private IntWritable trafficValue = new IntWritable();
    private Text hour = new Text();

    @Override
    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString();

        if (line.startsWith("holiday")) {
            return;
        }

        try {

            String[] fields = line.split(",");

            String dateTime = fields[7];
            int traffic = Integer.parseInt(fields[8]);

            String extractedHour = dateTime.substring(11, 13);

            hour.set(extractedHour);
            trafficValue.set(traffic);

            context.write(hour, trafficValue);

        } catch (Exception e) {
            // Ignore malformed lines
        }
    }
}