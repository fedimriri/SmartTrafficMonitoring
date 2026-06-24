package com.traffic.mapreduce.weather;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TrafficWeatherDriver {

    public static void main(String[] args)
            throws Exception {

        if (args.length != 2) {
            System.err.println(
                "Usage: TrafficWeatherDriver <input> <output>");
            System.exit(2);
        }

        Configuration conf = new Configuration();

        Job job = Job.getInstance(
                conf,
                "Traffic Volume By Weather");

        job.setJarByClass(
                TrafficWeatherDriver.class);

        job.setMapperClass(
                TrafficWeatherMapper.class);

        job.setReducerClass(
                TrafficWeatherReducer.class);

        // Mapper emits (Text weather, IntWritable volume)
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // Reducer emits (Text label, Text average/annotation)
        // Note: a Combiner is not used here because the Reducer computes
        // averages. Combining partial sums without counts would produce
        // incorrect averages in the final Reducer.
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(
                job,
                new Path(args[0]));

        FileOutputFormat.setOutputPath(
                job,
                new Path(args[1]));

        System.exit(
                job.waitForCompletion(true)
                        ? 0
                        : 1);
    }
}
