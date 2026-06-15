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

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

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