package com.traffic.mapreduce.hourly;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TrafficHourDriver {

    public static void main(String[] args) throws Exception {

        if (args.length != 2) {
            System.err.println(
                "Usage: TrafficHourDriver <input> <output>");
            System.exit(2);
        }

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf,
                "Traffic Volume By Hour");

        job.setJarByClass(TrafficHourDriver.class);

        job.setMapperClass(TrafficHourMapper.class);
        job.setReducerClass(TrafficHourReducer.class);

        // Mapper emits (Text hour, IntWritable volume)
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
