package com.yanglu.hadoop.mr.flowkeysort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class FlowKeyRunner {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration configuration = new Configuration();

        // configuration.set("fs.defaultFS", "hdfs://centos01:9000");
        //configuration.set("fs.defaultFS", "hdfs://centos01:9000");
        //configuration.set("mapreduce.job.jar", "/Users/yl/.m2/repository/com/yanglu/hadoop-hdfs/1.0-SNAPSHOT/hadoop-hdfs-1.0-SNAPSHOT.jar");


        Job job = Job.getInstance(configuration);

        job.setJarByClass(FlowKeyRunner.class);

        job.setMapperClass(MYMapper.class);
        job.setReducerClass(MYReducer.class);

        //job.setMapOutputKeyClass(SortAPI.class);
        //job.setMapOutputValueClass(NullWritable.class);

        job.setOutputKeyClass(SortAPI.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.out.println("===================new3=======================");
        System.exit(job.waitForCompletion(true) ? 1 : 0);

    }
}