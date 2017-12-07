package com.yanglu.hadoop.mr.flowsort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;


/**
 *
 *
 *
 */
public class FlowSortMR {

//    public static class SortMapper extends Mapper<LongWritable, Text, FlowBean2, NullWritable> {
//
//        /**
//         * FlowBean作为key进行输出
//         *
//         * @param key
//         * @param value
//         * @param context
//         */
//        @Override
//        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
//
//            String line = value.toString();
//
//            String[] fields = StringUtils.split(line, "\t");
//
//            String phoneNum = fields[0];
//            long u_flow = Long.parseLong(fields[1]);
//            long d_flow = Long.parseLong(fields[2]);
//
//            context.write(new FlowBean2(phoneNum, u_flow, d_flow), NullWritable.get());
//
//        }
//
//    }
//
//
//    public static class SortReducer extends Reducer<FlowBean2, NullWritable, FlowBean2, NullWritable> {
//
//        /**
//         * This method is called once for each key. Most applications will define
//         * their reduce class by overriding this method. The default implementation
//         * is an identity function.
//         *
//         * @param key
//         * @param values
//         * @param context
//         */
//        @Override
//        protected void reduce(FlowBean2 key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
//
//            context.write(key, NullWritable.get());
//
//        }
//    }


    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration configuration = new Configuration();

        //configuration.set("fs.defaultFS", "hdfs://centos01:9000");

        Job job = Job.getInstance(configuration);

        job.setJarByClass(FlowSortMR.class);

        job.setMapperClass(SortMapper.class);
        job.setReducerClass(SortReducer.class);

        //job.setMapOutputKeyClass(FlowSort.class);
        //job.setMapOutputValueClass(NullWritable.class);

        job.setOutputKeyClass(FlowSort.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 1 : 0);

    }


}
