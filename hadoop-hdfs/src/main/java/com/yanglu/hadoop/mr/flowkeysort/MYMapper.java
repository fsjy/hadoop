package com.yanglu.hadoop.mr.flowkeysort;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MYMapper extends Mapper<LongWritable, Text, SortAPI, NullWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] splied = value.toString().split("\t");
        try {
            long first = Long.parseLong(splied[1]);
            long second = Long.parseLong(splied[2]);
            context.write(new SortAPI(splied[0], first, second), NullWritable.get());
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }
}