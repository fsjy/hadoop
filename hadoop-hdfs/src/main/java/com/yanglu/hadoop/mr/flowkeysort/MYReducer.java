package com.yanglu.hadoop.mr.flowkeysort;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MYReducer extends Reducer<SortAPI, LongWritable, SortAPI, NullWritable> {

    @Override
    protected void reduce(SortAPI key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        context.write(key, NullWritable.get());
    }

}
