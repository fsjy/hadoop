package com.yanglu.hadoop.mr.flowsort;

import com.yanglu.hadoop.mr.flow.FlowBean;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SortReducer extends Reducer<FlowBean, NullWritable, FlowBean, NullWritable> {

    /**
     * This method is called once for each key. Most applications will define
     * their reduce class by overriding this method. The default implementation
     * is an identity function.
     *
     * @param key
     * @param values
     * @param context
     */
    @Override
    protected void reduce(FlowBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {

        context.write(key, NullWritable.get());

    }
}
