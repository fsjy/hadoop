package com.yanglu.hadoop.mr.flowsort;

import com.yanglu.hadoop.mr.flow.FlowBean;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SortMapper extends Mapper<LongWritable, Text, FlowBean, NullWritable> {

    /**
     * FlowBean作为key进行输出
     *
     * @param key
     * @param value
     * @param context
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String line = value.toString();

        String[] fields = StringUtils.split(line, "\t");

        String phoneNum = fields[0];
        long u_flow = Long.parseLong(fields[1]);
        long d_flow = Long.parseLong(fields[2]);

        context.write(new FlowBean(phoneNum, u_flow, d_flow), NullWritable.get());

    }

}
