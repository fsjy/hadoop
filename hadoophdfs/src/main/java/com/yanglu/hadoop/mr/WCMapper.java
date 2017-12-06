package com.yanglu.hadoop.mr;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.codehaus.jackson.jaxrs.MapperConfigurator;

import java.io.IOException;

public class WCMapper extends Mapper<LongWritable, Text, Text, LongWritable> {


    /**
     * mapreduce框架每读一次数据就嗲用一次此方法。传递进来就是key与value。
     *
     * @param key
     * @param value
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {


    }
}
