package com.yanglu.hadoop.mr;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.codehaus.jackson.jaxrs.MapperConfigurator;

import java.io.IOException;

/**
 * 四个范型中，前两个是指定mapper输入数据的类型，为key-value的键值对。
 * <p></p>
 * map和reduce的数据输入输出都是key-value对封装
 * <p></p>
 * 默认情况下，框架传递给我们的mapper的输入数据中，key是要处理的文本中一行的其实偏移量，这一行的内容作为value传递进来
 *
 * @see org.apache.hadoop.mapreduce.Mapper
 * @author Dacular
 */
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
