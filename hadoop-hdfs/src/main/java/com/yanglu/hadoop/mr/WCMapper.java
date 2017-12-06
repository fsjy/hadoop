package com.yanglu.hadoop.mr;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

/**
 * 四个范型中，前两个是指定mapper输入数据的类型，为key-value的键值对。
 * <p></p>
 * map和reduce的数据输入输出都是key-value对封装
 * <p></p>
 * 默认情况下，框架传递给我们的mapper的输入数据中，key是要处理的文本中一行的其实偏移量，这一行的内容作为value传递进来
 *
 * @author Dacular
 * @see org.apache.hadoop.mapreduce.Mapper
 */
public class WCMapper extends Mapper<LongWritable, Text, Text, LongWritable> {


    /**
     * mapreduce框架每读一次数据就嗲用一次此方法。传递进来就是key与value。
     *
     * @param key     每一行数据起始偏移量
     * @param value   每一行数据的内容
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        // 取得每一行的数据
        String line = value.toString();

        // 切分单词
        String[] words = StringUtils.split(line, " ");

        // 输出为kv形式
        // k = 单词
        // v = 1
        for (String word : words) {
            context.write(new Text(word), new LongWritable(1L));
        }

    }
}
