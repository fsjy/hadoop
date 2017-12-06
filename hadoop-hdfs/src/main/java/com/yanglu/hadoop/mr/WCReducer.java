package com.yanglu.hadoop.mr;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

public class WCReducer extends Reducer<Text, LongWritable, Text, LongWritable> {


    /**
     * 框架在map处理完成后，素有kv对缓存进行分组。然后传递一个组<key, values{}>调用一次reduce方法
     *
     * @param key     map中进行write时的key，即一行内容的一个word分词
     * @param values  context.write时的value的集合，比如 <hello, {1,1,1,1,1....}>
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {

        long count = 0;

        for (LongWritable value : values) {
            count += value.get();
        }

        // 输出一个单词的统计结果
        context.write(key, new LongWritable(count));
    }
}
