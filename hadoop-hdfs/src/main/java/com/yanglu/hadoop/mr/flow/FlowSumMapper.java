package com.yanglu.hadoop.mr.flow;

import com.yanglu.hadoop.mr.flow.untity.FlowBean;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowSumMapper extends Mapper<LongWritable, Text, Text, FlowBean> {


    /**
     *
     * 拿到一行数据，切取数据，拿到手机号码以及个
     *
     *
     *
     * @param key
     * @param value
     * @param context
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        // 一行数据获取
        String line = value.toString();

        // tab 切分
        String[] fileds = StringUtils.split(line, "\t");

        String phoneNum = fileds[1];
        long u_flow = Long.parseLong(fileds[7]);
        long d_flow = Long.parseLong(fileds[8]);

        context.write(new Text(phoneNum), new FlowBean(phoneNum, u_flow,d_flow));

    }
}
