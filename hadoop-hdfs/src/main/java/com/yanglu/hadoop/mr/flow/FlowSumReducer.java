package com.yanglu.hadoop.mr.flow;

import com.yanglu.hadoop.mr.flow.FlowBean;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FlowSumReducer extends Reducer<Text, FlowBean, Text, FlowBean> {


    /**
     * 框架每传递一组数据<phoneNum, {flowBean,flowBean,flowBean....}> 就调用一次此reduce方法
     * 此方法逻辑：
     * 遍历values，累加求和再输出，
     *
     * @param key
     * @param values
     * @param context
     */
    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {

        long up_flow = 0;
        long d_flow = 0;

        for (FlowBean bean : values) {

            up_flow += bean.getUp_flow();
            d_flow += bean.getD_flow();

        }

        context.write(key, new FlowBean(key.toString(), up_flow, d_flow));

    }
}
