package com.yanglu.hadoop.mr;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


import java.io.IOException;


/**
 * 描述一个特定的作业，描述使用哪一个map和reduce
 * <p></p>
 * 指定作业处理的数据所在路径，作业输出结果所在路径，etc
 * 事前需要准备：
 * <p></p>
 * 1. hdfs集群中建立/wordcount/input/ 文件夹，并上传文件（单个复数个都可以）
 * 2. local 本地模式:
 *    FileInputFormat.setInputPaths
 *    FileInputFormat.setOutputPaths
 *    在设置input以及output的path的场合，设置到本地目录，则会启动local model
 *    ！！【注意】！！
 *    maven项目在resources下不要设置core-site.xml hdfs-site.xml，否则会启动集群的configuration配置
 *
 * 3. remote 集群模式：
 *    设置为集群路径即可
 *
 * @author Dacular
 */
public class WCRunner {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration configuration = new Configuration();
        // configuration.set();

        // 实例化job
        Job job = Job.getInstance(configuration);

        // 设置整个job所用的类在哪里
        // 告诉mapreduce是哪一个jar包
        job.setJarByClass(WCRunner.class);

        // 本job使用的mapper 和 reducer的类
        job.setMapperClass(WCMapper.class);
        job.setReducerClass(WCReducer.class);

        // 指定reduce的输出数据kv类型(对map以及reduce都起作用)
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        // 指定map的输入出的kv类型(只对map起作用)
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        // 指定原始数据存储路径
        // 【注意】
        // FileInputFormat在两个地方都有
        // 使用：org.apache.hadoop.mapreduce.lib.input。此版为新版本
        // 导入maven：hadoop-mapreduce-client-jobclient
        // 指定父目录即可
        // 本地模式则使用本地路径
        // FileInputFormat.setInputPaths(job, new Path("Users/xx/wordcount/input"));

        // 集群模式则使用集群路径
        FileInputFormat.setInputPaths(job, new Path("hdfs://centos01:9000/wordcount/input"));

        // 指定处理结果输出路径
        // 本地模式则使用本地路径
        // FileInputFormat.setOutputPath(job, new Path("Users/xx/wordcount/output2"));

        // 集群模式则使用集群路径
        FileOutputFormat.setOutputPath(job, new Path("hdfs://centos01:9000/wordcount/output2"));

        // 将job提交给集群运行
        job.waitForCompletion(true);


    }

}
