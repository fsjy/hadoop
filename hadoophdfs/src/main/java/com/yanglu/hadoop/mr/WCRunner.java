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
 * _/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/
 *  1. 本地JVM运行mapreduce程序：
 *
 *    1.1 local 本地文件的:
 *
 *      FileInputFormat.setInputPaths
 *      FileInputFormat.setOutputPaths
 *      在设置input以及output的path的场合，设置到本地目录，则会启动local model
 *      ！！【注意】！！
 *      maven项目在resources下不要设置core-site.xml hdfs-site.xml，否则会启动集群的configuration配置
 *
 *    1.2 remote 集群文件的使用：
 *
 *      设置为集群路径即可
 *      hdfs集群中建立/wordcount/input/ 文件夹，并上传文件（单个复数个都可以）
 *
 * _/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/
 *
 * 2. 利用集群来计算
 *    文件从hdfs读取使用，使用集群进行wc的计算。
 *
 *    2.1 设置fs.defaultFS
 *
 *      在conf中或者core-site.xml中设置fs.defaultFS = hdfs://xxxx:9000，目的是让client端知道中间数据存放哪里。
 *      所谓中间数据，指staging.dir的数据，即resourceManager需要分发的中间数据的存放处，一般是
 *      /tmp/hadoop-yarn/staging/{user.name}/.staging/job_xxxxxxx的文件夹，如果不设定fs.defaultF，则staging的中间
 *      文件就会生成在运行mr的client端机器上，但是集群开始运行mr进程的场合无法拿到client机器上的staging数据。
 *      所以，必须让hadoop知道staging文件数据要存放在集群中。
 *
 *    2.2 设置jar位置
 *
 *      告诉hadoop要运行的jar在哪里，否则没有jar的话无法运行，采用conf.set(k,v)
 *
 *    2.3 xml配置
 *
 *      需要mapred-site.xml 以及 yarn-site.xml
 *      mapred-site.xml: 告知系统由yarn进行整体资源调度管理
 *      yarn-site.xml  : 告知系统resourceManager由哪一个节点来处理
 * _/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/
 *
 *
 * @author Dacular
 */
public class WCRunner {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration configuration = new Configuration();
        // configuration.set();

        configuration.set("fs.defaultFS","hdfs://centos01:9000");
        configuration.set("mapreduce.job.jar", "/Users/{username}/.m2/repository/com/yanglu/hadoop-hdfs/1.0-SNAPSHOT/hadoop-hdfs-1.0-SNAPSHOT.jar");

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
        FileOutputFormat.setOutputPath(job, new Path("hdfs://centos01:9000/wordcount/output_hdfs1"));

        // 将job提交给集群运行
        job.waitForCompletion(true);


    }

}
