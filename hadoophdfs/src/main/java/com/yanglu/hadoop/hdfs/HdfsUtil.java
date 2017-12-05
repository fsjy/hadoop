package com.yanglu.hadoop.hdfs;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.Before;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;

public class HdfsUtil {

    public static void main(String[] args) throws IOException {


        // upload file to hdfs

        Configuration configuration = new Configuration();

        // configuration.addResource("hdfs://centos01:9000");

        FileSystem fs = FileSystem.get(configuration);


        Path path = new Path("hdfs://centos01:9000/wordcount/input/wc.txt");

        FSDataInputStream fsDataInputStream = fs.open(path);

        FileOutputStream fileOutputStream = new FileOutputStream("/Users/yl/source/hadoop/wc.txt");

        IOUtils.copy(fsDataInputStream, fileOutputStream);


    }

    FileSystem fs = null;

    @Before
    public void init() throws IOException, URISyntaxException, InterruptedException {

        // 读取classpath下的 xxx-site.xml 配置文件，解析内容封装到conf对象
        Configuration configuration = new Configuration();

        // 也可以在代码中在conf中的配置信息进行手动配置，可覆盖掉配置文件中读取的值
        configuration.set("fs.defaultFS", "hdfs://centos01:9000/");

        // 根据配置信息去获取一个文件系统的客户端操作实例对象
        fs = FileSystem.get(new URI("hdfs://centos01:9000/"), configuration, "root");

    }

    /**
     * 比较底层的写法
     *
     * @param filePath
     * @param dstFile
     * @throws IOException
     */
    public void upload(String filePath, String dstFile) throws IOException {

        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(configuration);

        Path path = new Path("hdfs://centos01:9000/upload/" + dstFile);

        FSDataOutputStream os = fs.create(path);

        FileInputStream is = new FileInputStream(filePath);

        IOUtils.copy(is, os);

    }

    /**
     * 简单的写法
     *
     * @throws IOException
     */
    @Test
    public void uploadSimple() throws IOException {


        fs.copyFromLocalFile(new Path("/Users/yl/source/hadoop/wc.txt"),
                new Path("hdfs://centos01:9000/upload/uploadsimple.txt"));


    }


    /**
     * 下载
     */
    @Test
    public void download() throws IOException {

        fs.copyToLocalFile(new Path("hdfs://centos01:9000/upload/uploadsimple.txt"),
                new Path("/Users/yl/source/hadoop/wc_download2.txt"));

    }


    /**
     * list files
     */
    @Test
    public void listFiles() throws IOException {

        // 递归 列出文件
        RemoteIterator<LocatedFileStatus> files = fs.listFiles(new Path("/wordcount"), true);

        while (files.hasNext()) {
            LocatedFileStatus locatedFileStatus = files.next();
            System.out.println("===== File name is =====");
            System.out.println(locatedFileStatus.getPath().getName());
        }


        System.out.println("--------------------------------------------");

        // 列出文件和文件夹信息，不提供自带的递归遍历
        FileStatus[] fileStatuses = fs.listStatus(new Path("/user/root"));

        Arrays.stream(fileStatuses).forEach(path -> {

            String name = path.getPath().getName();
            System.out.println(name + (path.isDirectory() ? " is dir" : " is file"));


        });


    }


    /**
     * 创建目录
     */
    @Test
    public void mkdir() throws IOException {

        fs.mkdirs(new Path("/upload/mkdir/mkdir_deep"));

    }

    /**
     * 删除
     */
    @Test
    public void rm() throws IOException {

        fs.delete(new Path("/upload"), true);

    }


    /**
     * 文件、文件夹移动
     *
     * @throws IOException
     */
    @Test
    public void mv() throws IOException {
        fs.rename(new Path("/wordcount/output/input"), new Path("/wordcount/input"));
    }
}
