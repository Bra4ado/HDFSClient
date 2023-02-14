package com.atguigu.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import java.net.URI;
import java.net.URISyntaxException;


public class HdfsClient {

    private static  FileSystem fs;

    @Test
    public void testMkdirs() throws IOException, URISyntaxException, InterruptedException {
        // 2 创建目录
        fs.mkdirs(new Path("/xiyou/huaguoshan1/"));

    }

    @Test
    public void testUpload() throws IOException, URISyntaxException, InterruptedException {

        fs.copyFromLocalFile()

    }

    @Before
    public void init() throws URISyntaxException, IOException, InterruptedException {
        // 1 获取文件系统
        Configuration configuration = new Configuration();

        fs = FileSystem.get(new URI("hdfs://hadoop102:8020"), configuration,"atguigu");

    }

    @After
    public void close() throws IOException {
        fs.close();
    }
}
