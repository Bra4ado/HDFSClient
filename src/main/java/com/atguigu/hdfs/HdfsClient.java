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
    public void testUpload() throws IOException {

        //1, 是否删除原数据
        //2，是否能覆盖Hadoop上的老数据
        //3，本机文件的路径
        //4，Hadoop上的路径
        fs.copyFromLocalFile(true,true,new Path("C:/Users/lenovo/Documents/sunwukong.txt"),new Path("/xiyou/huaguoshan"));
        //配置优先级 hads-default.xml<hdfs-site.xml<resource中的配置<new Configuration()中的设置

    }
    @Test
    public void testDownload() throws IOException {

        //1, 是否删除原数据
        //2，是否能覆盖Hadoop上的老数据
        //3，本机文件的路径
        //4，Hadoop上的路径
        fs.copyToLocalFile(true,new Path("/xiyou/huaguoshan/sunwukong.txt"),new Path("C:/Users/lenovo/Documents"),false);
        //配置优先级 hads-default.xml<hdfs-site.xml<resource中的配置<new Configuration()中的设置

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
