package com.atguigu.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;


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
    @Test
    public void testRemove() throws IOException {

        //1, 是否删除原数据
        //2，是否能覆盖Hadoop上的老数据
        //3，本机文件的路径
        //4，Hadoop上的路径
        fs.delete(new Path("/jdk-8u212-linux-x64.tar.gz"),false);
        //配置优先级 hads-default.xml<hdfs-site.xml<resource中的配置<new Configuration()中的设置

    }
    @Test
    public void testMoveAndReName() throws IOException {
        fs.rename(new Path("/xiyou/input.txt"),new Path("/ss.txt"));
    }

    @Test
    public void testListFiles() throws IOException, InterruptedException, URISyntaxException {



        // 2 获取文件详情
        RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"), true);

        while (listFiles.hasNext()) {
            LocatedFileStatus fileStatus = listFiles.next();

            System.out.println("========" + fileStatus.getPath() + "=========");
            System.out.println(fileStatus.getPermission());
            System.out.println(fileStatus.getOwner());
            System.out.println(fileStatus.getGroup());
            System.out.println(fileStatus.getLen());
            System.out.println(fileStatus.getModificationTime());
            System.out.println(fileStatus.getReplication());
            System.out.println(fileStatus.getBlockSize());
            System.out.println(fileStatus.getPath().getName());

            // 获取块信息
            BlockLocation[] blockLocations = fileStatus.getBlockLocations();
            System.out.println(Arrays.toString(blockLocations));
        }

    }

    @Test
    public void testListStatus() throws IOException, InterruptedException, URISyntaxException{

        // 2 判断是文件还是文件夹
        FileStatus[] listStatus = fs.listStatus(new Path("/"));

        for (FileStatus fileStatus : listStatus) {
            // 如果是文件
            if (fileStatus.isFile()) {
                System.out.println("f:"+fileStatus.getPath().getName());
            }else {
                System.out.println("d:"+fileStatus.getPath().getName());
            }
        }

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
