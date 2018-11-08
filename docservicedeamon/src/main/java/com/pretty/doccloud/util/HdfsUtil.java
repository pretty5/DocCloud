package com.pretty.doccloud.util;

import com.google.common.io.Resources;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.File;
import java.io.IOException;
import java.util.UUID;

/*
*@ClassName:HdfsUtil
 @Description:TODO
 @Author:
 @Date:2018/10/29 17:17 
 @Version:v1.0
*/
public class HdfsUtil {
    //文档上传工具类
    public static void upload(byte[] src, String docName, String dst) throws IOException {
        //加载配置文件
        Configuration coreSiteConf = new Configuration();
        coreSiteConf.addResource(Resources.getResource("core-site.xml"));
        //获取文件系统客户端对象
        FileSystem fileSystem = FileSystem.get(coreSiteConf);

        FSDataOutputStream fsDataOutputStream = fileSystem.create(new Path(dst + "/" + docName));

        fsDataOutputStream.write(src);
        fsDataOutputStream.close();
        fileSystem.close();
    }
    public static void copyFromLocal(String src,String dst) throws IOException {
        Configuration coreSiteConf = new Configuration();
        coreSiteConf.addResource(Resources.getResource("core-site.xml"));
        //获取文件系统客户端对象
        FileSystem fileSystem = FileSystem.get(coreSiteConf);
        fileSystem.copyFromLocalFile(new Path(src),new Path(dst));
        fileSystem.close();
    }
    public static void download(String input,String output) throws IOException {
        Configuration coreSiteConf = new Configuration();
        coreSiteConf.addResource(Resources.getResource("core-site.xml"));
        //获取文件系统客户端对象
        FileSystem fileSystem = FileSystem.get(coreSiteConf);

        fileSystem.copyToLocalFile(new Path(input),new Path(output));
        fileSystem.close();
    }

    public static void main(String[] args) throws IOException {
        String tmpWorkDirPath = "/tmp/docjobdaemon/" + UUID.randomUUID().toString() + "/";
        //创建临时工作目录
        File tmpWorkDir = new File(tmpWorkDirPath);
        tmpWorkDir.mkdirs();
        String input="hdfs://192.168.18.3:9000/doccloud//2018-10-30/26478338-ae66-4659-8b87-fb7945f45dbb//hadoopclientcode.docx";

        download(input,tmpWorkDirPath);
    }

}
