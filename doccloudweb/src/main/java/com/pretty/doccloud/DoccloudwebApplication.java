package com.pretty.doccloud;

import com.pretty.doccloud.doccloudweb.util.SpringUtil;
import com.pretty.doccloud.job.callback.DocJobCallback;
import com.pretty.doccloud.job.callback.DocJobCallbackImpl;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.web.servlet.MultipartConfigFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.web.context.support.WebApplicationContextUtils;

import javax.servlet.MultipartConfigElement;
import java.io.IOException;

@SpringBootApplication
public class DoccloudwebApplication {

    public static void main(String[] args) throws IOException {
        SpringApplication.run(DoccloudwebApplication.class, args);
        //暴露服务端口
        startDocJobCallBack();
    }

    private static void startDocJobCallBack() throws IOException {
        //DocJobCallbackImpl instance = new DocJobCallbackImpl();
        //从容器外部获取容器内部的bean
        DocJobCallbackImpl instance = SpringUtil.getApplicationContext().getBean(DocJobCallbackImpl.class);
        // 创建一个RPC builder
        RPC.Builder builder = new RPC.Builder(new Configuration());

        //指定RPC Server的参数
        builder.setBindAddress("localhost");
        builder.setPort(8877);

        //将自己的程序部署到server上
        builder.setProtocol(DocJobCallback.class);
        builder.setInstance(instance);

        //创建Server
        RPC.Server server = builder.build();

        //启动服务
        server.start();

    }

    @Bean
    public MultipartConfigElement multipartConfigElement() {
        MultipartConfigFactory factory = new MultipartConfigFactory();
        //单个文件最大
        factory.setMaxFileSize("102400KB"); //KB,MB
        /// 设置总上传数据总大小
        factory.setMaxRequestSize("102400KB");

        return factory.createMultipartConfig();
    }
}
