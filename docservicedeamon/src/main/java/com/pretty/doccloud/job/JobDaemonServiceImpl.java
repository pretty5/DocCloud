package com.pretty.doccloud.job;

import com.pretty.doccloud.util.BdbPersistentQueue;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.ipc.ProtocolSignature;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/*
*@ClassName:JobDaemonServiceImpl
 @Description:TODO
 @Author:
 @Date:2018/10/30 10:56 
 @Version:v1.0
*/
@Slf4j
public class JobDaemonServiceImpl implements JobDaemonService ,Runnable{
    public  BdbPersistentQueue<DocJob> queue;
    //多线程并行处理
    private ExecutorService pool=Executors.newFixedThreadPool(4);

    private volatile boolean flag=true;

    public JobDaemonServiceImpl() {
        //创建工作目录
        File workDir = new File(WORK_DIR + "/bdb/");
        if (!workDir.exists()){
            workDir.mkdirs();
            System.out.println(workDir.getAbsolutePath());
        }
        //初始化持久化队列
        queue=new BdbPersistentQueue<DocJob>(WORK_DIR+"/bdb/","docjob",DocJob.class);

    }


    private static final String WORK_DIR="/tmp/docjobdaemon/";


    public void submitDocJob(DocJob job) {
        //放入队列
        log.info("receive job {}",job);
        queue.offer(job);
        //System.out.println(job);
    }

    public long getProtocolVersion(String protocol, long clientVersion) throws IOException {
        return versionID;
    }

    public ProtocolSignature getProtocolSignature(String protocol, long clientVersion, int clientMethodsHash) throws IOException {
        return null;
    }

    public void run() {
        while (flag){
            DocJob docJob = queue.poll();
            if (docJob==null){
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }else{
                pool.submit(new DocJobHandler(docJob));
            }

        }
    }
}
