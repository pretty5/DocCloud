package com.pretty.doccloud.job;


import org.apache.hadoop.ipc.VersionedProtocol;

/*
*@ClassName:JobDeamonSerive
 @Description:TODO
 @Author:
 @Date:2018/10/30 10:43 
 @Version:v1.0
*/
public interface JobDaemonService extends VersionedProtocol {
    long versionID=1L;
    void submitDocJob(DocJob job);
}
