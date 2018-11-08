package com.pretty.doccloud.job.callback;

import org.apache.hadoop.ipc.VersionedProtocol;

/*
*@ClassName:DocJobCallback
 @Description:TODO
 @Author:
 @Date:2018/10/31 17:18 
 @Version:v1.0
*/
public interface DocJobCallback extends VersionedProtocol {
    long versionID=2L;
    void reportDocJob(DocJobResponse docJobResponse);
}
