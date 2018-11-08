package com.pretty.doccloud.job.callback;

import com.pretty.doccloud.doccloudweb.entity.Doc;
import com.pretty.doccloud.doccloudweb.entity.DocJobEntity;
import com.pretty.doccloud.doccloudweb.service.DocJobService;
import com.pretty.doccloud.doccloudweb.service.DocService;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.ipc.ProtocolSignature;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.Optional;

/*
*@ClassName:DocJobCallbackImpl
 @Description:TODO
 @Author:
 @Date:2018/10/31 17:21 
 @Version:v1.0
*/
@Slf4j
@Component
public class DocJobCallbackImpl implements DocJobCallback {


    @Autowired
    DocJobService docJobService;
    @Autowired
    DocService docService;
    @Override
    public void reportDocJob(DocJobResponse docJobResponse) {
        log.info("receice job response : {}",docJobResponse);
        //更新数据库job状态
        //如果成功更新job状态为成功，更新文档状态为可浏览
        if (docJobResponse.isSuccess()){
            log.info("docjob :{} success,update job status and doc status",docJobResponse.getDocJobId());
            String docJobId = docJobResponse.getDocJobId();
            Optional<DocJobEntity> docJobEntityOptional = docJobService.findById(docJobId);
           if (docJobEntityOptional.isPresent()){
               //更新job状态
               DocJobEntity docJobEntity = docJobEntityOptional.get();
               docJobEntity.setJobStatus("success");
               docJobService.save(docJobEntity);
               //修改文档状态
               Doc doc = docService.findbyId(docJobEntity.getDocId()).get();
               doc.setDocStatus("view");
               doc.setNumOfPage(docJobResponse.getNumOfPage());
               docService.save(doc);
               log.info("doc : {},docName:{} can be view",doc.getId(),doc.getDocName());

           }else{
               log.error("docjob : {} is not present",docJobResponse.getDocJobId());
               throw new RuntimeException("docjob : "+ docJobResponse.getDocJobId()+ " is not present");
           }

        }else{
            //如果失败，更新job状态为失败，增加重试次数，需要定时调度，从数据库中取出失败job，再次提交
            //如果任务执行次数达到两次还没有成功，则放弃该任务，通知文档上传失败

            log.info("docjob : {} failed,start to retry",docJobResponse.getDocJobId());
        }

       // docJobResponse.getDocJobId();
        //数据库修改job状态
    }

    @Override
    public long getProtocolVersion(String protocol, long clientVersion) throws IOException {
        return versionID;
    }

    @Override
    public ProtocolSignature getProtocolSignature(String protocol, long clientVersion, int clientMethodsHash) throws IOException {
        return null;
    }
}
