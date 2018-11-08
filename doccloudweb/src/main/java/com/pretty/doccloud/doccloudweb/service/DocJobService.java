package com.pretty.doccloud.doccloudweb.service;

import com.pretty.doccloud.doccloudweb.dao.DocJobRepository;
import com.pretty.doccloud.doccloudweb.entity.DocJobEntity;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Optional;

/*
*@ClassName:DocJobService
 @Description:TODO
 @Author:
 @Date:2018/11/1 9:56 
 @Version:v1.0
*/
@Service
public class DocJobService {
    @Autowired
    private DocJobRepository docJobRepository;

    public Optional<DocJobEntity> findById(String id){
        return docJobRepository.findById(id);
    }
    public DocJobEntity save(DocJobEntity docJobEntity){
        return docJobRepository.save(docJobEntity);
    }


}
