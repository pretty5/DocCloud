package com.pretty.doccloud.doccloudweb.service;

import com.pretty.doccloud.doccloudweb.dao.DocRepository;
import com.pretty.doccloud.doccloudweb.entity.Doc;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Optional;

/*
*@ClassName:DocService
 @Description:TODO
 @Author:
 @Date:2018/10/29 17:05 
 @Version:v1.0
*/
@Service
public class DocService {
    @Autowired
    private DocRepository docRepository;

    public Optional<Doc> findbyId(int id){
        return docRepository.findById(id);
    }
    public Optional<Doc> findbyMd5(String md5){
        return docRepository.findByMd5(md5);
    }


    public Doc save(Doc docEntity) {
        return docRepository.save(docEntity);

    }
}
