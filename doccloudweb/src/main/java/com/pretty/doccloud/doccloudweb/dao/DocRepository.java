package com.pretty.doccloud.doccloudweb.dao;

import com.pretty.doccloud.doccloudweb.entity.Doc;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.Optional;

/*
*@ClassName:DocRepository
 @Description:TODO
 @Author:
 @Date:2018/10/29 15:52 
 @Version:v1.0
*/
@Repository
public interface DocRepository extends JpaRepository<Doc,Integer> {
    Optional<Doc> findByMd5(String md5);
}
