package com.pretty.doccloud.doccloudweb.entity;

import lombok.Data;
import org.hibernate.annotations.GenericGenerator;

import javax.persistence.*;
import java.util.Date;

/*
*@ClassName:Doc
 @Description:TODO
 @Author:
 @Date:2018/10/29 15:40 
 @Version:v1.0
*/
@Entity
@Table(name = "doc")
@Data
public class Doc {
    @Id
    @GeneratedValue(strategy=GenerationType.IDENTITY)//自动生成自增id
    private int id;
    @Column(name = "md5")//如果数据库字段与entity中字段名一样，则不用加此注解
    private String md5;
    @Column(name = "doc_name")
    private String docName;
    @Column(name = "doc_type")
    private String docType;
    @Column(name = "doc_status")
    private String docStatus;
    @Column(name = "doc_size")
    private int docSize;
    @Column(name = "doc_dir")
    private String docDir;
    @Column(name = "user_id")
    private int userId;
    @Column(name = "doc_create_time")
    private Date docCreateTime;
    @Column(name = "doc_comment")
    private String docComment;
    @Column(name = "doc_permission")
    private String docPermission;
    @Column(name="num_of_page")
    private int numOfPage;





}
