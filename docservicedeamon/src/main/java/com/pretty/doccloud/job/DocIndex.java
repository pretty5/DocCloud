package com.pretty.doccloud.job;

import lombok.Data;
import org.apache.solr.client.solrj.beans.Field;

/*
*@ClassName:DocIndex
 @Description:TODO
 @Author:
 @Date:2018/10/31 14:10 
 @Version:v1.0
*/
//用来封装索引字段
@Data
public class DocIndex {
    @Field
    private int id;
    @Field
    private String docName;
    @Field
    private String url;
    @Field
    private String docContent;
    @Field
    private String docType;

}
