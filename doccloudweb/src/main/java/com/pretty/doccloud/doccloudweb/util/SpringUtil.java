package com.pretty.doccloud.doccloudweb.util;

import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.stereotype.Component;

/*
*@ClassName:SpringUtil
 @Description:TODO
 @Author:
 @Date:2018/11/1 11:28 
 @Version:v1.0
*/
@Component
public class SpringUtil implements ApplicationContextAware {
    private static ApplicationContext spring;
    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
            if (spring==null){
                spring=applicationContext;
            }
    }
    public static ApplicationContext getApplicationContext(){
        return spring;
    }

}
