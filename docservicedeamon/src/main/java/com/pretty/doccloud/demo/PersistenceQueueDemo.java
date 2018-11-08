package com.pretty.doccloud.demo;

import com.pretty.doccloud.util.BdbPersistentQueue;

/*
*@ClassName:PersistenceQueueDemo
 @Description:TODO
 @Author:
 @Date:2018/10/30 15:49 
 @Version:v1.0
*/
public class PersistenceQueueDemo {
    public static void main(String[] args) {
        BdbPersistentQueue<String> queue = new BdbPersistentQueue<String>("d:\\bdb", "test", String.class);

       /*queue.offer("tri");
        queue.offer("qua");*/
       String data1 = queue.poll();
        //String data2 = queue.poll();

        System.out.println(data1);
        //System.out.println(data2);

    }


}
