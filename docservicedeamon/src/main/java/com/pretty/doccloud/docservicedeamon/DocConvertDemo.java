package com.pretty.doccloud.docservicedeamon;

import org.apache.commons.io.IOUtils;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.UUID;

/*
*@ClassName:DocConvertDemo
 @Description:TODO
 @Author:
 @Date:2018/10/30 9:31 
 @Version:v1.0
*/
public class DocConvertDemo {
    public static void main(String[] args) throws IOException {
        String command="soffice --headless --invisible --convert-to pdf d:\\hadoopclientcode.docx";
        String workDir="d:\\tmp\\.stage\\"+UUID.randomUUID().toString()+"\\";
        File file = new File(workDir);
        file.mkdirs();

        Process process = Runtime.getRuntime().exec(command,null,file);

        InputStream errorStream = process.getErrorStream();

        InputStream inputStream = process.getInputStream();

        String error = IOUtils.toString(errorStream);

        String result = IOUtils.toString(inputStream);

        System.out.println(error);

        System.out.println(result);


    }
}
