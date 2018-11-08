package com.pretty.doccloud.job.callback;

import lombok.Data;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/*
*@ClassName:DocJobResponse
 @Description:TODO
 @Author:
 @Date:2018/10/31 17:10 
 @Version:v1.0
*/
@Data
public class DocJobResponse implements Writable {
    private String docJobId;
    private String message;
    private boolean success;
    private int numOfPage;
    private int retryTime;
    private long finishTime;



    public void write(DataOutput out) throws IOException {
        out.writeUTF(docJobId);
        out.writeUTF(message);
        out.writeBoolean(success);
        out.writeInt(numOfPage);
        out.writeInt(retryTime);
        out.writeLong(finishTime);
    }

    public void readFields(DataInput in) throws IOException {
        docJobId=in.readUTF();
        message=in.readUTF();
        success=in.readBoolean();
        numOfPage=in.readInt();
        retryTime=in.readInt();
        finishTime=in.readLong();
    }
}
