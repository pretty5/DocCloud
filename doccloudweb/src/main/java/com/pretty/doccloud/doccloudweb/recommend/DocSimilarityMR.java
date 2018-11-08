package com.pretty.doccloud.doccloudweb.recommend;

import com.google.common.io.Resources;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.*;

/*
*@ClassName:DocSimilarityMR
 @Description:TODO
 @Author:
 @Date:2018/11/5 9:27 
 @Version:v1.0
*/
public class DocSimilarityMR {
    public static void main(String[] args) throws IOException, InterruptedException {
        Configuration coreSiteConf = new Configuration();
        coreSiteConf.addResource(Resources.getResource("core-site.xml"));
        Job job1 = setJob1();
        Job job3 = setJob3();
        Job job2 = setJob2();

        ControlledJob controlledJob1 = new ControlledJob(coreSiteConf);
        controlledJob1.setJob(job1);

        ControlledJob controlledJob2 = new ControlledJob(coreSiteConf);
        controlledJob2.setJob(job2);
        ControlledJob controlledJob3 = new ControlledJob(coreSiteConf);
        controlledJob3.setJob(job3);

        controlledJob2.addDependingJob(controlledJob1);
        controlledJob3.addDependingJob(controlledJob2);

        JobControl jobControl = new JobControl("demo");
        jobControl.addJob(controlledJob1);
        jobControl.addJob(controlledJob2);
        jobControl.addJob(controlledJob3);

        new Thread(jobControl).start();

        while (true){
            List<ControlledJob> jobList = jobControl.getRunningJobList();
            System.out.println(jobList);
            Thread.sleep(5000);
        }





    }

    private static Job setJob2() throws IOException {
        Configuration coreSiteConf = new Configuration();


        coreSiteConf.addResource(Resources.getResource("core-site.xml"));
        //设置一个任务
        Job job = Job.getInstance(coreSiteConf, "wcoo");
        //设置job的运行类
        job.setJarByClass(DocSimilarityMR.class);
        //mrdemo/target/mrdemo-1.0-SNAPSHOT.jar
        //job.setJar("mrdemo/target/mrdemo-1.0-SNAPSHOT.jar");
        //设置Map和Reduce处理类
        job.setMapperClass(DocSimilarityMapper2.class);

        job.setReducerClass(DocSimilarityReducer2.class);


        //map输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        //设置job/reduce输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        //设置任务的输入路径
        FileInputFormat.addInputPath(job, new Path("/simout1/"));
        // FileInputFormat.addInputPath(job, new Path("/wc/"));
        FileOutputFormat.setOutputPath(job, new Path("/simout2/"));
        return job;

    }
    private static Job setJob3() throws IOException {
        Configuration coreSiteConf = new Configuration();


        coreSiteConf.addResource(Resources.getResource("core-site.xml"));
        //设置一个任务
        Job job = Job.getInstance(coreSiteConf, "wcoo");
        //设置job的运行类
        job.setJarByClass(DocSimilarityMR.class);
        //mrdemo/target/mrdemo-1.0-SNAPSHOT.jar
        //job.setJar("mrdemo/target/mrdemo-1.0-SNAPSHOT.jar");
        //设置Map和Reduce处理类
        job.setMapperClass(DocSimilarityMapper3.class);

        job.setReducerClass(DocSimilarityReducer3.class);


        //map输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        //设置job/reduce输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        //设置任务的输入路径
        FileInputFormat.addInputPath(job, new Path("/simout2/"));
        // FileInputFormat.addInputPath(job, new Path("/wc/"));
        FileOutputFormat.setOutputPath(job, new Path("/simout3/"));
        return job;

    }

    private static Job setJob1() throws IOException {
        Configuration coreSiteConf = new Configuration();

        //coreSiteConf.addResource(Resources.getResource("core-site.xml"));
        coreSiteConf.addResource(Resources.getResource("core-site.xml"));

        //coreSiteConf.set("sim.docs.name","doc1,doc2");
        coreSiteConf.set("sim.docs.input","/siminput");
        //设置一个任务
        //new JobConf()
        Job job1 = Job.getInstance(coreSiteConf, "job1");
        //设置job的运行类
        job1.setJarByClass(DocSimilarityMR.class);
        //mrdemo/target/mrdemo-1.0-SNAPSHOT.jar
        //job1.setJar("mrdemo/target/mrdemo-1.0-SNAPSHOT.jar");
        //设置Map和Reduce处理类
        job1.setMapperClass(DocSimilarityMapper1.class);
        job1.setReducerClass(DocSimilarityReducer1.class);

        //map输出类型
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(IntWritable.class);
        //设置job/reduce输出类型
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);


        FileSystem fileSystem = FileSystem.get(coreSiteConf);


        RemoteIterator<LocatedFileStatus> listFiles = fileSystem.listFiles(new Path("/"), true);



        //设置任务的输入路径

         FileInputFormat.addInputPath(job1, new Path("/siminput/"));

        FileOutputFormat.setOutputPath(job1, new Path("/simout1/"));
        //运行任务
        return job1;
    }

    public static class DocSimilarityMapper1 extends Mapper<LongWritable, Text, Text, IntWritable> {
        List<String> fileNames=new ArrayList<String>();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
           // "/siminput";
            String input = context.getConfiguration().get("sim.docs.input");

            RemoteIterator<LocatedFileStatus> remoteIterator = FileSystem.get(context.getConfiguration()).listFiles(new Path(input), true);
            while (remoteIterator.hasNext()){
                LocatedFileStatus fileStatus = remoteIterator.next();
                if (fileStatus.isFile()){
                    fileNames.add(fileStatus.getPath().getName());
                }

            }
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //a.txt hello world hello henan
            //b.txt hello world hello zhengzhou
//   目的     hello@a.txt     2
//            hello@b.txt     2
//            henan@a.txt     1
//            henan@b.txt     0
//            world@a.txt     1
//            world@b.txt     1
//            zhengzhou@a.txt 0
//            zhengzhou@b.txt 1


            String[] strings = value.toString().split(" ");
            FileSplit fileSplit = (FileSplit) context.getInputSplit();
            String fileName = fileSplit.getPath().getName();
            for (String word :
                    strings) {
                for (int i = 0; i < fileNames.size(); i++) {
                    String cachedFileName = fileNames.get(i);
                    if (cachedFileName.equals(fileName)){
                        context.write(new Text(word + "@" + cachedFileName), new IntWritable(1));

                    }else{
                        context.write(new Text(word + "@" + cachedFileName), new IntWritable(0));
                    }
                }

            }
        }
    }

    public static class DocSimilarityReducer1 extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int count = 0;
            Iterator<IntWritable> iterator = values.iterator();
            while (iterator.hasNext()) {
                IntWritable one = iterator.next();
                count += one.get();
            }
            context.write(key, new IntWritable(count));
        }
    }

    public static class DocSimilarityMapper2 extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
//            hello@a.txt     2
//            hello@b.txt     2
//            henan@a.txt     1
//            henan@b.txt     0
//            world@a.txt     1
//            world@b.txt     1
//            zhengzhou@a.txt 0
//            zhengzhou@b.txt 1

            String[] kv = value.toString().split("\t");
            String word = kv[0].split("@")[0];
            context.write(new Text(word), new Text(kv[0].split("@")[1] + "@" + kv[1]));

        }
    }
    //hello 1.doc@2 2.doc@1 3.doc@3

    //hi 1.doc@1 2.doc@2

    //hello <1.doc,3.doc> <2,3>


    //<1.doc,2.doc> <2,1>
    //<1.doc,3.doc> <2,3>
    //<2.doc,3.doc> <1,3>
    public static class DocSimilarityReducer2 extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            StringBuilder stringBuilder = new StringBuilder();

            Iterator<Text> iterator = values.iterator();
            while (iterator.hasNext()) {
                stringBuilder.append(iterator.next().toString()).append("\t");
            }
            context.write(key, new Text(stringBuilder.toString()));
        }
    }

    public static class DocSimilarityMapper3 extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
 //           hello   b.txt@2 a.txt@2
//            henan   b.txt@0 a.txt@1
//            world   b.txt@1 a.txt@1
//            zhengzhou       b.txt@1 a.txt@0

//   目的         <a.txt,b.txt>   0.8333333333333334

            String[] strings = value.toString().split("\t");
            //
            TreeMap<String, String> map = new TreeMap<>();

            for (int i = 1; i < strings.length; i++) {
                String[] kv = strings[i].split("@");
                map.put(kv[0], kv[1]);
            }
            Set<String> fileNames = map.keySet();

            Iterator<String> iterator1 = fileNames.iterator();

            while (iterator1.hasNext()) {
                String fileName1 = iterator1.next();
                Iterator<String> iterator2 = fileNames.iterator();
                while (iterator2.hasNext()) {
                    String fileName2 = iterator2.next();
                    if (fileName1.compareTo(fileName2) < 0) {
                        context.write(new Text("<" + fileName1 + "," + fileName2 + ">"), new Text("<" + map.get(fileName1) + "," + map.get(fileName2) + ">"));
                    }
                }
            }
        }
    }
    //input :<1.doc,2.doc> [<2,1>,<1,2>....]

    public static class DocSimilarityReducer3 extends Reducer<Text, Text, Text, DoubleWritable> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Iterator<Text> iterator = values.iterator();
            ArrayList<String> doc1 = new ArrayList<>();
            ArrayList<String> doc2 = new ArrayList<>();

            while (iterator.hasNext()) {
                Text kv = iterator.next();
                String[] strings = kv.toString().substring(1, kv.toString().length() - 1).split(",");
                doc1.add(strings[0]);
                doc2.add(strings[1]);
            }

            //计算余弦
            //doc1:a1,b1,c1
            //doc2:a2,b2,c2
            int numerator = 0;
            for (int i = 0; i < doc1.size(); i++) {
                numerator += Integer.parseInt(doc1.get(i)) * Integer.parseInt(doc2.get(i));
            }
            //计算分母  每个元素的平方和
            int denominator = 0;
            int part1 = 0;
            int part2 = 0;
            for (int i = 0; i < doc1.size(); i++) {
                part1 += Integer.parseInt(doc1.get(i)) * Integer.parseInt(doc1.get(i));
                part2 += Integer.parseInt(doc2.get(i)) * Integer.parseInt(doc2.get(i));

            }
            double cos = Math.pow((numerator * numerator) / (part1 * part2 + 0.0), 0.5);

            context.write(key, new DoubleWritable(cos));

        }
    }


}
