package com.pretty.doccloud.doccloudweb.controller;

import com.pretty.doccloud.doccloudweb.dao.DocRepository;
import com.pretty.doccloud.doccloudweb.entity.Doc;

import com.pretty.doccloud.doccloudweb.entity.DocJobEntity;
import com.pretty.doccloud.doccloudweb.service.DocJobService;
import com.pretty.doccloud.doccloudweb.service.DocService;
import com.pretty.doccloud.doccloudweb.util.HdfsUtil;
import com.pretty.doccloud.doccloudweb.util.MD5Util;
import com.pretty.doccloud.job.DocJob;
import com.pretty.doccloud.job.DocJobType;
import com.pretty.doccloud.job.JobDaemonService;
import com.pretty.doccloud.job.JobStatus;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.ipc.RPC;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.multipart.MultipartFile;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.*;

import java.net.InetSocketAddress;
import java.net.URISyntaxException;
import java.text.SimpleDateFormat;
import java.util.*;

/*
*@ClassName:DocController
 @Description:TODO
 @Author:
 @Date:2018/10/29 11:22 
 @Version:v1.0
*/
@Controller
@RequestMapping("/doc")
@Slf4j
public class DocController {
    @Autowired
    private DocRepository docRepository;
    @Autowired
    private DocService docService;
    @Autowired
    private DocJobService docJobService;

    public static final String[] DOC_SUFFIXS = new String[]{"doc", "docx", "ppt", "pptx", "txt", "xls", "xlsx", "pdf"};
    public static final int DOC_MAX_SIZE = 128 * 1024 * 1024;
    public static final String HOME = "hdfs://192.168.18.3:9000/doccloud/";

    @RequestMapping("/doclist")
    @ResponseBody
    Doc docList() {
        Optional<Doc> doc = docRepository.findById(1);
        if (doc.isPresent()) {
            return doc.get();
        }
        return null;

    }
    @RequestMapping("/recommmend")
    @ResponseBody
    public List<Map> recommmend(){
        HashMap<String, String> map = new HashMap<>();
        map.put("coverUrl","tmp/pdf.jpg");
        map.put("docUrl","/detail.html?md5=5314004295490880485d1587f5d0577c");
        ArrayList<Map> maps = new ArrayList<>();
        maps.add(map);
        return maps;
    }


    @RequestMapping("/download")
    //@ResponseBody
    public void download(String md5, HttpServletResponse response, HttpServletRequest request) throws IOException, URISyntaxException {
        Optional<Doc> docOptional = docService.findbyMd5(md5);
        if (docOptional.isPresent()) {
            Doc doc = docOptional.get();
            String docName = doc.getDocName();
            String docPath=doc.getDocDir()+"/"+doc.getDocName();
            log.info("doc path:{}",docPath);

            response.setCharacterEncoding(request.getCharacterEncoding());
            response.setContentType("application/octet-stream");
            FSDataInputStream dataInputStream = FileSystem.get(new Configuration()).open(new Path(docPath));
            try {
                response.setHeader("Content-Disposition", "attachment; filename=" + docName);
                IOUtils.copy(dataInputStream, response.getOutputStream());
                response.flushBuffer();
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                if (dataInputStream != null) {
                    try {
                        dataInputStream.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }

            }
        }
        //return "";
    }

    @RequestMapping(value = "/view",method =RequestMethod.POST)
    @ResponseBody
    public String view(String md5, HttpServletResponse response, HttpServletRequest request) throws IOException, URISyntaxException {
        Optional<Doc> docOptional = docService.findbyMd5(md5);
        if (docOptional.isPresent()) {
            Doc doc = docOptional.get();
            String docName = doc.getDocName();
            String viewName = docName.substring(0, docName.indexOf(".")) + ".pdf";
            String viewPath = doc.getDocDir() + "/" + viewName;
            log.info("view path:{}",viewPath);
            String classPath=DocController.class.getClassLoader().getResource("").getPath();
            String relativeViewPath="tmp/"+UUID.randomUUID().toString()+"/"+viewName;
            String tmpViewPath=classPath+"/static/"+relativeViewPath;
            HdfsUtil.download(viewPath,tmpViewPath);
            return relativeViewPath;
        }
        return null;
    }


    @RequestMapping("/helloworld")
    @ResponseBody
    String helloworld() {
        return "helloworld";
    }

    @ResponseBody
    @RequestMapping("/upload")
    public String upload(@RequestParam("file") MultipartFile file) throws IOException {
        if (file.isEmpty()) {
            return "file is empty";
        }
        //b.服务端校验文件后缀是否符合文档格式。
        String filename = file.getOriginalFilename();
        //得到文件的后缀名
        String[] strings = filename.split("\\.");
        if (strings.length == 1) {
            return "file does not has suffix";
        }
        String suffix = strings[1];
        log.info("doc suffix is {}", suffix);
        //判断文件后缀是否合法
        boolean flag = isSuffixLegal(suffix);
        if (!flag) {
            return "file is illegal";
        }

        try {
            //判断文件大小是否符合标准
            byte[] bytes = file.getBytes();
            log.info("file size is {}", bytes.length);
            if (bytes.length > DOC_MAX_SIZE) {
                return "file is large,max size is " + DOC_MAX_SIZE;
            }
            //计算文档md5值
            String md5 = getMd5(bytes);
            log.info("file md5 is {}", md5);
            //从数据库校验md5
            Optional<Doc> doc = docService.findbyMd5(md5);
            if (doc.isPresent()) {
                Doc docEntity = doc.get();
                //没有登录，先模拟数据
                docEntity.setUserId(new Random().nextInt());
                //docEntity.setId();
                //docRepository.save(docEntity);
                docService.save(docEntity);
                //docService.
            } else {
                //文库中没有此文档 需要上传
                //生成文件存放目录路径
                //获取当前日期
                String date = getDate();
                String dst = HOME + "/" + date + "/" + UUID.randomUUID().toString() + "/";
                log.info("dst {}", dst);
                HdfsUtil.upload(bytes, file.getOriginalFilename(), dst);
                //保存文档元数据
                Doc docEntity = new Doc();
                docEntity.setUserId(new Random().nextInt());
                docEntity.setDocComment("hadoop");
                docEntity.setDocDir(dst);
                docEntity.setDocName(filename);
                docEntity.setDocSize(bytes.length);
                docEntity.setDocPermission("1");
                docEntity.setDocType(suffix);
                docEntity.setDocStatus("upload");
                docEntity.setMd5(md5);
                docEntity.setDocCreateTime(new Date());
                Doc savedDoc = docService.save(docEntity);
                //上传成功以后需要提交文档转换任务
                //转换成html,
                DocJob docJob = submitDocJob(savedDoc, new Random().nextInt());
                //保存job信息
                 saveDocJob(docJob);
            }

            /*Path path = Paths.get("d:\\doccloud\\" + file.getOriginalFilename());

            Files.write(path, bytes);

            log.info("upload file {} ", file.getOriginalFilename());*/

        } catch (IOException e) {
            e.printStackTrace();
        }

        return "upload success";
    }

    private void saveDocJob(DocJob docJob) {
        DocJobEntity docJobEntity = new DocJobEntity();
        docJobEntity.setId(docJob.getId());
        docJobEntity.setJobStatus("running");
        docJobEntity.setSubmitTime(docJob.getSubmitTime());
        docJobEntity.setDocId(docJob.getDocId());
        docJobEntity.setInput(docJob.getInput());
        docJobEntity.setOutput(docJobEntity.getOutput());
        docJobEntity.setName(docJob.getName());
        docJobEntity.setRetryTime(docJob.getRetryTime());
        docJobEntity.setJobType(docJob.getJobType().name());
        docJobEntity.setFileName(docJob.getFileName());
        docJobEntity.setUserId(docJob.getUserId());
        docJobService.save(docJobEntity);
    }

    private DocJob submitDocJob(Doc docEntity, int userId) throws IOException {
        DocJob docJob;
        docJob = new DocJob();
        docJob.setName("doc convert");
        docJob.setId(UUID.randomUUID().toString());
        //设置文件路径
        docJob.setInput(docEntity.getDocDir() + "/" + docEntity.getDocName());
        docJob.setOutput(docEntity.getDocDir());
        docJob.setUserId(userId);
        docJob.setSubmitTime(System.nanoTime());
        docJob.setRetryTime(2);
        docJob.setFileName(docEntity.getDocName());
        docJob.setJobStatus(JobStatus.SUBMIT);
        docJob.setJobType(DocJobType.DOC_JOB_CONVERT);
        docJob.setDocId(docEntity.getId());
        //保存job元数据，防止任务出错
        JobDaemonService jobDaemonService = RPC.getProxy(JobDaemonService.class, 1L, new InetSocketAddress("localhost", 7788), new Configuration());
        log.info("submit job:{}", docJob);
        jobDaemonService.submitDocJob(docJob);
        return docJob;


    }

    //获取当前日期
    private String getDate() {
        Date date = new Date();
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
        return simpleDateFormat.format(date);
    }

    //获取文件的md5
    private String getMd5(byte[] bytes) {
        return MD5Util.getMD5String(bytes);

    }

    private boolean isSuffixLegal(String suffix) {
        for (String docSuffix :
                DOC_SUFFIXS) {
            if (suffix.equals(docSuffix)) {
                return true;
            }
        }
        return false;
    }
}
