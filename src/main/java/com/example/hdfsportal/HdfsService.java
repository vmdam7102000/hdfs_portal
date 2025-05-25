package com.example.hdfsportal;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.commons.io.IOUtils;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import java.io.InputStream;
import java.net.URI;

@Service
public class HdfsService {

    // Thay đổi các thông tin này cho phù hợp với cluster HDFS của bạn
    private static final String HDFS_URI = "hdfs://localhost:9000";
    private static final String HDFS_UPLOAD_DIR = "/user/upload/";

    public void uploadFileToHdfs(MultipartFile file) throws Exception {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", HDFS_URI);

        try (InputStream inputStream = file.getInputStream();
             FileSystem fs = FileSystem.get(new URI(HDFS_URI), conf)) {

            Path hdfsPath = new Path(HDFS_UPLOAD_DIR + file.getOriginalFilename());
            try (FSDataOutputStream outputStream = fs.create(hdfsPath, true)) {
                IOUtils.copy(inputStream, outputStream);
            }
        }
    }
}
