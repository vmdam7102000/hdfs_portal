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
    private static final String HDFS_UPLOAD_DIR = "";

    public void uploadFileToHdfs(String path, MultipartFile file) throws Exception {
        // Tạo cấu hình HDFS
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", HDFS_URI);
        
        // Tạo đối tượng FileSystem
        FileSystem fileSystem = FileSystem.get(URI.create(HDFS_URI), configuration);

        // Tạo đường dẫn HDFS
        Path hdfsPath = new Path(HDFS_UPLOAD_DIR + path);
        
        // Kiểm tra xem thư mục đã tồn tại chưa, nếu không thì tạo mới
        if (!fileSystem.exists(hdfsPath.getParent())) {
            fileSystem.mkdirs(hdfsPath.getParent());
        }

        // Mở luồng đầu ra để ghi dữ liệu vào HDFS
        try (InputStream inputStream = file.getInputStream();
             FSDataOutputStream outputStream = fileSystem.create(hdfsPath)) {
            IOUtils.copy(inputStream, outputStream);
        } finally {
            fileSystem.close();
        }
    }
}
