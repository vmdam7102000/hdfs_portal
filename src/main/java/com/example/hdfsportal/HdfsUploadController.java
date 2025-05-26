package com.example.hdfsportal;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.http.ResponseEntity;
import org.springframework.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@Controller
public class HdfsUploadController {

    // logger 

    // logger = LoggerFactory.getLogger(HdfsUploadController.class)
    private static final Logger logger;
    static {
        logger = LoggerFactory.getLogger(HdfsUploadController.class);
    }

    @Autowired
    private HdfsService hdfsService;

    @GetMapping("/upload")
    public String showUploadForm() {
        return "upload";
    }

    @PostMapping("/upload")
    public ResponseEntity<?> uploadFile(@RequestParam("file") MultipartFile file,
                                        @RequestParam("username") String username) {
        String userPath = "/user/data/" + username + "/" + file.getOriginalFilename();
        logger.info("Uploading file to HDFS at path: " + userPath);
        String message;
        try {
            hdfsService.uploadFileToHdfs(userPath, file);
            message = "Upload thành công: " + file.getOriginalFilename();
            return ResponseEntity.ok(message);
        } catch (Exception e) {
            message = "Lỗi upload: " + e.getMessage();
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(message);
        }
    }



}