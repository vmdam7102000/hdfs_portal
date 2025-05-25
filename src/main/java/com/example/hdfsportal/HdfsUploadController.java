package com.example.hdfsportal;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.multipart.MultipartFile;

@Controller
public class HdfsUploadController {

    @Autowired
    private HdfsService hdfsService;

    @GetMapping("/upload")
    public String showUploadForm() {
        return "upload";
    }

    @PostMapping("/upload")
    public String handleFileUpload(@RequestParam("file") MultipartFile file, Model model) {
        String message;
        try {
            hdfsService.uploadFileToHdfs(file);
            message = "Upload thành công: " + file.getOriginalFilename();
        } catch (Exception e) {
            message = "Lỗi upload: " + e.getMessage();
        }
        model.addAttribute("message", message);
        return "upload";
    }
}
