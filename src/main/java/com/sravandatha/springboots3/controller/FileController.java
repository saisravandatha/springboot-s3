package com.sravandatha.springboots3.controller;


import com.sravandatha.springboots3.domain.AccessType;
import com.sravandatha.springboots3.service.FileService;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody;
import software.amazon.awssdk.http.SdkHttpMethod;

import java.io.IOException;
import java.util.Map;

import static com.sravandatha.springboots3.service.FileService.buildFileName;

@RestController
@RequestMapping("/files")
public class FileController {

    private final FileService fileService;

    private FileController(FileService fileService){
        this.fileService = fileService;
    }


    @GetMapping("/{fileName}")
    public ResponseEntity<Map<String,String>> getUrl(@PathVariable String fileName){
        String url = fileService.generatePreSignedUrl(fileName, SdkHttpMethod.GET, null);
        return ResponseEntity.ok()
                .body(Map.of("url", url));
    }

    @PostMapping("/pre-signed-url")
    public ResponseEntity<Map<String, Object>> generateUrl(@RequestParam(name = "filename", required = false, defaultValue = "") String filename,
                                                           @RequestParam(name = "accessType", required = false, defaultValue = "PRIVATE") AccessType accessType) {
        filename = buildFileName(filename);
        String url = fileService.generatePreSignedUrl(filename, SdkHttpMethod.PUT, accessType);
        return ResponseEntity.ok(Map.of("url", url, "file", filename));
    }

    @GetMapping("/download/{fileName}")
    public ResponseEntity<StreamingResponseBody> downloadFile(@PathVariable String fileName) throws IOException {
        return fileService.downloadFileResponse(fileName);
    }

    @PostMapping("/upload")
    public ResponseEntity<String> uploadFile(@RequestParam("file") MultipartFile file,
                                @RequestParam(name = "accessType", required = false, defaultValue = "PRIVATE") AccessType accessType) throws IOException {
        if (file.isEmpty()) {
            return ResponseEntity.badRequest().body("Empty file");
        }
        String fileName = fileService.uploadMultiPartFile(file, accessType);
        return ResponseEntity.ok(fileName);
    }

}
