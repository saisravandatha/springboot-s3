package com.sravandatha.springboots3.service;


import com.sravandatha.springboots3.domain.AccessType;
import com.sravandatha.springboots3.domain.S3ObjectInputStreamWrapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.http.SdkHttpMethod;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.s3.presigner.S3Presigner;
import software.amazon.awssdk.services.s3.presigner.model.GetObjectPresignRequest;
import software.amazon.awssdk.services.s3.presigner.model.PresignedGetObjectRequest;
import software.amazon.awssdk.services.s3.presigner.model.PresignedPutObjectRequest;
import software.amazon.awssdk.services.s3.presigner.model.PutObjectPresignRequest;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.Normalizer;
import java.time.Duration;

@Service
@Slf4j
public class FileService {


    @Value("${aws.bucket}")
    private String bucketName;

    private final S3Client s3Client;

    private final S3Presigner s3Presigner;

    public FileService(S3Client s3Client, S3Presigner s3Presigner) {
        this.s3Client = s3Client;
        this.s3Presigner = s3Presigner;
    }

    /*Generates a preassigned URL for GET or PUT operations with specified access*/

    public String generatePreSignedUrl(String filePath,
                                       SdkHttpMethod method,
                                       AccessType accessType) {
        if(method == SdkHttpMethod.GET) {
            return generateGetPreSignedUrl(filePath);
        } else if (method == SdkHttpMethod.PUT) {
            return generatePutPreSignedUrl(filePath, accessType);
        }else {
            throw new UnsupportedOperationException("Unsupported HTTP Method: " + method);
        }
    }


    /*Uploads a multipart file to S3 with specified access type*/
    public String uploadMultiPartFile(MultipartFile multipartFile, AccessType accessType) throws IOException {
        String fileName = buildFileName(multipartFile.getOriginalFilename());

        try(InputStream inputStream = multipartFile.getInputStream()){
            PutObjectRequest.Builder putObjectRequestBuilder = PutObjectRequest.builder()
                    .bucket(bucketName)
                    .key(fileName);

            if(accessType == AccessType.PUBLIC) {
                putObjectRequestBuilder.acl(ObjectCannedACL.PUBLIC_READ);
            }

            PutObjectRequest putObjectRequest = putObjectRequestBuilder.build();

            s3Client.putObject(putObjectRequest, RequestBody.fromInputStream(inputStream, multipartFile.getSize()));
        }
        return fileName;
    }

    /*
    * Downloads a file from S3 and returns an InputStream and ETag.
    * */
    public S3ObjectInputStreamWrapper downloadFile(String file){
        GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                .bucket(bucketName)
                .key(file).build();

        ResponseInputStream<GetObjectResponse> responseInputStream = s3Client.getObject(getObjectRequest);
        String eTag = responseInputStream.response().eTag();
        return new S3ObjectInputStreamWrapper(responseInputStream, eTag);
    }

    public static String buildFileName(String fileName) {
        return String.format("%s.%s", System.currentTimeMillis(), sanitizeFileName(fileName));
    }

    private static String sanitizeFileName(String fileName) {
        String newFileName = Normalizer.normalize(fileName, Normalizer.Form.NFD);
        return newFileName.replaceAll("\\s+", "_")
                .replaceAll("[^a-zA-Z0-9.\\\\-_]", "");
    }

    private String generatePutPreSignedUrl(String filePath, AccessType accessType) {
        PutObjectRequest.Builder putObjectRequestBuilder = PutObjectRequest.builder()
                .bucket(bucketName)
                .key(filePath);

        if (accessType == AccessType.PUBLIC) {
            putObjectRequestBuilder.acl(ObjectCannedACL.PUBLIC_READ);
        }

        PutObjectRequest putObjectRequest = putObjectRequestBuilder.build();

        PutObjectPresignRequest putObjectPresignRequest = PutObjectPresignRequest.builder()
                .signatureDuration(Duration.ofMinutes(60))
                .putObjectRequest(putObjectRequest)
                .build();

        log.info("preSigned put file: {}", putObjectPresignRequest);

        PresignedPutObjectRequest presignedPutObjectRequest = s3Presigner.presignPutObject(putObjectPresignRequest);

        log.info("preSignedPutObjectRequest: {}", presignedPutObjectRequest);

        return presignedPutObjectRequest.url().toString();
    }

    private String generateGetPreSignedUrl(String filePath) {
        GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                .bucket(bucketName)
                .key(filePath)
                .build();

        GetObjectPresignRequest getObjectPresignRequest = GetObjectPresignRequest.builder()
                .signatureDuration(Duration.ofMinutes(60))
                .getObjectRequest(getObjectRequest)
                .build();

        log.info("preSigned get file: {}", getObjectPresignRequest);

        PresignedGetObjectRequest presignedGetObjectRequest = s3Presigner.presignGetObject(getObjectPresignRequest);

        log.info("preSignedGetObjectRequest: {}", presignedGetObjectRequest);

        return presignedGetObjectRequest.url().toString();
    }

    public ResponseEntity<StreamingResponseBody> downloadFileResponse(String fileName) throws IOException {
        String contentType = Files.probeContentType(Paths.get(fileName));
        S3ObjectInputStreamWrapper wrapper = downloadFile(fileName);

        StreamingResponseBody streamingResponseBody = outputStream -> {
            try(InputStream inputStream = wrapper.inputStream()){
                byte[] buffer = new byte[8192];
                int bytesRead;
                while ((bytesRead = inputStream.read(buffer)) != -1) {
                    outputStream.write(buffer, 0, bytesRead);
                }
            }catch (IOException e){
                e.printStackTrace();
            }
        };
        HttpHeaders headers = new HttpHeaders();
        headers.add("Content-Type", contentType != null ? contentType : "application/octet-stream");
        headers.add("Cache-Control", "no-cache");
        headers.add(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=" + fileName);
        if(wrapper.eTag() != null){
            headers.add(HttpHeaders.ETAG, wrapper.eTag());
        }
        return ResponseEntity.ok()
                .headers(headers)
                .body(streamingResponseBody);
    }
}
