package com.echoarchive.storage;

import com.echoarchive.config.EchoArchiveProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.AbortMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.ChecksumAlgorithm;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompletedMultipartUpload;
import software.amazon.awssdk.services.s3.model.CompletedPart;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.HeadBucketRequest;
import software.amazon.awssdk.services.s3.model.NoSuchBucketException;
import software.amazon.awssdk.services.s3.model.NoSuchUploadException;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.ServerSideEncryption;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;
import software.amazon.awssdk.services.s3.presigner.S3Presigner;
import software.amazon.awssdk.services.s3.presigner.model.GetObjectPresignRequest;

import java.net.URL;
import java.time.Duration;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

@Component
public class S3ObjectStore implements ObjectStore {

    private static final Logger log = LoggerFactory.getLogger(S3ObjectStore.class);

    private final S3Client s3;
    private final S3Presigner presigner;
    private final EchoArchiveProperties.Storage config;

    public S3ObjectStore(S3Client s3, S3Presigner presigner, EchoArchiveProperties properties) {
        this.s3 = s3;
        this.presigner = presigner;
        this.config = properties.storage();
    }

    @Override
    public String bucket() {
        return config.bucket();
    }

    @EventListener(ApplicationReadyEvent.class)
    public void onReady() {
        if (config.createBucketIfMissing()) {
            ensureBucket();
        }
    }

    @Override
    public void ensureBucket() {
        try {
            s3.headBucket(HeadBucketRequest.builder().bucket(bucket()).build());
        } catch (NoSuchBucketException e) {
            log.info("Creating bucket {}", bucket());
            s3.createBucket(CreateBucketRequest.builder().bucket(bucket()).build());
        } catch (S3Exception e) {
            if (e.statusCode() == 404) {
                log.info("Creating bucket {}", bucket());
                s3.createBucket(CreateBucketRequest.builder().bucket(bucket()).build());
            } else {
                throw e;
            }
        }
    }

    @Override
    public boolean isReachable() {
        try {
            s3.headBucket(HeadBucketRequest.builder().bucket(bucket()).build());
            return true;
        } catch (RuntimeException e) {
            log.warn("Object store not reachable: {}", e.getMessage());
            return false;
        }
    }

    @Override
    public String put(String key, String contentType, byte[] data, Map<String, String> metadata) {
        PutObjectRequest.Builder req = PutObjectRequest.builder()
                .bucket(bucket())
                .key(key)
                .contentType(contentType)
                .contentLength((long) data.length)
                .metadata(metadata)
                .checksumAlgorithm(ChecksumAlgorithm.SHA256);
        applyEncryption(req);
        return s3.putObject(req.build(), RequestBody.fromBytes(data)).eTag();
    }

    @Override
    public String createMultipartUpload(String key, String contentType, Map<String, String> metadata) {
        CreateMultipartUploadRequest.Builder req = CreateMultipartUploadRequest.builder()
                .bucket(bucket())
                .key(key)
                .contentType(contentType)
                .metadata(metadata);
        if (isAws()) {
            req.serverSideEncryption(ServerSideEncryption.AES256);
        }
        return s3.createMultipartUpload(req.build()).uploadId();
    }

    @Override
    public String uploadPart(String key, String uploadId, int partNumber, byte[] data) {
        UploadPartRequest req = UploadPartRequest.builder()
                .bucket(bucket())
                .key(key)
                .uploadId(uploadId)
                .partNumber(partNumber)
                .contentLength((long) data.length)
                .build();
        return s3.uploadPart(req, RequestBody.fromBytes(data)).eTag();
    }

    @Override
    public void completeMultipartUpload(String key, String uploadId, List<PartETag> parts) {
        List<CompletedPart> completed = parts.stream()
                .sorted(Comparator.comparingInt(PartETag::partNumber))
                .map(p -> CompletedPart.builder().partNumber(p.partNumber()).eTag(p.eTag()).build())
                .toList();
        s3.completeMultipartUpload(CompleteMultipartUploadRequest.builder()
                .bucket(bucket())
                .key(key)
                .uploadId(uploadId)
                .multipartUpload(CompletedMultipartUpload.builder().parts(completed).build())
                .build());
    }

    @Override
    public void abortMultipartUpload(String key, String uploadId) {
        try {
            s3.abortMultipartUpload(AbortMultipartUploadRequest.builder()
                    .bucket(bucket()).key(key).uploadId(uploadId).build());
        } catch (NoSuchUploadException e) {
            log.debug("Multipart upload {} already gone", uploadId);
        }
    }

    @Override
    public URL presignGet(String key, Duration ttl, String downloadFileName) {
        GetObjectPresignRequest req = GetObjectPresignRequest.builder()
                .signatureDuration(ttl)
                .getObjectRequest(g -> {
                    g.bucket(bucket()).key(key);
                    if (downloadFileName != null) {
                        g.responseContentDisposition("attachment; filename=\""
                                + downloadFileName.replace("\"", "") + "\"");
                    }
                })
                .build();
        return presigner.presignGetObject(req).url();
    }

    private void applyEncryption(PutObjectRequest.Builder req) {
        // SSE-S3 on AWS. On-prem stores typically handle encryption at the volume level and may
        // reject the header if KMS isn't configured.
        if (isAws()) {
            req.serverSideEncryption(ServerSideEncryption.AES256);
        }
    }

    private boolean isAws() {
        return config.endpoint() == null || config.endpoint().isBlank();
    }
}
