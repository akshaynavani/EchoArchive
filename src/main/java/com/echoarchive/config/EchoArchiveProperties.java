package com.echoarchive.config;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

import java.time.Duration;

/**
 * Central, typed configuration for EchoArchive. Bound from the {@code echoarchive.*} namespace.
 */
@Validated
@ConfigurationProperties(prefix = "echoarchive")
public record EchoArchiveProperties(
        Topics topics,
        Storage storage,
        Ingest ingest,
        Retrieval retrieval) {

    public record Topics(
            @NotBlank String mediaMessages,
            @NotBlank String recordingChunks,
            @NotBlank String mediaPersisted,
            @Min(1) int partitions,
            @Min(1) short replicationFactor) {
    }

    /**
     * @param bucket         target bucket for all media
     * @param region         AWS region (also used as signing region for MinIO)
     * @param endpoint       optional endpoint override; set for on-prem MinIO / LocalStack
     * @param pathStyleAccess required by most on-prem S3-compatible stores
     * @param accessKey      optional static key (on-prem); cloud uses the default credential chain
     * @param secretKey      optional static secret (on-prem)
     */
    public record Storage(
            @NotBlank String bucket,
            @NotBlank String region,
            String endpoint,
            boolean pathStyleAccess,
            String accessKey,
            String secretKey,
            boolean createBucketIfMissing) {
    }

    /**
     * @param maxMessageBytes     max payload size for a single multimedia message
     * @param maxChunkBytes       max size of one recording chunk
     * @param multipartPartBytes  recording bytes buffered before an S3 part is uploaded (>= 5 MiB)
     */
    public record Ingest(
            @Min(1) long maxMessageBytes,
            @Min(1) long maxChunkBytes,
            @Min(5 * 1024 * 1024) long multipartPartBytes) {
    }

    /**
     * @param presignTtl       lifetime of presigned download URLs
     * @param staleUploadAfter recordings with no chunk for this long are aborted
     */
    public record Retrieval(
            Duration presignTtl,
            Duration staleUploadAfter) {
    }
}
