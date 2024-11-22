package com.echoarchive.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.retry.RetryMode;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.presigner.S3Presigner;

import java.net.URI;

/**
 * Builds the S3 client for either AWS (cloud profile) or an S3-compatible on-prem store such as
 * MinIO (onprem profile). The only differences are endpoint override, path-style addressing and
 * where credentials come from.
 */
@Configuration
public class S3Config {

    private final EchoArchiveProperties.Storage storage;

    public S3Config(EchoArchiveProperties properties) {
        this.storage = properties.storage();
    }

    @Bean(destroyMethod = "close")
    public S3Client s3Client() {
        var builder = S3Client.builder()
                .region(Region.of(storage.region()))
                .credentialsProvider(credentials())
                .serviceConfiguration(s3Configuration())
                .overrideConfiguration(o -> o.retryStrategy(RetryMode.STANDARD));
        if (hasEndpoint()) {
            builder.endpointOverride(URI.create(storage.endpoint()));
        }
        return builder.build();
    }

    @Bean(destroyMethod = "close")
    public S3Presigner s3Presigner() {
        var builder = S3Presigner.builder()
                .region(Region.of(storage.region()))
                .credentialsProvider(credentials())
                .serviceConfiguration(s3Configuration());
        if (hasEndpoint()) {
            builder.endpointOverride(URI.create(storage.endpoint()));
        }
        return builder.build();
    }

    private S3Configuration s3Configuration() {
        return S3Configuration.builder()
                .pathStyleAccessEnabled(storage.pathStyleAccess())
                .build();
    }

    private AwsCredentialsProvider credentials() {
        if (notBlank(storage.accessKey()) && notBlank(storage.secretKey())) {
            return StaticCredentialsProvider.create(
                    AwsBasicCredentials.create(storage.accessKey(), storage.secretKey()));
        }
        return DefaultCredentialsProvider.create();
    }

    private boolean hasEndpoint() {
        return notBlank(storage.endpoint());
    }

    private static boolean notBlank(String s) {
        return s != null && !s.isBlank();
    }
}
