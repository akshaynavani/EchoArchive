package com.echoarchive.storage;

import java.net.URL;
import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * Minimal object-storage contract used by the pipeline. Implemented over S3; kept narrow so
 * tests can swap in an in-memory fake.
 */
public interface ObjectStore {

    String bucket();

    void ensureBucket();

    boolean isReachable();

    /** @return the object's ETag */
    String put(String key, String contentType, byte[] data, Map<String, String> metadata);

    /** @return the multipart upload ID */
    String createMultipartUpload(String key, String contentType, Map<String, String> metadata);

    /** @return the part ETag */
    String uploadPart(String key, String uploadId, int partNumber, byte[] data);

    void completeMultipartUpload(String key, String uploadId, List<PartETag> parts);

    void abortMultipartUpload(String key, String uploadId);

    URL presignGet(String key, Duration ttl, String downloadFileName);

    record PartETag(int partNumber, String eTag) {
    }
}
