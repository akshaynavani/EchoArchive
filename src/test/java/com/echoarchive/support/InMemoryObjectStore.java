package com.echoarchive.support;

import com.echoarchive.storage.ObjectStore;

import java.io.ByteArrayOutputStream;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Test double that mimics S3 multipart semantics closely enough to catch ordering/size bugs:
 * non-final parts must be at least 5 MiB, and parts are concatenated in part-number order.
 */
public class InMemoryObjectStore implements ObjectStore {

    private static final int MIN_PART = 5 * 1024 * 1024;

    public final Map<String, byte[]> objects = new ConcurrentHashMap<>();
    public final Map<String, TreeMap<Integer, byte[]>> uploads = new ConcurrentHashMap<>();
    public final Map<String, String> uploadKeys = new ConcurrentHashMap<>();

    @Override
    public String bucket() {
        return "test-bucket";
    }

    @Override
    public void ensureBucket() {
    }

    @Override
    public boolean isReachable() {
        return true;
    }

    @Override
    public String put(String key, String contentType, byte[] data, Map<String, String> metadata) {
        objects.put(key, data.clone());
        return "etag-" + key.hashCode();
    }

    @Override
    public String createMultipartUpload(String key, String contentType, Map<String, String> metadata) {
        String id = UUID.randomUUID().toString();
        uploads.put(id, new TreeMap<>());
        uploadKeys.put(id, key);
        return id;
    }

    @Override
    public String uploadPart(String key, String uploadId, int partNumber, byte[] data) {
        requireUpload(key, uploadId).put(partNumber, data.clone());
        return "etag-" + partNumber;
    }

    @Override
    public void completeMultipartUpload(String key, String uploadId, List<PartETag> parts) {
        TreeMap<Integer, byte[]> stored = requireUpload(key, uploadId);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        for (int i = 0; i < parts.size(); i++) {
            byte[] part = stored.get(parts.get(i).partNumber());
            if (part == null) {
                throw new IllegalStateException("Missing part " + parts.get(i).partNumber());
            }
            if (i < parts.size() - 1 && part.length < MIN_PART) {
                throw new IllegalStateException("EntityTooSmall: part " + parts.get(i).partNumber());
            }
            out.writeBytes(part);
        }
        objects.put(key, out.toByteArray());
        uploads.remove(uploadId);
    }

    @Override
    public void abortMultipartUpload(String key, String uploadId) {
        uploads.remove(uploadId);
    }

    @Override
    public URL presignGet(String key, Duration ttl, String downloadFileName) {
        try {
            return URI.create("http://minio.test/" + bucket() + "/" + key + "?X-Amz-Expires=" + ttl.toSeconds())
                    .toURL();
        } catch (MalformedURLException e) {
            throw new IllegalStateException(e);
        }
    }

    private TreeMap<Integer, byte[]> requireUpload(String key, String uploadId) {
        TreeMap<Integer, byte[]> parts = uploads.get(uploadId);
        if (parts == null || !key.equals(uploadKeys.get(uploadId))) {
            throw new IllegalStateException("NoSuchUpload " + uploadId);
        }
        return parts;
    }
}
