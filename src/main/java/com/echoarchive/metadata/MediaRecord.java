package com.echoarchive.metadata;

import com.echoarchive.domain.MediaKind;
import com.echoarchive.domain.Priority;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.time.Instant;

/**
 * Searchable index entry for every object persisted to storage (messages and recordings).
 */
@Entity
@Table(name = "media_record")
public class MediaRecord {

    @Id
    @Column(name = "media_id", length = 64)
    private String mediaId;

    @Column(nullable = false, length = 64)
    private String agency;

    @Column(name = "incident_id", nullable = false, length = 64)
    private String incidentId;

    @Column(name = "sender_id", length = 64)
    private String senderId;

    @Column(name = "call_id", length = 64)
    private String callId;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 32)
    private MediaKind kind;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 16)
    private Priority priority;

    @Column(name = "content_type", nullable = false, length = 128)
    private String contentType;

    @Column(name = "file_name", length = 255)
    private String fileName;

    @Column(name = "size_bytes", nullable = false)
    private long sizeBytes;

    @Column(nullable = false, length = 64)
    private String sha256;

    @Column(nullable = false, length = 128)
    private String bucket;

    @Column(name = "object_key", nullable = false, length = 512)
    private String objectKey;

    @Column(name = "captured_at", nullable = false)
    private Instant capturedAt;

    @Column(name = "received_at", nullable = false)
    private Instant receivedAt;

    @Column(name = "persisted_at", nullable = false)
    private Instant persistedAt;

    protected MediaRecord() {
    }

    private MediaRecord(Builder b) {
        this.mediaId = b.mediaId;
        this.agency = b.agency;
        this.incidentId = b.incidentId;
        this.senderId = b.senderId;
        this.callId = b.callId;
        this.kind = b.kind;
        this.priority = b.priority;
        this.contentType = b.contentType;
        this.fileName = b.fileName;
        this.sizeBytes = b.sizeBytes;
        this.sha256 = b.sha256;
        this.bucket = b.bucket;
        this.objectKey = b.objectKey;
        this.capturedAt = b.capturedAt;
        this.receivedAt = b.receivedAt;
        this.persistedAt = b.persistedAt;
    }

    public static Builder builder() {
        return new Builder();
    }

    public String getMediaId() { return mediaId; }
    public String getAgency() { return agency; }
    public String getIncidentId() { return incidentId; }
    public String getSenderId() { return senderId; }
    public String getCallId() { return callId; }
    public MediaKind getKind() { return kind; }
    public Priority getPriority() { return priority; }
    public String getContentType() { return contentType; }
    public String getFileName() { return fileName; }
    public long getSizeBytes() { return sizeBytes; }
    public String getSha256() { return sha256; }
    public String getBucket() { return bucket; }
    public String getObjectKey() { return objectKey; }
    public Instant getCapturedAt() { return capturedAt; }
    public Instant getReceivedAt() { return receivedAt; }
    public Instant getPersistedAt() { return persistedAt; }

    public static final class Builder {
        private String mediaId;
        private String agency;
        private String incidentId;
        private String senderId;
        private String callId;
        private MediaKind kind;
        private Priority priority = Priority.ROUTINE;
        private String contentType;
        private String fileName;
        private long sizeBytes;
        private String sha256;
        private String bucket;
        private String objectKey;
        private Instant capturedAt;
        private Instant receivedAt;
        private Instant persistedAt;

        public Builder mediaId(String v) { this.mediaId = v; return this; }
        public Builder agency(String v) { this.agency = v; return this; }
        public Builder incidentId(String v) { this.incidentId = v; return this; }
        public Builder senderId(String v) { this.senderId = v; return this; }
        public Builder callId(String v) { this.callId = v; return this; }
        public Builder kind(MediaKind v) { this.kind = v; return this; }
        public Builder priority(Priority v) { this.priority = v; return this; }
        public Builder contentType(String v) { this.contentType = v; return this; }
        public Builder fileName(String v) { this.fileName = v; return this; }
        public Builder sizeBytes(long v) { this.sizeBytes = v; return this; }
        public Builder sha256(String v) { this.sha256 = v; return this; }
        public Builder bucket(String v) { this.bucket = v; return this; }
        public Builder objectKey(String v) { this.objectKey = v; return this; }
        public Builder capturedAt(Instant v) { this.capturedAt = v; return this; }
        public Builder receivedAt(Instant v) { this.receivedAt = v; return this; }
        public Builder persistedAt(Instant v) { this.persistedAt = v; return this; }

        public MediaRecord build() {
            return new MediaRecord(this);
        }
    }
}
