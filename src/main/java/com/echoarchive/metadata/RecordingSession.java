package com.echoarchive.metadata;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.persistence.Version;

import java.time.Instant;

/**
 * Tracks one call recording across both sides of the pipeline:
 * the ingest side advances {@code lastIngestedSequence}; the persistence side advances
 * {@code lastPersistedSequence} and owns the S3 multipart upload state.
 */
@Entity
@Table(name = "recording_session")
public class RecordingSession {

    @Id
    @Column(name = "recording_id", length = 64)
    private String recordingId;

    @Column(nullable = false, length = 64)
    private String agency;

    @Column(name = "incident_id", nullable = false, length = 64)
    private String incidentId;

    @Column(name = "call_id", nullable = false, length = 64)
    private String callId;

    @Column(name = "content_type", nullable = false, length = 128)
    private String contentType;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 16)
    private RecordingStatus status;

    @Column(name = "last_ingested_sequence", nullable = false)
    private long lastIngestedSequence = -1;

    @Column(name = "last_persisted_sequence", nullable = false)
    private long lastPersistedSequence = -1;

    @Column(name = "bytes_ingested", nullable = false)
    private long bytesIngested;

    @Column(name = "object_key", length = 512)
    private String objectKey;

    @Column(name = "upload_id", length = 256)
    private String uploadId;

    @Column(name = "next_part_number", nullable = false)
    private int nextPartNumber = 1;

    @Column(name = "started_at", nullable = false)
    private Instant startedAt;

    @Column(name = "last_activity_at", nullable = false)
    private Instant lastActivityAt;

    @Column(name = "completed_at")
    private Instant completedAt;

    @Version
    private long version;

    protected RecordingSession() {
    }

    public RecordingSession(String recordingId, String agency, String incidentId, String callId,
                            String contentType, Instant startedAt) {
        this.recordingId = recordingId;
        this.agency = agency;
        this.incidentId = incidentId;
        this.callId = callId;
        this.contentType = contentType;
        this.status = RecordingStatus.OPEN;
        this.startedAt = startedAt;
        this.lastActivityAt = startedAt;
    }

    public void markChunkIngested(long sequence, long bytes, Instant at) {
        this.lastIngestedSequence = sequence;
        this.bytesIngested += bytes;
        this.lastActivityAt = at;
    }

    public void markFinalizing(Instant at) {
        this.status = RecordingStatus.FINALIZING;
        this.lastActivityAt = at;
    }

    public void markChunkPersisted(long sequence, Instant at) {
        this.lastPersistedSequence = sequence;
        this.lastActivityAt = at;
    }

    public void startUpload(String objectKey, String uploadId) {
        this.objectKey = objectKey;
        this.uploadId = uploadId;
    }

    public int takeNextPartNumber() {
        return nextPartNumber++;
    }

    public void markStored(Instant at) {
        this.status = RecordingStatus.STORED;
        this.completedAt = at;
        this.lastActivityAt = at;
        this.uploadId = null;
    }

    public void markAborted(Instant at) {
        this.status = RecordingStatus.ABORTED;
        this.completedAt = at;
        this.lastActivityAt = at;
        this.uploadId = null;
    }

    public boolean isAcceptingChunks() {
        return status == RecordingStatus.OPEN;
    }

    public String getRecordingId() { return recordingId; }
    public String getAgency() { return agency; }
    public String getIncidentId() { return incidentId; }
    public String getCallId() { return callId; }
    public String getContentType() { return contentType; }
    public RecordingStatus getStatus() { return status; }
    public long getLastIngestedSequence() { return lastIngestedSequence; }
    public long getLastPersistedSequence() { return lastPersistedSequence; }
    public long getBytesIngested() { return bytesIngested; }
    public String getObjectKey() { return objectKey; }
    public String getUploadId() { return uploadId; }
    public int getNextPartNumber() { return nextPartNumber; }
    public Instant getStartedAt() { return startedAt; }
    public Instant getLastActivityAt() { return lastActivityAt; }
    public Instant getCompletedAt() { return completedAt; }
}
