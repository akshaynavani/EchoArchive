package com.echoarchive.metadata;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.IdClass;
import jakarta.persistence.Table;

import java.io.Serializable;

/**
 * An uploaded S3 multipart part; needed to complete the upload after a consumer restart.
 */
@Entity
@IdClass(RecordingPart.Key.class)
@Table(name = "recording_part")
public class RecordingPart {

    @Id
    @Column(name = "recording_id", length = 64)
    private String recordingId;

    @Id
    @Column(name = "part_number")
    private int partNumber;

    @Column(name = "etag", nullable = false, length = 128)
    private String eTag;

    @Column(name = "size_bytes", nullable = false)
    private long sizeBytes;

    protected RecordingPart() {
    }

    public RecordingPart(String recordingId, int partNumber, String eTag, long sizeBytes) {
        this.recordingId = recordingId;
        this.partNumber = partNumber;
        this.eTag = eTag;
        this.sizeBytes = sizeBytes;
    }

    public String getRecordingId() { return recordingId; }
    public int getPartNumber() { return partNumber; }
    public String getETag() { return eTag; }
    public long getSizeBytes() { return sizeBytes; }

    public record Key(String recordingId, int partNumber) implements Serializable {
    }
}
