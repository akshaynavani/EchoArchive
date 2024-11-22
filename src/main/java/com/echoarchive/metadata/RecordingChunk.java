package com.echoarchive.metadata;

import jakarta.persistence.Basic;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.FetchType;
import jakarta.persistence.Id;
import jakarta.persistence.IdClass;
import jakarta.persistence.Table;

import java.time.Instant;

/**
 * Per-chunk ledger for a recording. While a chunk is waiting to be folded into an S3 part its
 * bytes are staged in {@code payload}; once uploaded the payload is cleared and {@code partNumber}
 * set. The ledger (sequence, size, sha256) is kept permanently as chain-of-custody evidence.
 */
@Entity
@IdClass(RecordingChunkId.class)
@Table(name = "recording_chunk")
public class RecordingChunk {

    @Id
    @Column(name = "recording_id", length = 64)
    private String recordingId;

    @Id
    @Column(name = "sequence")
    private long sequence;

    @Column(name = "size_bytes", nullable = false)
    private long sizeBytes;

    @Column(nullable = false, length = 64)
    private String sha256;

    @Basic(fetch = FetchType.LAZY)
    @Column(name = "payload")
    private byte[] payload;

    @Column(name = "part_number")
    private Integer partNumber;

    @Column(name = "received_at", nullable = false)
    private Instant receivedAt;

    protected RecordingChunk() {
    }

    public RecordingChunk(String recordingId, long sequence, String sha256, byte[] payload,
                          Instant receivedAt) {
        this.recordingId = recordingId;
        this.sequence = sequence;
        this.sha256 = sha256;
        this.payload = payload;
        this.sizeBytes = payload.length;
        this.receivedAt = receivedAt;
    }

    public void markUploaded(int partNumber) {
        this.partNumber = partNumber;
        this.payload = null;
    }

    public String getRecordingId() { return recordingId; }
    public long getSequence() { return sequence; }
    public long getSizeBytes() { return sizeBytes; }
    public String getSha256() { return sha256; }
    public byte[] getPayload() { return payload; }
    public Integer getPartNumber() { return partNumber; }
    public Instant getReceivedAt() { return receivedAt; }
}
