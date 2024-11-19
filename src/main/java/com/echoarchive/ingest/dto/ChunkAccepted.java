package com.echoarchive.ingest.dto;

/**
 * @param duplicate true when this sequence had already been accepted (safe client retry)
 */
public record ChunkAccepted(String recordingId, long sequence, boolean duplicate, long nextSequence) {
}
