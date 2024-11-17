package com.echoarchive.domain;

import java.time.Instant;

/**
 * One chunk of a call recording published to {@code recording.chunks}. Keyed by recording ID so
 * every chunk of a call lands on the same partition, in order.
 *
 * @param sequence zero-based, contiguous chunk number
 * @param last     true for the terminating chunk; payload may be empty
 */
public record RecordingChunkEvent(
        String recordingId,
        String agency,
        String incidentId,
        String callId,
        String contentType,
        long sequence,
        boolean last,
        String sha256,
        Instant receivedAt,
        byte[] payload) {
}
