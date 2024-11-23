package com.echoarchive.retrieval.dto;

import com.echoarchive.metadata.RecordingSession;
import com.echoarchive.metadata.RecordingStatus;

import java.time.Instant;

public record RecordingStatusView(
        String recordingId,
        String incidentId,
        String callId,
        RecordingStatus status,
        long chunksIngested,
        long chunksPersisted,
        long bytesIngested,
        Instant startedAt,
        Instant lastActivityAt,
        Instant completedAt) {

    public static RecordingStatusView of(RecordingSession s) {
        return new RecordingStatusView(s.getRecordingId(), s.getIncidentId(), s.getCallId(),
                s.getStatus(), s.getLastIngestedSequence() + 1,
                Math.min(s.getLastPersistedSequence() + 1, s.getLastIngestedSequence() + 1),
                s.getBytesIngested(), s.getStartedAt(), s.getLastActivityAt(), s.getCompletedAt());
    }
}
