package com.echoarchive.ingest.dto;

import com.echoarchive.metadata.RecordingStatus;

public record RecordingStarted(String recordingId, RecordingStatus status, long nextSequence) {
}
