package com.echoarchive.ingest.dto;

import com.echoarchive.metadata.RecordingStatus;

public record RecordingCompletion(String recordingId, RecordingStatus status, long totalChunks,
                                  long bytesIngested) {
}
