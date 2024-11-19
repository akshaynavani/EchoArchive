package com.echoarchive.ingest.dto;

import jakarta.validation.constraints.Min;

/**
 * @param expectedChunks optional; if set, completion fails unless exactly this many chunks arrived
 */
public record CompleteRecordingRequest(@Min(1) Long expectedChunks) {
}
