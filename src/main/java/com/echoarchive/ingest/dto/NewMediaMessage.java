package com.echoarchive.ingest.dto;

import com.echoarchive.domain.MediaKind;
import com.echoarchive.domain.Priority;

import java.time.Instant;

/**
 * Metadata accompanying an uploaded multimedia message (bound from multipart form fields).
 */
public record NewMediaMessage(
        String messageId,
        String agency,
        String incidentId,
        String senderId,
        MediaKind kind,
        Priority priority,
        Instant capturedAt,
        String sha256) {
}
