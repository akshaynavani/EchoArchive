package com.echoarchive.ingest.dto;

import java.time.Instant;

public record MessageAccepted(
        String messageId,
        String incidentId,
        String sha256,
        long sizeBytes,
        Instant receivedAt) {
}
