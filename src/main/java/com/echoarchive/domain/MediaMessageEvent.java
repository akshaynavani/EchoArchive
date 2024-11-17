package com.echoarchive.domain;

import java.time.Instant;

/**
 * A complete multimedia message published to {@code media.messages}. Keyed by incident ID.
 *
 * @param messageId  client-supplied unique ID (idempotency key)
 * @param sha256     hex checksum computed at ingest for chain-of-custody
 */
public record MediaMessageEvent(
        String messageId,
        String agency,
        String incidentId,
        String senderId,
        MediaKind kind,
        Priority priority,
        String contentType,
        String fileName,
        long sizeBytes,
        String sha256,
        Instant capturedAt,
        Instant receivedAt,
        byte[] payload) {
}
