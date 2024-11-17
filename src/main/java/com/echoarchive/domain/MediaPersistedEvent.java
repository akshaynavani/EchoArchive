package com.echoarchive.domain;

import java.time.Instant;

/**
 * Published to {@code media.persisted} once an object is durably stored, so downstream systems
 * (CAD integrations, evidence management) can react without polling.
 */
public record MediaPersistedEvent(
        String mediaId,
        String agency,
        String incidentId,
        MediaKind kind,
        String bucket,
        String objectKey,
        long sizeBytes,
        String sha256,
        Instant persistedAt) {
}
