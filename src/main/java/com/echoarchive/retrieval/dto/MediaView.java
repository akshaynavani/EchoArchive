package com.echoarchive.retrieval.dto;

import com.echoarchive.domain.MediaKind;
import com.echoarchive.domain.Priority;
import com.echoarchive.metadata.MediaRecord;

import java.time.Instant;

public record MediaView(
        String mediaId,
        String agency,
        String incidentId,
        String senderId,
        String callId,
        MediaKind kind,
        Priority priority,
        String contentType,
        String fileName,
        long sizeBytes,
        String sha256,
        String objectKey,
        Instant capturedAt,
        Instant receivedAt,
        Instant persistedAt) {

    public static MediaView of(MediaRecord r) {
        return new MediaView(r.getMediaId(), r.getAgency(), r.getIncidentId(), r.getSenderId(),
                r.getCallId(), r.getKind(), r.getPriority(), r.getContentType(), r.getFileName(),
                r.getSizeBytes(), r.getSha256(), r.getObjectKey(), r.getCapturedAt(),
                r.getReceivedAt(), r.getPersistedAt());
    }
}
