package com.echoarchive.retrieval.dto;

import com.echoarchive.domain.MediaKind;
import com.echoarchive.domain.Priority;

import java.time.Instant;

public record MediaQuery(
        String agency,
        String incidentId,
        String senderId,
        MediaKind kind,
        Priority minPriority,
        Instant from,
        Instant to) {

    public String describe() {
        return "agency=" + agency + ",incident=" + incidentId + ",sender=" + senderId + ",kind=" + kind
                + ",minPriority=" + minPriority + ",from=" + from + ",to=" + to;
    }
}
