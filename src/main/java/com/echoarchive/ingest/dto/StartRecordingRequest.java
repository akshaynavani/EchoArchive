package com.echoarchive.ingest.dto;

import com.echoarchive.ingest.Identifiers;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Pattern;

/**
 * @param recordingId optional client-generated ID; makes the start call idempotent on retry
 */
public record StartRecordingRequest(
        @Pattern(regexp = Identifiers.REGEX) String recordingId,
        @NotBlank @Pattern(regexp = Identifiers.REGEX) String agency,
        @NotBlank @Pattern(regexp = Identifiers.REGEX) String incidentId,
        @NotBlank @Pattern(regexp = Identifiers.REGEX) String callId,
        @NotBlank String contentType) {
}
