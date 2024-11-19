package com.echoarchive.ingest;

import com.echoarchive.ingest.dto.ChunkAccepted;
import com.echoarchive.ingest.dto.CompleteRecordingRequest;
import com.echoarchive.ingest.dto.RecordingCompletion;
import com.echoarchive.ingest.dto.RecordingStarted;
import com.echoarchive.ingest.dto.StartRecordingRequest;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Min;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

/**
 * Streaming call-recording ingest: start a session, PUT ordered audio chunks, then complete.
 */
@Validated
@RestController
@RequestMapping("/api/v1/recordings")
public class RecordingIngestController {

    private final IngestService ingest;

    public RecordingIngestController(IngestService ingest) {
        this.ingest = ingest;
    }

    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    public RecordingStarted start(@Valid @RequestBody StartRecordingRequest request) {
        return ingest.startRecording(request);
    }

    @PutMapping(value = "/{recordingId}/chunks/{sequence}",
            consumes = MediaType.APPLICATION_OCTET_STREAM_VALUE)
    @ResponseStatus(HttpStatus.ACCEPTED)
    public ChunkAccepted appendChunk(
            @PathVariable String recordingId,
            @PathVariable @Min(0) long sequence,
            @RequestHeader(value = "X-Content-SHA256", required = false) String sha256,
            @RequestBody byte[] body) {
        return ingest.appendChunk(Identifiers.require("recordingId", recordingId), sequence, body, sha256);
    }

    @PostMapping("/{recordingId}/complete")
    @ResponseStatus(HttpStatus.ACCEPTED)
    public RecordingCompletion complete(
            @PathVariable String recordingId,
            @Valid @RequestBody(required = false) CompleteRecordingRequest request) {
        return ingest.completeRecording(Identifiers.require("recordingId", recordingId), request);
    }
}
