package com.echoarchive.ingest;

import com.echoarchive.domain.MediaKind;
import com.echoarchive.domain.Priority;
import com.echoarchive.ingest.dto.MessageAccepted;
import com.echoarchive.ingest.dto.NewMediaMessage;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RequestPart;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;
import java.time.Instant;

@RestController
@RequestMapping("/api/v1/messages")
public class MediaIngestController {

    private final IngestService ingest;

    public MediaIngestController(IngestService ingest) {
        this.ingest = ingest;
    }

    /**
     * Upload a multimedia message (photo, video clip, voice note, text attachment).
     * Returns 202: the media is durable in Kafka and will be persisted to object storage
     * asynchronously.
     */
    @PostMapping(consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
    @ResponseStatus(HttpStatus.ACCEPTED)
    public MessageAccepted upload(
            @RequestPart("file") MultipartFile file,
            @RequestParam("agency") String agency,
            @RequestParam("incidentId") String incidentId,
            @RequestParam("senderId") String senderId,
            @RequestParam(value = "messageId", required = false) String messageId,
            @RequestParam(value = "kind", defaultValue = "IMAGE") MediaKind kind,
            @RequestParam(value = "priority", defaultValue = "ROUTINE") Priority priority,
            @RequestParam(value = "capturedAt", required = false)
            @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) Instant capturedAt,
            @RequestParam(value = "sha256", required = false) String sha256) throws IOException {

        NewMediaMessage meta = new NewMediaMessage(messageId, agency, incidentId, senderId, kind,
                priority, capturedAt, sha256);
        return ingest.ingestMessage(meta, file.getContentType(), file.getOriginalFilename(),
                file.getBytes());
    }
}
