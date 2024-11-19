package com.echoarchive.ingest;

import com.echoarchive.api.BadRequestException;
import com.echoarchive.api.ConflictException;
import com.echoarchive.api.IngestUnavailableException;
import com.echoarchive.api.NotFoundException;
import com.echoarchive.api.PayloadTooLargeException;
import com.echoarchive.config.EchoArchiveProperties;
import com.echoarchive.domain.MediaKind;
import com.echoarchive.domain.MediaMessageEvent;
import com.echoarchive.domain.Priority;
import com.echoarchive.domain.RecordingChunkEvent;
import com.echoarchive.ingest.dto.ChunkAccepted;
import com.echoarchive.ingest.dto.CompleteRecordingRequest;
import com.echoarchive.ingest.dto.MessageAccepted;
import com.echoarchive.ingest.dto.NewMediaMessage;
import com.echoarchive.ingest.dto.RecordingCompletion;
import com.echoarchive.ingest.dto.RecordingStarted;
import com.echoarchive.ingest.dto.StartRecordingRequest;
import com.echoarchive.metadata.RecordingSession;
import com.echoarchive.metadata.RecordingSessionRepository;
import com.echoarchive.metadata.RecordingStatus;
import com.echoarchive.util.Checksums;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Clock;
import java.time.Instant;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Accepts media from the field and hands it to Kafka. A request is only acknowledged once the
 * broker has confirmed the write (acks=all), so a 2xx response means the media is durable.
 */
@Service
public class IngestService {

    private static final Logger log = LoggerFactory.getLogger(IngestService.class);
    private static final long SEND_TIMEOUT_SECONDS = 15;

    private final KafkaTemplate<String, Object> kafka;
    private final RecordingSessionRepository sessions;
    private final EchoArchiveProperties props;
    private final Clock clock;

    public IngestService(KafkaTemplate<String, Object> kafka, RecordingSessionRepository sessions,
                         EchoArchiveProperties props, Clock clock) {
        this.kafka = kafka;
        this.sessions = sessions;
        this.props = props;
        this.clock = clock;
    }

    // ---------------------------------------------------------------- multimedia messages

    public MessageAccepted ingestMessage(NewMediaMessage meta, String contentType, String fileName,
                                         byte[] payload) {
        String agency = Identifiers.require("agency", meta.agency());
        String incidentId = Identifiers.require("incidentId", meta.incidentId());
        String senderId = Identifiers.require("senderId", meta.senderId());
        String messageId = meta.messageId() == null || meta.messageId().isBlank()
                ? UUID.randomUUID().toString()
                : Identifiers.require("messageId", meta.messageId());
        MediaKind kind = Objects.requireNonNullElse(meta.kind(), MediaKind.IMAGE);
        if (kind == MediaKind.CALL_RECORDING) {
            throw new BadRequestException("Call recordings must use the /recordings endpoints");
        }
        if (payload.length == 0) {
            throw new BadRequestException("Empty media payload");
        }
        if (payload.length > props.ingest().maxMessageBytes()) {
            throw new PayloadTooLargeException("Message exceeds " + props.ingest().maxMessageBytes()
                    + " bytes; send large media as a chunked recording");
        }
        String sha256 = Checksums.sha256Hex(payload);
        if (!Checksums.matchesIfPresent(meta.sha256(), sha256)) {
            throw new BadRequestException("sha256 mismatch: payload corrupted in transit");
        }

        Instant now = clock.instant();
        MediaMessageEvent event = new MediaMessageEvent(
                messageId, agency, incidentId, senderId, kind,
                Objects.requireNonNullElse(meta.priority(), Priority.ROUTINE),
                contentType == null ? "application/octet-stream" : contentType,
                fileName, payload.length, sha256,
                Objects.requireNonNullElse(meta.capturedAt(), now), now, payload);

        send(props.topics().mediaMessages(), incidentId, event);
        log.info("Accepted message {} for incident {} ({} bytes, {})",
                messageId, incidentId, payload.length, event.priority());
        return new MessageAccepted(messageId, incidentId, sha256, payload.length, now);
    }

    // ---------------------------------------------------------------- call recordings

    @Transactional
    public RecordingStarted startRecording(StartRecordingRequest req) {
        String recordingId = req.recordingId() == null || req.recordingId().isBlank()
                ? UUID.randomUUID().toString()
                : req.recordingId();

        return sessions.findById(recordingId)
                .map(existing -> {
                    if (!existing.getIncidentId().equals(req.incidentId())
                            || !existing.getCallId().equals(req.callId())) {
                        throw new ConflictException("Recording " + recordingId
                                + " already exists for a different call");
                    }
                    return new RecordingStarted(recordingId, existing.getStatus(),
                            existing.getLastIngestedSequence() + 1);
                })
                .orElseGet(() -> {
                    RecordingSession s = new RecordingSession(recordingId, req.agency(),
                            req.incidentId(), req.callId(), req.contentType(), clock.instant());
                    sessions.save(s);
                    log.info("Started recording {} for call {} / incident {}",
                            recordingId, req.callId(), req.incidentId());
                    return new RecordingStarted(recordingId, s.getStatus(), 0);
                });
    }

    /**
     * Appends one chunk. Chunks must arrive in order; re-sending the last accepted sequence is a
     * no-op so recorders can safely retry after a timeout.
     */
    @Transactional
    public ChunkAccepted appendChunk(String recordingId, long sequence, byte[] payload,
                                     String declaredSha256) {
        RecordingSession s = load(recordingId);
        long expected = s.getLastIngestedSequence() + 1;

        if (sequence < expected) {
            return new ChunkAccepted(recordingId, sequence, true, expected);
        }
        if (!s.isAcceptingChunks()) {
            throw new ConflictException("Recording " + recordingId + " is " + s.getStatus());
        }
        if (sequence != expected) {
            throw new ConflictException("Out-of-order chunk: expected sequence " + expected
                    + " but got " + sequence);
        }
        if (payload.length == 0) {
            throw new BadRequestException("Empty chunk");
        }
        if (payload.length > props.ingest().maxChunkBytes()) {
            throw new PayloadTooLargeException("Chunk exceeds " + props.ingest().maxChunkBytes() + " bytes");
        }
        String sha256 = Checksums.sha256Hex(payload);
        if (!Checksums.matchesIfPresent(declaredSha256, sha256)) {
            throw new BadRequestException("sha256 mismatch for chunk " + sequence);
        }

        Instant now = clock.instant();
        send(props.topics().recordingChunks(), recordingId, new RecordingChunkEvent(
                recordingId, s.getAgency(), s.getIncidentId(), s.getCallId(), s.getContentType(),
                sequence, false, sha256, now, payload));

        // If this commit fails after the send, the client retries the same sequence and the
        // consumer drops the duplicate by sequence number.
        s.markChunkIngested(sequence, payload.length, now);
        return new ChunkAccepted(recordingId, sequence, false, sequence + 1);
    }

    @Transactional
    public RecordingCompletion completeRecording(String recordingId, CompleteRecordingRequest req) {
        RecordingSession s = load(recordingId);
        long chunks = s.getLastIngestedSequence() + 1;

        if (s.getStatus() == RecordingStatus.FINALIZING || s.getStatus() == RecordingStatus.STORED) {
            return new RecordingCompletion(recordingId, s.getStatus(), chunks, s.getBytesIngested());
        }
        if (!s.isAcceptingChunks()) {
            throw new ConflictException("Recording " + recordingId + " is " + s.getStatus());
        }
        if (chunks == 0) {
            throw new ConflictException("Recording " + recordingId + " has no audio");
        }
        if (req != null && req.expectedChunks() != null && req.expectedChunks() != chunks) {
            throw new ConflictException("Expected " + req.expectedChunks() + " chunks but received "
                    + chunks + "; resend the missing chunks before completing");
        }

        Instant now = clock.instant();
        send(props.topics().recordingChunks(), recordingId, new RecordingChunkEvent(
                recordingId, s.getAgency(), s.getIncidentId(), s.getCallId(), s.getContentType(),
                chunks, true, null, now, new byte[0]));
        s.markFinalizing(now);
        log.info("Recording {} finalizing after {} chunks ({} bytes)",
                recordingId, chunks, s.getBytesIngested());
        return new RecordingCompletion(recordingId, s.getStatus(), chunks, s.getBytesIngested());
    }

    private RecordingSession load(String recordingId) {
        return sessions.findById(recordingId)
                .orElseThrow(() -> new NotFoundException("Unknown recording " + recordingId));
    }

    private void send(String topic, String key, Object event) {
        try {
            kafka.send(topic, key, event).get(SEND_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IngestUnavailableException("Interrupted while publishing to " + topic, e);
        } catch (ExecutionException | TimeoutException e) {
            throw new IngestUnavailableException("Could not publish to " + topic, e);
        }
    }
}
