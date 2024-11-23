package com.echoarchive.consume;

import com.echoarchive.config.EchoArchiveProperties;
import com.echoarchive.domain.MediaKind;
import com.echoarchive.domain.Priority;
import com.echoarchive.domain.RecordingChunkEvent;
import com.echoarchive.metadata.MediaRecord;
import com.echoarchive.metadata.MediaRecordRepository;
import com.echoarchive.metadata.RecordingChunk;
import com.echoarchive.metadata.RecordingChunkRepository;
import com.echoarchive.metadata.RecordingPart;
import com.echoarchive.metadata.RecordingPartRepository;
import com.echoarchive.metadata.RecordingSession;
import com.echoarchive.metadata.RecordingSessionRepository;
import com.echoarchive.metadata.RecordingStatus;
import com.echoarchive.storage.ObjectKeyBuilder;
import com.echoarchive.storage.ObjectStore;
import com.echoarchive.util.Checksums;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Clock;
import java.time.Instant;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;

/**
 * Turns an ordered stream of recording chunks into a single S3 object via multipart upload.
 *
 * <p>Chunks are small (~1 MiB) but S3 parts must be at least 5 MiB, so chunks are staged in the
 * {@code recording_chunk} table until enough bytes have accumulated, then folded into one part.
 * Staging in the DB (rather than memory) means a consumer crash loses nothing: Kafka offsets and
 * staged bytes commit together with the session's progress.</p>
 *
 * <p>The recording's checksum is a SHA-256 over the ordered list of chunk SHA-256s (a hash list),
 * which can be recomputed from the chunk ledger without re-reading the audio.</p>
 */
@Service
public class RecordingAssembler {

    private static final Logger log = LoggerFactory.getLogger(RecordingAssembler.class);

    private final RecordingSessionRepository sessions;
    private final RecordingChunkRepository chunks;
    private final RecordingPartRepository parts;
    private final MediaRecordRepository records;
    private final ObjectStore store;
    private final ObjectKeyBuilder keys;
    private final PersistedEventPublisher publisher;
    private final long partBytes;
    private final Clock clock;

    public RecordingAssembler(RecordingSessionRepository sessions, RecordingChunkRepository chunks,
                              RecordingPartRepository parts, MediaRecordRepository records,
                              ObjectStore store, ObjectKeyBuilder keys, PersistedEventPublisher publisher,
                              EchoArchiveProperties props, Clock clock) {
        this.sessions = sessions;
        this.chunks = chunks;
        this.parts = parts;
        this.records = records;
        this.store = store;
        this.keys = keys;
        this.publisher = publisher;
        this.partBytes = props.ingest().multipartPartBytes();
        this.clock = clock;
    }

    @Transactional
    public void accept(RecordingChunkEvent event) {
        RecordingSession session = sessions.findById(event.recordingId())
                .orElseThrow(() -> new NonRetryableMediaException(
                        "Chunk for unknown recording " + event.recordingId()));

        if (session.getStatus() == RecordingStatus.STORED || session.getStatus() == RecordingStatus.ABORTED) {
            log.debug("Recording {} is {}; ignoring chunk {}", session.getRecordingId(),
                    session.getStatus(), event.sequence());
            return;
        }
        long expected = session.getLastPersistedSequence() + 1;
        if (event.sequence() < expected) {
            log.debug("Duplicate chunk {} for recording {}", event.sequence(), event.recordingId());
            return;
        }
        if (event.sequence() > expected) {
            // Ingest only publishes sequence n+1 after n is committed and all chunks share a
            // partition, so a gap means data loss upstream. Don't silently stitch around it.
            throw new NonRetryableMediaException("Gap in recording " + event.recordingId()
                    + ": expected " + expected + " got " + event.sequence());
        }

        Instant now = clock.instant();
        if (event.last()) {
            finish(session, now);
            return;
        }

        byte[] payload = event.payload();
        if (payload == null || !Checksums.sha256Hex(payload).equals(event.sha256())) {
            throw new NonRetryableMediaException("Checksum mismatch for chunk " + event.sequence()
                    + " of recording " + event.recordingId());
        }
        chunks.save(new RecordingChunk(event.recordingId(), event.sequence(), event.sha256(),
                payload, event.receivedAt()));
        chunks.flush();
        session.markChunkPersisted(event.sequence(), now);

        if (chunks.pendingBytes(event.recordingId()) >= partBytes) {
            flushPart(session);
        }
    }

    private void finish(RecordingSession session, Instant now) {
        if (chunks.pendingBytes(session.getRecordingId()) > 0) {
            flushPart(session); // final part may be < 5 MiB, which S3 allows for the last part
        }
        List<ObjectStore.PartETag> uploaded = parts.findByRecordingIdOrderByPartNumberAsc(session.getRecordingId())
                .stream()
                .map(p -> new ObjectStore.PartETag(p.getPartNumber(), p.getETag()))
                .toList();
        if (uploaded.isEmpty() || session.getUploadId() == null) {
            throw new NonRetryableMediaException("Recording " + session.getRecordingId()
                    + " finished with no uploaded audio");
        }

        store.completeMultipartUpload(session.getObjectKey(), session.getUploadId(), uploaded);

        MediaRecord record = MediaRecord.builder()
                .mediaId(session.getRecordingId())
                .agency(session.getAgency())
                .incidentId(session.getIncidentId())
                .callId(session.getCallId())
                .kind(MediaKind.CALL_RECORDING)
                .priority(Priority.ROUTINE)
                .contentType(session.getContentType())
                .sizeBytes(chunks.totalBytes(session.getRecordingId()))
                .sha256(hashList(chunks.chunkHashesInOrder(session.getRecordingId())))
                .bucket(store.bucket())
                .objectKey(session.getObjectKey())
                .capturedAt(session.getStartedAt())
                .receivedAt(session.getLastActivityAt())
                .persistedAt(now)
                .build();
        records.save(record);
        session.markChunkPersisted(session.getLastPersistedSequence() + 1, now);
        session.markStored(now);
        publisher.publish(record);

        log.info("Recording {} stored: {} parts, {} bytes -> s3://{}/{}", session.getRecordingId(),
                uploaded.size(), record.getSizeBytes(), store.bucket(), session.getObjectKey());
    }

    private void flushPart(RecordingSession session) {
        if (session.getUploadId() == null) {
            String key = keys.build(session.getAgency(), session.getIncidentId(),
                    MediaKind.CALL_RECORDING, session.getStartedAt(), session.getRecordingId(),
                    session.getContentType(), null);
            String uploadId = store.createMultipartUpload(key, session.getContentType(), Map.of(
                    "agency", session.getAgency(),
                    "incident-id", session.getIncidentId(),
                    "call-id", session.getCallId()));
            session.startUpload(key, uploadId);
        }

        List<RecordingChunk> pending =
                chunks.findByRecordingIdAndPartNumberIsNullOrderBySequenceAsc(session.getRecordingId());
        ByteArrayOutputStream buf = new ByteArrayOutputStream((int) Math.min(Integer.MAX_VALUE, partBytes * 2));
        for (RecordingChunk c : pending) {
            buf.writeBytes(c.getPayload());
        }
        byte[] data = buf.toByteArray();

        // Part number is only consumed when this transaction commits; a crash before commit
        // re-uploads the same part number, which S3 treats as an overwrite.
        int partNumber = session.takeNextPartNumber();
        String eTag = store.uploadPart(session.getObjectKey(), session.getUploadId(), partNumber, data);
        parts.save(new RecordingPart(session.getRecordingId(), partNumber, eTag, data.length));
        pending.forEach(c -> c.markUploaded(partNumber));

        log.debug("Uploaded part {} ({} bytes, {} chunks) for recording {}",
                partNumber, data.length, pending.size(), session.getRecordingId());
    }

    static String hashList(List<String> chunkHashes) {
        MessageDigest digest = Checksums.newSha256();
        for (String h : chunkHashes) {
            digest.update(h.getBytes(StandardCharsets.US_ASCII));
        }
        return HexFormat.of().formatHex(digest.digest());
    }
}
