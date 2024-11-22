package com.echoarchive.consume;

import com.echoarchive.domain.MediaMessageEvent;
import com.echoarchive.metadata.MediaRecord;
import com.echoarchive.metadata.MediaRecordRepository;
import com.echoarchive.storage.ObjectKeyBuilder;
import com.echoarchive.storage.ObjectStore;
import com.echoarchive.util.Checksums;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.Clock;
import java.util.Map;

/**
 * Persists complete multimedia messages to object storage and indexes them.
 * Idempotent: a redelivered message whose ID is already indexed is skipped. If the process dies
 * between the S3 put and the DB commit, the retry overwrites the same key with the same bytes.
 */
@Component
public class MediaMessageConsumer {

    private static final Logger log = LoggerFactory.getLogger(MediaMessageConsumer.class);

    private final ObjectStore store;
    private final ObjectKeyBuilder keys;
    private final MediaRecordRepository records;
    private final PersistedEventPublisher publisher;
    private final Clock clock;

    public MediaMessageConsumer(ObjectStore store, ObjectKeyBuilder keys, MediaRecordRepository records,
                                PersistedEventPublisher publisher, Clock clock) {
        this.store = store;
        this.keys = keys;
        this.records = records;
        this.publisher = publisher;
        this.clock = clock;
    }

    @KafkaListener(
            id = "media-message-persister",
            topics = "${echoarchive.topics.media-messages}",
            containerFactory = "mediaMessageListenerFactory")
    @Transactional
    public void onMessage(MediaMessageEvent event) {
        if (records.existsById(event.messageId())) {
            log.debug("Message {} already persisted; skipping redelivery", event.messageId());
            return;
        }
        byte[] payload = event.payload();
        if (payload == null || !Checksums.sha256Hex(payload).equals(event.sha256())) {
            throw new NonRetryableMediaException("Checksum mismatch for message " + event.messageId());
        }

        String key = keys.build(event.agency(), event.incidentId(), event.kind(), event.capturedAt(),
                event.messageId(), event.contentType(), event.fileName());
        store.put(key, event.contentType(), payload, Map.of(
                "sha256", event.sha256(),
                "agency", event.agency(),
                "incident-id", event.incidentId(),
                "sender-id", event.senderId(),
                "priority", event.priority().name()));

        MediaRecord record = MediaRecord.builder()
                .mediaId(event.messageId())
                .agency(event.agency())
                .incidentId(event.incidentId())
                .senderId(event.senderId())
                .kind(event.kind())
                .priority(event.priority())
                .contentType(event.contentType())
                .fileName(event.fileName())
                .sizeBytes(event.sizeBytes())
                .sha256(event.sha256())
                .bucket(store.bucket())
                .objectKey(key)
                .capturedAt(event.capturedAt())
                .receivedAt(event.receivedAt())
                .persistedAt(clock.instant())
                .build();
        records.save(record);
        publisher.publish(record);

        log.info("Persisted message {} -> s3://{}/{}", event.messageId(), store.bucket(), key);
    }
}
