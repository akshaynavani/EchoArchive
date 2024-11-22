package com.echoarchive.consume;

import com.echoarchive.config.EchoArchiveProperties;
import com.echoarchive.domain.MediaPersistedEvent;
import com.echoarchive.metadata.MediaRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

/**
 * Announces persisted media on {@code media.persisted}. When called inside a DB transaction the
 * event is sent only after commit so subscribers never see media that isn't indexed yet.
 */
@Component
public class PersistedEventPublisher {

    private static final Logger log = LoggerFactory.getLogger(PersistedEventPublisher.class);

    private final KafkaTemplate<String, Object> kafka;
    private final String topic;

    public PersistedEventPublisher(KafkaTemplate<String, Object> kafka, EchoArchiveProperties props) {
        this.kafka = kafka;
        this.topic = props.topics().mediaPersisted();
    }

    public void publish(MediaRecord record) {
        MediaPersistedEvent event = new MediaPersistedEvent(record.getMediaId(), record.getAgency(),
                record.getIncidentId(), record.getKind(), record.getBucket(), record.getObjectKey(),
                record.getSizeBytes(), record.getSha256(), record.getPersistedAt());

        Runnable send = () -> kafka.send(topic, record.getIncidentId(), event)
                .whenComplete((r, ex) -> {
                    if (ex != null) {
                        log.warn("Failed to publish persisted event for {}", record.getMediaId(), ex);
                    }
                });

        if (TransactionSynchronizationManager.isSynchronizationActive()) {
            TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
                @Override
                public void afterCommit() {
                    send.run();
                }
            });
        } else {
            send.run();
        }
    }
}
