package com.echoarchive.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.TopicConfig;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin;

/**
 * Declares the topics EchoArchive owns. Spring's {@link org.springframework.kafka.core.KafkaAdmin}
 * creates them on startup if they don't exist (broker auto-create is expected to be off).
 */
@Configuration
public class KafkaTopicConfig {

    /** Leaves room for base64 expansion of a 5 MiB payload plus metadata. */
    private static final String MAX_MESSAGE_BYTES = String.valueOf(8 * 1024 * 1024);

    private final EchoArchiveProperties.Topics topics;

    public KafkaTopicConfig(EchoArchiveProperties properties) {
        this.topics = properties.topics();
    }

    @Bean
    public KafkaAdmin.NewTopics echoArchiveTopics() {
        return new KafkaAdmin.NewTopics(
                mediaTopic(topics.mediaMessages()),
                mediaTopic(topics.recordingChunks()),
                eventTopic(topics.mediaPersisted()),
                mediaTopic(topics.mediaMessages() + ".DLT"),
                mediaTopic(topics.recordingChunks() + ".DLT"));
    }

    private NewTopic mediaTopic(String name) {
        return TopicBuilder.name(name)
                .partitions(topics.partitions())
                .replicas(topics.replicationFactor())
                .config(TopicConfig.MAX_MESSAGE_BYTES_CONFIG, MAX_MESSAGE_BYTES)
                // Media is only buffered here until persisted; S3 is the system of record.
                .config(TopicConfig.RETENTION_MS_CONFIG, String.valueOf(7L * 24 * 3600 * 1000))
                .config(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG,
                        String.valueOf(Math.min(2, topics.replicationFactor())))
                .build();
    }

    private NewTopic eventTopic(String name) {
        return TopicBuilder.name(name)
                .partitions(topics.partitions())
                .replicas(topics.replicationFactor())
                .config(TopicConfig.RETENTION_MS_CONFIG, String.valueOf(30L * 24 * 3600 * 1000))
                .build();
    }
}
