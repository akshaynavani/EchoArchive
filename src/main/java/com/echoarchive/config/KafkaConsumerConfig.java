package com.echoarchive.config;

import com.echoarchive.consume.NonRetryableMediaException;
import com.echoarchive.domain.MediaMessageEvent;
import com.echoarchive.domain.RecordingChunkEvent;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.ssl.SslBundles;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.util.backoff.ExponentialBackOff;

import java.util.Map;

/**
 * Two listener factories (one per event type) sharing one error-handling policy:
 * exponential backoff for transient failures (S3/DB hiccups), then dead-letter to {@code <topic>.DLT}.
 * Poison pills (undeserializable or corrupt records) skip retries.
 */
@Configuration
public class KafkaConsumerConfig {

    private static final Logger log = LoggerFactory.getLogger(KafkaConsumerConfig.class);

    private final KafkaProperties kafkaProperties;
    private final SslBundles sslBundles;
    private final EchoArchiveProperties props;

    public KafkaConsumerConfig(KafkaProperties kafkaProperties, SslBundles sslBundles,
                               EchoArchiveProperties props) {
        this.kafkaProperties = kafkaProperties;
        this.sslBundles = sslBundles;
        this.props = props;
    }

    @Bean
    public DefaultErrorHandler mediaErrorHandler(KafkaTemplate<String, Object> kafkaTemplate) {
        var recoverer = new DeadLetterPublishingRecoverer(kafkaTemplate, (record, ex) -> {
            log.error("Dead-lettering {}-{}@{} key={}: {}", record.topic(), record.partition(),
                    record.offset(), record.key(), ex.getMessage());
            return new TopicPartition(record.topic() + ".DLT", record.partition());
        });

        var backOff = new ExponentialBackOff(1_000L, 2.0);
        backOff.setMaxInterval(30_000L);
        backOff.setMaxElapsedTime(5 * 60_000L);

        var handler = new DefaultErrorHandler(recoverer, backOff);
        handler.addNotRetryableExceptions(NonRetryableMediaException.class);
        return handler;
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, MediaMessageEvent> mediaMessageListenerFactory(
            DefaultErrorHandler mediaErrorHandler) {
        return factory(MediaMessageEvent.class, mediaErrorHandler);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, RecordingChunkEvent> recordingChunkListenerFactory(
            DefaultErrorHandler mediaErrorHandler) {
        return factory(RecordingChunkEvent.class, mediaErrorHandler);
    }

    private <T> ConcurrentKafkaListenerContainerFactory<String, T> factory(
            Class<T> type, DefaultErrorHandler errorHandler) {
        var f = new ConcurrentKafkaListenerContainerFactory<String, T>();
        f.setConsumerFactory(consumerFactory(type));
        f.setCommonErrorHandler(errorHandler);
        f.setConcurrency(Math.min(props.topics().partitions(), 3));
        f.getContainerProperties().setAckMode(
                org.springframework.kafka.listener.ContainerProperties.AckMode.RECORD);
        return f;
    }

    private <T> ConsumerFactory<String, T> consumerFactory(Class<T> type) {
        Map<String, Object> config = kafkaProperties.buildConsumerProperties(sslBundles);
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);

        var json = new JsonDeserializer<>(type, false);
        json.addTrustedPackages("com.echoarchive.domain");
        return new DefaultKafkaConsumerFactory<>(config, new StringDeserializer(),
                new ErrorHandlingDeserializer<>(json));
    }
}
