package com.echoarchive.config;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.ssl.SslBundles;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.util.Map;

/**
 * Producer setup: String keys, JSON values. Reliability settings (acks=all, idempotence) come from
 * application.yml; here we only pin serializers and the max request size.
 */
@Configuration
public class KafkaProducerConfig {

    @Bean
    public ProducerFactory<String, Object> producerFactory(KafkaProperties kafkaProperties,
                                                           SslBundles sslBundles) {
        Map<String, Object> props = kafkaProperties.buildProducerProperties(sslBundles);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        // Consumers deserialize into a known type per topic; don't leak producer class names.
        props.put(JsonSerializer.ADD_TYPE_INFO_HEADERS, false);
        props.putIfAbsent(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, 8 * 1024 * 1024);
        return new DefaultKafkaProducerFactory<>(props);
    }

    @Bean
    public KafkaTemplate<String, Object> kafkaTemplate(ProducerFactory<String, Object> producerFactory) {
        return new KafkaTemplate<>(producerFactory);
    }
}
