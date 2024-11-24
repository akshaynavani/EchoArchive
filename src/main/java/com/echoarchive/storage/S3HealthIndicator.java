package com.echoarchive.storage;

import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.stereotype.Component;

/**
 * Reports DOWN when the media bucket can't be reached. Ingest keeps working (Kafka buffers), but
 * operators need to know persistence is stalled.
 */
@Component("objectStore")
public class S3HealthIndicator implements HealthIndicator {

    private final ObjectStore store;

    public S3HealthIndicator(ObjectStore store) {
        this.store = store;
    }

    @Override
    public Health health() {
        Health.Builder builder = store.isReachable() ? Health.up() : Health.down();
        return builder.withDetail("bucket", store.bucket()).build();
    }
}
