package com.echoarchive.support;

import com.echoarchive.config.EchoArchiveProperties;

import java.time.Duration;

public final class TestProperties {

    public static final long PART_BYTES = 5L * 1024 * 1024;

    private TestProperties() {
    }

    public static EchoArchiveProperties defaults() {
        return new EchoArchiveProperties(
                new EchoArchiveProperties.Topics("media.messages", "recording.chunks", "media.persisted", 3, (short) 1),
                new EchoArchiveProperties.Storage("test-bucket", "us-east-1", "http://localhost:9000", true,
                        null, null, false),
                new EchoArchiveProperties.Ingest(1024, 512, PART_BYTES),
                new EchoArchiveProperties.Retrieval(Duration.ofMinutes(15), Duration.ofHours(2)));
    }
}
