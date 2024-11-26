package com.echoarchive.storage;

import com.echoarchive.domain.MediaKind;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

class ObjectKeyBuilderTest {

    private final ObjectKeyBuilder keys = new ObjectKeyBuilder();
    private final Instant ts = Instant.parse("2026-03-04T23:59:59Z");

    @Test
    void buildsIncidentScopedDatePartitionedKey() {
        String key = keys.build("metro-fd", "INC-1001", MediaKind.IMAGE, ts, "msg-1", "image/jpeg", null);
        assertThat(key).isEqualTo("metro-fd/INC-1001/2026/03/04/image/msg-1.jpg");
    }

    @Test
    void prefersFileNameExtension() {
        assertThat(keys.extension(MediaKind.VIDEO, "video/mp4", "clip.MOV")).isEqualTo("mov");
    }

    @Test
    void ignoresUnsafeFileNameExtension() {
        assertThat(keys.extension(MediaKind.VIDEO, "video/mp4", "clip.m/../x")).isEqualTo("mp4");
    }

    @Test
    void stripsContentTypeParameters() {
        assertThat(keys.extension(MediaKind.VOICE_NOTE, "audio/ogg; codecs=opus", null)).isEqualTo("ogg");
    }

    @Test
    void fallsBackToKindDefault() {
        assertThat(keys.extension(MediaKind.CALL_RECORDING, "application/x-unknown", null)).isEqualTo("bin");
    }
}
