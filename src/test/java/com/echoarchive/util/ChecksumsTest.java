package com.echoarchive.util;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;

class ChecksumsTest {

    @Test
    void computesKnownSha256() {
        assertThat(Checksums.sha256Hex("abc".getBytes(StandardCharsets.UTF_8)))
                .isEqualTo("ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad");
    }

    @Test
    void declaredChecksumIsOptionalAndCaseInsensitive() {
        assertThat(Checksums.matchesIfPresent(null, "aa")).isTrue();
        assertThat(Checksums.matchesIfPresent(" ", "aa")).isTrue();
        assertThat(Checksums.matchesIfPresent("AA", "aa")).isTrue();
        assertThat(Checksums.matchesIfPresent("bb", "aa")).isFalse();
    }
}
