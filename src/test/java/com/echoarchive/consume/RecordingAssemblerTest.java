package com.echoarchive.consume;

import com.echoarchive.config.ClockConfig;
import com.echoarchive.config.EchoArchiveProperties;
import com.echoarchive.domain.MediaKind;
import com.echoarchive.domain.RecordingChunkEvent;
import com.echoarchive.metadata.MediaRecord;
import com.echoarchive.metadata.MediaRecordRepository;
import com.echoarchive.metadata.RecordingPartRepository;
import com.echoarchive.metadata.RecordingSession;
import com.echoarchive.metadata.RecordingSessionRepository;
import com.echoarchive.metadata.RecordingStatus;
import com.echoarchive.storage.ObjectKeyBuilder;
import com.echoarchive.support.InMemoryObjectStore;
import com.echoarchive.util.Checksums;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.ActiveProfiles;

import java.io.ByteArrayOutputStream;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;

@DataJpaTest
@ActiveProfiles("test")
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
@Import({RecordingAssembler.class, ObjectKeyBuilder.class, ClockConfig.class,
        RecordingAssemblerTest.StoreConfig.class})
@EnableConfigurationProperties(EchoArchiveProperties.class)
class RecordingAssemblerTest {

    private static final int MIB = 1024 * 1024;

    @TestConfiguration
    static class StoreConfig {
        @Bean
        InMemoryObjectStore objectStore() {
            return new InMemoryObjectStore();
        }
    }

    @Autowired RecordingAssembler assembler;
    @Autowired RecordingSessionRepository sessions;
    @Autowired RecordingPartRepository parts;
    @Autowired MediaRecordRepository records;
    @Autowired InMemoryObjectStore store;
    @MockBean PersistedEventPublisher publisher;

    @BeforeEach
    void openSession() {
        sessions.save(new RecordingSession("rec-1", "metro-pd", "INC-7", "call-3", "audio/ogg",
                Instant.parse("2026-05-01T10:00:00Z")));
    }

    @Test
    void stitchesChunksIntoMultipartObject() {
        List<byte[]> audio = new ArrayList<>();
        for (int i = 0; i < 7; i++) {
            byte[] chunk = new byte[MIB];
            Arrays.fill(chunk, (byte) i);
            audio.add(chunk);
            assembler.accept(chunk(i, chunk));
        }
        // 5 MiB threshold reached after chunk 4 -> one part uploaded, two chunks still staged
        assertThat(parts.findByRecordingIdOrderByPartNumberAsc("rec-1")).hasSize(1);

        assembler.accept(finalMarker(7));

        RecordingSession s = sessions.findById("rec-1").orElseThrow();
        assertThat(s.getStatus()).isEqualTo(RecordingStatus.STORED);
        assertThat(parts.findByRecordingIdOrderByPartNumberAsc("rec-1")).hasSize(2);

        ByteArrayOutputStream expected = new ByteArrayOutputStream();
        audio.forEach(expected::writeBytes);
        assertThat(store.objects.get(s.getObjectKey())).isEqualTo(expected.toByteArray());
        assertThat(s.getObjectKey()).isEqualTo("metro-pd/INC-7/2026/05/01/recording/rec-1.ogg");

        MediaRecord record = records.findById("rec-1").orElseThrow();
        assertThat(record.getKind()).isEqualTo(MediaKind.CALL_RECORDING);
        assertThat(record.getSizeBytes()).isEqualTo(7L * MIB);
        List<String> hashes = audio.stream().map(Checksums::sha256Hex).toList();
        assertThat(record.getSha256()).isEqualTo(RecordingAssembler.hashList(hashes));
        verify(publisher).publish(any(MediaRecord.class));
    }

    @Test
    void shortRecordingIsSinglePart() {
        assembler.accept(chunk(0, new byte[]{1, 2, 3}));
        assembler.accept(finalMarker(1));

        RecordingSession s = sessions.findById("rec-1").orElseThrow();
        assertThat(s.getStatus()).isEqualTo(RecordingStatus.STORED);
        assertThat(store.objects.get(s.getObjectKey())).containsExactly(1, 2, 3);
    }

    @Test
    void redeliveredChunkIsIgnored() {
        assembler.accept(chunk(0, new byte[]{1}));
        assembler.accept(chunk(0, new byte[]{1}));
        assembler.accept(chunk(1, new byte[]{2}));
        assembler.accept(finalMarker(2));

        RecordingSession s = sessions.findById("rec-1").orElseThrow();
        assertThat(store.objects.get(s.getObjectKey())).containsExactly(1, 2);
    }

    @Test
    void gapIsNotRetryable() {
        assembler.accept(chunk(0, new byte[]{1}));
        assertThatThrownBy(() -> assembler.accept(chunk(2, new byte[]{3})))
                .isInstanceOf(NonRetryableMediaException.class)
                .hasMessageContaining("expected 1");
    }

    @Test
    void corruptChunkIsNotRetryable() {
        RecordingChunkEvent bad = new RecordingChunkEvent("rec-1", "metro-pd", "INC-7", "call-3", "audio/ogg",
                0, false, "0000", Instant.now(), new byte[]{9});
        assertThatThrownBy(() -> assembler.accept(bad)).isInstanceOf(NonRetryableMediaException.class);
    }

    private static RecordingChunkEvent chunk(long seq, byte[] data) {
        return new RecordingChunkEvent("rec-1", "metro-pd", "INC-7", "call-3", "audio/ogg",
                seq, false, Checksums.sha256Hex(data), Instant.now(), data);
    }

    private static RecordingChunkEvent finalMarker(long seq) {
        return new RecordingChunkEvent("rec-1", "metro-pd", "INC-7", "call-3", "audio/ogg",
                seq, true, null, Instant.now(), new byte[0]);
    }
}
