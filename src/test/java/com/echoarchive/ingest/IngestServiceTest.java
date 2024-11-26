package com.echoarchive.ingest;

import com.echoarchive.api.BadRequestException;
import com.echoarchive.api.ConflictException;
import com.echoarchive.api.IngestUnavailableException;
import com.echoarchive.api.PayloadTooLargeException;
import com.echoarchive.domain.MediaKind;
import com.echoarchive.domain.MediaMessageEvent;
import com.echoarchive.domain.Priority;
import com.echoarchive.domain.RecordingChunkEvent;
import com.echoarchive.ingest.dto.ChunkAccepted;
import com.echoarchive.ingest.dto.CompleteRecordingRequest;
import com.echoarchive.ingest.dto.MessageAccepted;
import com.echoarchive.ingest.dto.NewMediaMessage;
import com.echoarchive.ingest.dto.RecordingCompletion;
import com.echoarchive.metadata.RecordingSession;
import com.echoarchive.metadata.RecordingSessionRepository;
import com.echoarchive.metadata.RecordingStatus;
import com.echoarchive.support.TestProperties;
import com.echoarchive.util.Checksums;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class IngestServiceTest {

    private static final Instant NOW = Instant.parse("2026-05-01T12:00:00Z");

    @SuppressWarnings("unchecked")
    private final KafkaTemplate<String, Object> kafka = mock(KafkaTemplate.class);
    private final RecordingSessionRepository sessions = mock(RecordingSessionRepository.class);
    private IngestService service;

    @BeforeEach
    void setUp() {
        service = new IngestService(kafka, sessions, TestProperties.defaults(),
                Clock.fixed(NOW, ZoneOffset.UTC));
        CompletableFuture<SendResult<String, Object>> ok = CompletableFuture.completedFuture(null);
        when(kafka.send(anyString(), anyString(), any())).thenReturn(ok);
    }

    @Test
    void publishesMessageKeyedByIncident() {
        byte[] payload = "photo-bytes".getBytes();
        MessageAccepted accepted = service.ingestMessage(meta(null), "image/jpeg", "a.jpg", payload);

        ArgumentCaptor<Object> event = ArgumentCaptor.forClass(Object.class);
        verify(kafka).send(eq("media.messages"), eq("INC-1"), event.capture());
        MediaMessageEvent e = (MediaMessageEvent) event.getValue();
        assertThat(e.sha256()).isEqualTo(Checksums.sha256Hex(payload));
        assertThat(e.priority()).isEqualTo(Priority.URGENT);
        assertThat(e.capturedAt()).isEqualTo(NOW);
        assertThat(accepted.messageId()).isEqualTo(e.messageId()).isNotBlank();
    }

    @Test
    void rejectsChecksumMismatch() {
        assertThatThrownBy(() -> service.ingestMessage(meta("deadbeef"), "image/jpeg", "a.jpg", new byte[]{1}))
                .isInstanceOf(BadRequestException.class);
        verify(kafka, never()).send(anyString(), anyString(), any());
    }

    @Test
    void rejectsOversizedMessage() {
        assertThatThrownBy(() -> service.ingestMessage(meta(null), "image/jpeg", "a.jpg", new byte[2048]))
                .isInstanceOf(PayloadTooLargeException.class);
    }

    @Test
    void rejectsUnsafeIdentifiers() {
        NewMediaMessage bad = new NewMediaMessage(null, "../etc", "INC-1", "unit-7", MediaKind.IMAGE,
                Priority.ROUTINE, null, null);
        assertThatThrownBy(() -> service.ingestMessage(bad, "image/jpeg", "a.jpg", new byte[]{1}))
                .isInstanceOf(BadRequestException.class);
    }

    @Test
    void surfacesKafkaFailureAsUnavailable() {
        when(kafka.send(anyString(), anyString(), any()))
                .thenReturn(CompletableFuture.failedFuture(new RuntimeException("broker down")));
        assertThatThrownBy(() -> service.ingestMessage(meta(null), "image/jpeg", "a.jpg", new byte[]{1}))
                .isInstanceOf(IngestUnavailableException.class);
    }

    @Test
    void appendsChunksInOrder() {
        RecordingSession s = openSession();
        ChunkAccepted first = service.appendChunk("rec-1", 0, new byte[]{1, 2}, null);
        assertThat(first.duplicate()).isFalse();
        assertThat(first.nextSequence()).isEqualTo(1);
        assertThat(s.getLastIngestedSequence()).isZero();
        assertThat(s.getBytesIngested()).isEqualTo(2);

        ArgumentCaptor<Object> event = ArgumentCaptor.forClass(Object.class);
        verify(kafka).send(eq("recording.chunks"), eq("rec-1"), event.capture());
        assertThat(((RecordingChunkEvent) event.getValue()).incidentId()).isEqualTo("INC-1");
    }

    @Test
    void retriedChunkIsAcknowledgedWithoutRepublishing() {
        RecordingSession s = openSession();
        s.markChunkIngested(0, 2, NOW);
        ChunkAccepted again = service.appendChunk("rec-1", 0, new byte[]{1, 2}, null);
        assertThat(again.duplicate()).isTrue();
        verify(kafka, never()).send(anyString(), anyString(), any());
    }

    @Test
    void outOfOrderChunkIsRejected() {
        openSession();
        assertThatThrownBy(() -> service.appendChunk("rec-1", 3, new byte[]{1}, null))
                .isInstanceOf(ConflictException.class)
                .hasMessageContaining("expected sequence 0");
    }

    @Test
    void completionPublishesFinalMarkerAndChecksCount() {
        RecordingSession s = openSession();
        s.markChunkIngested(0, 10, NOW);
        s.markChunkIngested(1, 10, NOW);

        assertThatThrownBy(() -> service.completeRecording("rec-1", new CompleteRecordingRequest(3L)))
                .isInstanceOf(ConflictException.class);

        RecordingCompletion done = service.completeRecording("rec-1", new CompleteRecordingRequest(2L));
        assertThat(done.status()).isEqualTo(RecordingStatus.FINALIZING);
        assertThat(done.totalChunks()).isEqualTo(2);

        ArgumentCaptor<Object> event = ArgumentCaptor.forClass(Object.class);
        verify(kafka).send(eq("recording.chunks"), eq("rec-1"), event.capture());
        RecordingChunkEvent marker = (RecordingChunkEvent) event.getValue();
        assertThat(marker.last()).isTrue();
        assertThat(marker.sequence()).isEqualTo(2);
    }

    private RecordingSession openSession() {
        RecordingSession s = new RecordingSession("rec-1", "metro-pd", "INC-1", "call-9", "audio/ogg", NOW);
        when(sessions.findForUpdate("rec-1")).thenReturn(Optional.of(s));
        return s;
    }

    private static NewMediaMessage meta(String sha) {
        return new NewMediaMessage(null, "metro-pd", "INC-1", "unit-7", MediaKind.IMAGE, Priority.URGENT, null, sha);
    }
}
