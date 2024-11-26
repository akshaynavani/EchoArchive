package com.echoarchive.retrieval;

import com.echoarchive.api.BadRequestException;
import com.echoarchive.audit.AuditEntry;
import com.echoarchive.audit.AuditService;
import com.echoarchive.domain.MediaKind;
import com.echoarchive.metadata.MediaRecord;
import com.echoarchive.metadata.MediaRecordRepository;
import com.echoarchive.metadata.RecordingSessionRepository;
import com.echoarchive.retrieval.dto.DownloadLink;
import com.echoarchive.retrieval.dto.MediaQuery;
import com.echoarchive.support.InMemoryObjectStore;
import com.echoarchive.support.TestProperties;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class RetrievalServiceTest {

    private static final Instant NOW = Instant.parse("2026-05-01T12:00:00Z");

    private final MediaRecordRepository records = mock(MediaRecordRepository.class);
    private final AuditService audit = mock(AuditService.class);
    private final RetrievalService service = new RetrievalService(records, mock(RecordingSessionRepository.class),
            new InMemoryObjectStore(), audit, TestProperties.defaults(), Clock.fixed(NOW, ZoneOffset.UTC));

    @Test
    void downloadLinkIsPresignedAndAudited() {
        when(records.findById("m-1")).thenReturn(Optional.of(MediaRecord.builder()
                .mediaId("m-1").agency("metro-pd").incidentId("INC-1").kind(MediaKind.IMAGE)
                .contentType("image/jpeg").sha256("abc").bucket("test-bucket")
                .objectKey("metro-pd/INC-1/2026/05/01/image/m-1.jpg")
                .capturedAt(NOW).receivedAt(NOW).persistedAt(NOW).build()));

        DownloadLink link = service.downloadLink("m-1", "sgt.lee", "10.0.0.5");

        assertThat(link.url()).contains("m-1.jpg").contains("X-Amz-Expires=900");
        assertThat(link.expiresAt()).isEqualTo(NOW.plusSeconds(900));
        verify(audit).record(eq("sgt.lee"), eq(AuditEntry.Action.ISSUE_DOWNLOAD_URL), eq("m-1"),
                eq("INC-1"), eq("10.0.0.5"), eq("ttl=PT15M"));
    }

    @Test
    void unscopedSearchIsRejected() {
        MediaQuery q = new MediaQuery(null, null, "unit-7", null, null, null, null);
        assertThatThrownBy(() -> service.search(q, 0, 10, "sgt.lee", "10.0.0.5"))
                .isInstanceOf(BadRequestException.class);
    }

    @Test
    void invertedTimeRangeIsRejected() {
        MediaQuery q = new MediaQuery("metro-pd", null, null, null, null, NOW, NOW.minusSeconds(1));
        assertThatThrownBy(() -> service.search(q, 0, 10, "sgt.lee", "10.0.0.5"))
                .isInstanceOf(BadRequestException.class);
    }
}
