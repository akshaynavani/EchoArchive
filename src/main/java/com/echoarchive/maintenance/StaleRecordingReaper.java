package com.echoarchive.maintenance;

import com.echoarchive.config.EchoArchiveProperties;
import com.echoarchive.ingest.IngestService;
import com.echoarchive.metadata.RecordingSession;
import com.echoarchive.metadata.RecordingSessionRepository;
import com.echoarchive.metadata.RecordingStatus;
import com.echoarchive.storage.ObjectStore;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.EnumSet;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Handles recordings whose recorder went silent (radio out of range, device crash):
 * <ul>
 *   <li>OPEN with audio: salvaged by completing it, so the partial call is still archived.</li>
 *   <li>OPEN with no audio: aborted.</li>
 *   <li>FINALIZING for too long: persistence is stuck (likely dead-lettered). Never aborted
 *       automatically, since that could destroy evidence; surfaced via a gauge and error log.</li>
 * </ul>
 */
@Component
public class StaleRecordingReaper {

    private static final Logger log = LoggerFactory.getLogger(StaleRecordingReaper.class);

    private final RecordingSessionRepository sessions;
    private final IngestService ingest;
    private final ObjectStore store;
    private final TransactionTemplate tx;
    private final Duration staleAfter;
    private final Clock clock;
    private final AtomicInteger stuckFinalizing = new AtomicInteger();

    public StaleRecordingReaper(RecordingSessionRepository sessions, IngestService ingest, ObjectStore store,
                                TransactionTemplate tx, EchoArchiveProperties props, Clock clock,
                                MeterRegistry meters) {
        this.sessions = sessions;
        this.ingest = ingest;
        this.store = store;
        this.tx = tx;
        this.staleAfter = props.retrieval().staleUploadAfter();
        this.clock = clock;
        meters.gauge("echoarchive.recordings.stuck_finalizing", stuckFinalizing);
    }

    @Scheduled(fixedDelayString = "${echoarchive.maintenance.reaper-interval:PT10M}",
            initialDelayString = "${echoarchive.maintenance.reaper-initial-delay:PT1M}")
    public void reap() {
        Instant cutoff = clock.instant().minus(staleAfter);
        List<RecordingSession> stale = sessions.findByStatusInAndLastActivityAtBefore(
                EnumSet.of(RecordingStatus.OPEN, RecordingStatus.FINALIZING), cutoff);

        int stuck = 0;
        for (RecordingSession s : stale) {
            try {
                if (s.getStatus() == RecordingStatus.FINALIZING) {
                    stuck++;
                    log.error("Recording {} (incident {}) stuck in FINALIZING since {}; check {}.DLT",
                            s.getRecordingId(), s.getIncidentId(), s.getLastActivityAt(), "recording.chunks");
                } else if (s.getLastIngestedSequence() >= 0) {
                    log.warn("Salvaging silent recording {} after {} chunks",
                            s.getRecordingId(), s.getLastIngestedSequence() + 1);
                    ingest.completeRecording(s.getRecordingId(), null);
                } else {
                    abortEmpty(s.getRecordingId());
                }
            } catch (RuntimeException e) {
                log.warn("Reaper failed for recording {}", s.getRecordingId(), e);
            }
        }
        stuckFinalizing.set(stuck);
    }

    private void abortEmpty(String recordingId) {
        tx.executeWithoutResult(status -> sessions.findForUpdate(recordingId).ifPresent(s -> {
            if (s.getStatus() != RecordingStatus.OPEN || s.getLastIngestedSequence() >= 0) {
                return; // raced with a late chunk
            }
            if (s.getUploadId() != null) {
                store.abortMultipartUpload(s.getObjectKey(), s.getUploadId());
            }
            s.markAborted(clock.instant());
            log.info("Aborted empty recording {}", recordingId);
        }));
    }
}
