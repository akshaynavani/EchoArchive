package com.echoarchive.retrieval;

import com.echoarchive.api.BadRequestException;
import com.echoarchive.api.NotFoundException;
import com.echoarchive.audit.AuditEntry;
import com.echoarchive.audit.AuditService;
import com.echoarchive.config.EchoArchiveProperties;
import com.echoarchive.metadata.MediaRecord;
import com.echoarchive.metadata.MediaRecordRepository;
import com.echoarchive.metadata.RecordingSessionRepository;
import com.echoarchive.metadata.RecordingStatus;
import com.echoarchive.retrieval.dto.DownloadLink;
import com.echoarchive.retrieval.dto.IncidentTimeline;
import com.echoarchive.retrieval.dto.MediaQuery;
import com.echoarchive.retrieval.dto.MediaView;
import com.echoarchive.retrieval.dto.RecordingStatusView;
import com.echoarchive.storage.ObjectStore;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.net.URL;
import java.time.Clock;
import java.time.Duration;
import java.util.List;

/**
 * Post-incident retrieval. Every access is audited with the caller's identity.
 */
@Service
@Transactional(readOnly = true)
public class RetrievalService {

    private static final int MAX_PAGE_SIZE = 200;

    private final MediaRecordRepository records;
    private final RecordingSessionRepository sessions;
    private final ObjectStore store;
    private final AuditService audit;
    private final Duration presignTtl;
    private final Clock clock;

    public RetrievalService(MediaRecordRepository records, RecordingSessionRepository sessions,
                            ObjectStore store, AuditService audit, EchoArchiveProperties props,
                            Clock clock) {
        this.records = records;
        this.sessions = sessions;
        this.store = store;
        this.audit = audit;
        this.presignTtl = props.retrieval().presignTtl();
        this.clock = clock;
    }

    public Page<MediaView> search(MediaQuery query, int page, int size, String actor, String clientIp) {
        if (query.agency() == null && query.incidentId() == null) {
            throw new BadRequestException("Provide at least agency or incidentId");
        }
        if (query.from() != null && query.to() != null && !query.from().isBefore(query.to())) {
            throw new BadRequestException("'from' must be before 'to'");
        }
        Pageable pageable = PageRequest.of(Math.max(page, 0), Math.min(Math.max(size, 1), MAX_PAGE_SIZE),
                Sort.by(Sort.Direction.ASC, "capturedAt"));
        audit.record(actor, AuditEntry.Action.SEARCH, null, query.incidentId(), clientIp, query.describe());
        return records.findAll(MediaSpecifications.matching(query), pageable).map(MediaView::of);
    }

    public MediaView get(String mediaId, String actor, String clientIp) {
        MediaRecord r = load(mediaId);
        audit.record(actor, AuditEntry.Action.VIEW_METADATA, mediaId, r.getIncidentId(), clientIp, null);
        return MediaView.of(r);
    }

    public DownloadLink downloadLink(String mediaId, String actor, String clientIp) {
        MediaRecord r = load(mediaId);
        String fileName = r.getFileName() != null ? r.getFileName()
                : r.getObjectKey().substring(r.getObjectKey().lastIndexOf('/') + 1);
        URL url = store.presignGet(r.getObjectKey(), presignTtl, fileName);
        audit.record(actor, AuditEntry.Action.ISSUE_DOWNLOAD_URL, mediaId, r.getIncidentId(), clientIp,
                "ttl=" + presignTtl);
        return new DownloadLink(mediaId, url.toString(), r.getSha256(), clock.instant().plus(presignTtl));
    }

    public IncidentTimeline incident(String incidentId, String actor, String clientIp) {
        List<MediaView> media = records.findAll(
                        MediaSpecifications.matching(new MediaQuery(null, incidentId, null, null, null, null, null)),
                        Sort.by("capturedAt"))
                .stream().map(MediaView::of).toList();
        List<RecordingStatusView> inFlight = sessions.findByIncidentIdOrderByStartedAtAsc(incidentId).stream()
                .filter(s -> s.getStatus() == RecordingStatus.OPEN || s.getStatus() == RecordingStatus.FINALIZING)
                .map(RecordingStatusView::of)
                .toList();
        if (media.isEmpty() && inFlight.isEmpty()) {
            throw new NotFoundException("No media for incident " + incidentId);
        }
        audit.record(actor, AuditEntry.Action.SEARCH, null, incidentId, clientIp, "incident timeline");
        return new IncidentTimeline(incidentId, media, inFlight);
    }

    public RecordingStatusView recordingStatus(String recordingId) {
        return sessions.findById(recordingId)
                .map(RecordingStatusView::of)
                .orElseThrow(() -> new NotFoundException("Unknown recording " + recordingId));
    }

    private MediaRecord load(String mediaId) {
        return records.findById(mediaId)
                .orElseThrow(() -> new NotFoundException("Unknown media " + mediaId));
    }
}
