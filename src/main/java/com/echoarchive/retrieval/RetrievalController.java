package com.echoarchive.retrieval;

import com.echoarchive.api.BadRequestException;
import com.echoarchive.audit.AuditEntry;
import com.echoarchive.audit.AuditRepository;
import com.echoarchive.domain.MediaKind;
import com.echoarchive.domain.Priority;
import com.echoarchive.retrieval.dto.DownloadLink;
import com.echoarchive.retrieval.dto.IncidentTimeline;
import com.echoarchive.retrieval.dto.MediaQuery;
import com.echoarchive.retrieval.dto.MediaView;
import com.echoarchive.retrieval.dto.RecordingStatusView;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.Instant;

/**
 * Read side of the archive. Callers identify themselves with {@code X-Actor} (in production this
 * is set by the authenticating gateway); the value is written to the audit log.
 */
@RestController
@RequestMapping("/api/v1")
public class RetrievalController {

    private static final String ACTOR_HEADER = "X-Actor";

    private final RetrievalService retrieval;
    private final AuditRepository auditLog;

    public RetrievalController(RetrievalService retrieval, AuditRepository auditLog) {
        this.retrieval = retrieval;
        this.auditLog = auditLog;
    }

    @GetMapping("/media")
    public Page<MediaView> search(
            @RequestHeader(ACTOR_HEADER) String actor,
            @RequestParam(required = false) String agency,
            @RequestParam(required = false) String incidentId,
            @RequestParam(required = false) String senderId,
            @RequestParam(required = false) MediaKind kind,
            @RequestParam(required = false) Priority minPriority,
            @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) Instant from,
            @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) Instant to,
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "50") int size,
            HttpServletRequest request) {
        MediaQuery q = new MediaQuery(agency, incidentId, senderId, kind, minPriority, from, to);
        return retrieval.search(q, page, size, requireActor(actor), request.getRemoteAddr());
    }

    @GetMapping("/media/{mediaId}")
    public MediaView get(@RequestHeader(ACTOR_HEADER) String actor, @PathVariable String mediaId,
                         HttpServletRequest request) {
        return retrieval.get(mediaId, requireActor(actor), request.getRemoteAddr());
    }

    @GetMapping("/media/{mediaId}/download-url")
    public DownloadLink downloadUrl(@RequestHeader(ACTOR_HEADER) String actor, @PathVariable String mediaId,
                                    HttpServletRequest request) {
        return retrieval.downloadLink(mediaId, requireActor(actor), request.getRemoteAddr());
    }

    @GetMapping("/incidents/{incidentId}")
    public IncidentTimeline incident(@RequestHeader(ACTOR_HEADER) String actor,
                                     @PathVariable String incidentId, HttpServletRequest request) {
        return retrieval.incident(incidentId, requireActor(actor), request.getRemoteAddr());
    }

    @GetMapping("/incidents/{incidentId}/audit")
    public Page<AuditEntry> incidentAudit(@PathVariable String incidentId,
                                          @RequestParam(defaultValue = "0") int page,
                                          @RequestParam(defaultValue = "100") int size) {
        return auditLog.findByIncidentIdOrderByOccurredAtDesc(incidentId,
                PageRequest.of(Math.max(page, 0), Math.min(Math.max(size, 1), 500)));
    }

    @GetMapping("/recordings/{recordingId}")
    public RecordingStatusView recordingStatus(@PathVariable String recordingId) {
        return retrieval.recordingStatus(recordingId);
    }

    private static String requireActor(String actor) {
        if (actor == null || actor.isBlank() || actor.length() > 128) {
            throw new BadRequestException(ACTOR_HEADER + " header is required");
        }
        return actor.trim();
    }
}
