package com.echoarchive.audit;

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.time.Clock;

@Service
public class AuditService {

    private final AuditRepository repository;
    private final Clock clock;

    public AuditService(AuditRepository repository, Clock clock) {
        this.repository = repository;
        this.clock = clock;
    }

    /**
     * Runs in its own transaction so the audit row is written even if the caller later fails.
     */
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void record(String actor, AuditEntry.Action action, String mediaId, String incidentId,
                       String clientIp, String detail) {
        String trimmed = detail != null && detail.length() > 512 ? detail.substring(0, 512) : detail;
        repository.save(new AuditEntry(actor, action, mediaId, incidentId, clientIp, trimmed,
                clock.instant()));
    }
}
