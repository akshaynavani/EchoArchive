package com.echoarchive.audit;

import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;

public interface AuditRepository extends JpaRepository<AuditEntry, Long> {

    Page<AuditEntry> findByIncidentIdOrderByOccurredAtDesc(String incidentId, Pageable pageable);

    Page<AuditEntry> findByMediaIdOrderByOccurredAtDesc(String mediaId, Pageable pageable);
}
