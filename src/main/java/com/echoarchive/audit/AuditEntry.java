package com.echoarchive.audit;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.time.Instant;

/**
 * Append-only record of who accessed which media. Post-incident review and evidentiary use both
 * require knowing every retrieval.
 */
@Entity
@Table(name = "audit_entry")
public class AuditEntry {

    public enum Action { SEARCH, VIEW_METADATA, ISSUE_DOWNLOAD_URL }

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(nullable = false, length = 128)
    private String actor;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 32)
    private Action action;

    @Column(name = "media_id", length = 64)
    private String mediaId;

    @Column(name = "incident_id", length = 64)
    private String incidentId;

    @Column(name = "client_ip", length = 64)
    private String clientIp;

    @Column(length = 512)
    private String detail;

    @Column(name = "occurred_at", nullable = false)
    private Instant occurredAt;

    protected AuditEntry() {
    }

    public AuditEntry(String actor, Action action, String mediaId, String incidentId, String clientIp,
                      String detail, Instant occurredAt) {
        this.actor = actor;
        this.action = action;
        this.mediaId = mediaId;
        this.incidentId = incidentId;
        this.clientIp = clientIp;
        this.detail = detail;
        this.occurredAt = occurredAt;
    }

    public Long getId() { return id; }
    public String getActor() { return actor; }
    public Action getAction() { return action; }
    public String getMediaId() { return mediaId; }
    public String getIncidentId() { return incidentId; }
    public String getClientIp() { return clientIp; }
    public String getDetail() { return detail; }
    public Instant getOccurredAt() { return occurredAt; }
}
