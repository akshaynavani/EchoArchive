CREATE TABLE recording_session (
    recording_id            VARCHAR(64)  PRIMARY KEY,
    agency                  VARCHAR(64)  NOT NULL,
    incident_id             VARCHAR(64)  NOT NULL,
    call_id                 VARCHAR(64)  NOT NULL,
    content_type            VARCHAR(128) NOT NULL,
    status                  VARCHAR(16)  NOT NULL,
    last_ingested_sequence  BIGINT       NOT NULL DEFAULT -1,
    last_persisted_sequence BIGINT       NOT NULL DEFAULT -1,
    bytes_ingested          BIGINT       NOT NULL DEFAULT 0,
    object_key              VARCHAR(512),
    upload_id               VARCHAR(256),
    next_part_number        INTEGER      NOT NULL DEFAULT 1,
    started_at              TIMESTAMP WITH TIME ZONE NOT NULL,
    last_activity_at        TIMESTAMP WITH TIME ZONE NOT NULL,
    completed_at            TIMESTAMP WITH TIME ZONE,
    version                 BIGINT       NOT NULL DEFAULT 0
);

CREATE INDEX idx_recording_session_incident ON recording_session (incident_id);
CREATE INDEX idx_recording_session_status_activity ON recording_session (status, last_activity_at);
