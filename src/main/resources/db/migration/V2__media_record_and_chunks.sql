CREATE TABLE media_record (
    media_id     VARCHAR(64)  PRIMARY KEY,
    agency       VARCHAR(64)  NOT NULL,
    incident_id  VARCHAR(64)  NOT NULL,
    sender_id    VARCHAR(64),
    call_id      VARCHAR(64),
    kind         VARCHAR(32)  NOT NULL,
    priority     VARCHAR(16)  NOT NULL,
    content_type VARCHAR(128) NOT NULL,
    file_name    VARCHAR(255),
    size_bytes   BIGINT       NOT NULL,
    sha256       VARCHAR(64)  NOT NULL,
    bucket       VARCHAR(128) NOT NULL,
    object_key   VARCHAR(512) NOT NULL,
    captured_at  TIMESTAMP WITH TIME ZONE NOT NULL,
    received_at  TIMESTAMP WITH TIME ZONE NOT NULL,
    persisted_at TIMESTAMP WITH TIME ZONE NOT NULL
);

CREATE INDEX idx_media_record_incident ON media_record (incident_id, captured_at);
CREATE INDEX idx_media_record_agency_time ON media_record (agency, captured_at);

CREATE TABLE recording_chunk (
    recording_id VARCHAR(64) NOT NULL,
    sequence     BIGINT      NOT NULL,
    size_bytes   BIGINT      NOT NULL,
    sha256       VARCHAR(64) NOT NULL,
    payload      BYTEA,
    part_number  INTEGER,
    received_at  TIMESTAMP WITH TIME ZONE NOT NULL,
    PRIMARY KEY (recording_id, sequence)
);

CREATE TABLE recording_part (
    recording_id VARCHAR(64)  NOT NULL,
    part_number  INTEGER      NOT NULL,
    etag         VARCHAR(128) NOT NULL,
    size_bytes   BIGINT       NOT NULL,
    PRIMARY KEY (recording_id, part_number)
);
