# EchoArchive — Implementation Plan

Mission-critical multimedia messaging and call-recording backend for public-safety teams.
Media (images, video clips, voice notes, call-recording audio) is ingested over REST, routed
through Kafka, and persisted to S3-compatible object storage (AWS S3 in the cloud, MinIO on-prem)
so it can be retrieved after an incident.

**Stack:** Java 21, Spring Boot 3.3, Spring Kafka, AWS SDK v2 (S3), Spring Data JPA
(PostgreSQL in prod, H2 for tests), Testcontainers, Docker Compose (Kafka, Postgres, MinIO).

## Key design decisions

- **Kafka as the durable buffer.** The API acknowledges uploads once Kafka has them (`acks=all`,
  idempotent producer). Storage outages therefore don't drop field media; consumers catch up.
- **Topics:** `media.messages` (complete multimedia messages, keyed by incident ID so per-incident
  ordering holds), `recording.chunks` (call-recording audio chunks, keyed by recording/call ID so
  all chunks for a call land on one partition in order), `media.persisted` (events after storage),
  plus `*.DLT` dead-letter topics.
- **Payload size:** Kafka messages carry payload bytes up to a configurable limit (default 5 MB);
  recordings are streamed as chunks so no single record is huge.
- **Recordings use S3 multipart upload**: one part per chunk batch (min 5 MB except last),
  completed when the "final" chunk arrives. Upload state is tracked in the DB so a consumer
  restart resumes cleanly.
- **Idempotency:** each message/chunk has a client-supplied ID; the metadata table has a unique
  constraint so Kafka redelivery doesn't create duplicates.
- **Object key layout:** `{agency}/{incidentId}/{yyyy}/{MM}/{dd}/{type}/{id}.{ext}`, SHA-256
  checksum stored in metadata and as object metadata for chain-of-custody.
- **Deployment profiles:** `cloud` (AWS S3, default credential chain) and `onprem`
  (MinIO endpoint, path-style access). Same code; only config differs.
- **Retrieval:** search metadata by incident/agency/time window; download via short-lived
  presigned URLs. Every retrieval is written to an audit log.

## Phase 1 — Project scaffold, domain model and local infrastructure
Deliverables: Maven build, Spring Boot app, configuration properties, domain model, local stack.
Files: `pom.xml`, `src/main/java/com/echoarchive/EchoArchiveApplication.java`,
`config/EchoArchiveProperties.java`, `domain/*` (MediaType, MediaMessageEvent,
RecordingChunkEvent, MediaPersistedEvent), `src/main/resources/application.yml`,
`application-cloud.yml`, `application-onprem.yml`, `docker-compose.yml`, `.env.example`.

## Phase 2 — Ingestion API and Kafka producers
Deliverables: REST endpoints for multimedia messages (multipart upload) and call-recording chunk
streaming (start / append chunk / finish), validation, Kafka topic + producer config, error handling.
Files: `config/KafkaTopicConfig.java`, `config/KafkaProducerConfig.java`,
`ingest/MediaIngestController.java`, `ingest/RecordingIngestController.java`,
`ingest/IngestService.java`, `ingest/dto/*`, `api/GlobalExceptionHandler.java`,
`util/Checksums.java`.

## Phase 3 — Storage layer and Kafka consumers
Deliverables: storage abstraction over S3 (put object, multipart create/upload-part/complete/abort),
S3 client config for cloud vs on-prem, key builder, JPA metadata entities and repositories,
consumers for messages and recording chunks with idempotency, retry with backoff and DLT,
`media.persisted` event publishing.
Files: `config/S3Config.java`, `storage/*` (ObjectStore, S3ObjectStore, ObjectKeyBuilder),
`metadata/*` (MediaRecord, RecordingSession, repositories), `consume/MediaMessageConsumer.java`,
`consume/RecordingChunkConsumer.java`, `consume/RecordingAssembler.java`,
`config/KafkaConsumerConfig.java`, `src/main/resources/db/migration/*` (Flyway).

## Phase 4 — Post-incident retrieval and audit
Deliverables: search API (incident, agency, type, time range, pagination), presigned download
URLs, recording status endpoint, retrieval audit log, retention cleanup job for aborted
multipart uploads, actuator health indicator for S3.
Files: `retrieval/RetrievalController.java`, `retrieval/RetrievalService.java`,
`retrieval/dto/*`, `audit/*`, `storage/S3HealthIndicator.java`, `maintenance/StaleUploadReaper.java`.

## Phase 5 — Tests and hardening
Deliverables: unit tests (key builder, checksum, assembler buffering, ingest validation),
controller slice tests, Testcontainers integration test (Kafka + MinIO) covering the end-to-end
path, Dockerfile.
Files: `src/test/java/com/echoarchive/**`, `src/test/resources/application-test.yml`, `Dockerfile`.
