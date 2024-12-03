package com.echoarchive;

import com.echoarchive.metadata.MediaRecordRepository;
import com.echoarchive.metadata.RecordingSessionRepository;
import com.echoarchive.metadata.RecordingStatus;
import com.echoarchive.util.Checksums;
import com.fasterxml.jackson.databind.JsonNode;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.testcontainers.containers.MinIOContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.services.s3.S3Client;

import java.io.ByteArrayOutputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Map;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * End-to-end: REST ingest -> Kafka -> consumers -> MinIO (S3 API) -> retrieval.
 * Requires Docker. Run with {@code mvn verify}.
 */
@Testcontainers
@ActiveProfiles({"test", "onprem"})
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class EchoArchivePipelineIT {

    @Container
    static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:3.7.1");

    @Container
    static final MinIOContainer MINIO = new MinIOContainer("minio/minio:RELEASE.2024-09-22T00-33-43Z");

    @DynamicPropertySource
    static void props(DynamicPropertyRegistry r) {
        r.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
        r.add("echoarchive.storage.endpoint", MINIO::getS3URL);
        r.add("echoarchive.storage.access-key", MINIO::getUserName);
        r.add("echoarchive.storage.secret-key", MINIO::getPassword);
        r.add("echoarchive.storage.create-bucket-if-missing", () -> "true");
        r.add("echoarchive.ingest.max-chunk-bytes", () -> String.valueOf(2 * 1024 * 1024));
    }

    @Autowired TestRestTemplate http;
    @Autowired MediaRecordRepository records;
    @Autowired RecordingSessionRepository sessions;
    @Autowired S3Client s3;

    @Test
    void messageIsArchivedAndRetrievableViaPresignedUrl() throws Exception {
        byte[] photo = randomBytes(300_000);

        MultiValueMap<String, Object> form = new LinkedMultiValueMap<>();
        form.add("file", new ByteArrayResource(photo) {
            @Override
            public String getFilename() {
                return "scene.jpg";
            }
        });
        form.add("agency", "metro-fd");
        form.add("incidentId", "INC-2001");
        form.add("senderId", "engine-12");
        form.add("messageId", "msg-it-1");
        form.add("priority", "EMERGENCY");
        form.add("sha256", Checksums.sha256Hex(photo));

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.MULTIPART_FORM_DATA);
        ResponseEntity<JsonNode> accepted = http.postForEntity("/api/v1/messages",
                new HttpEntity<>(form, headers), JsonNode.class);
        assertThat(accepted.getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);

        await().atMost(Duration.ofSeconds(30)).until(() -> records.existsById("msg-it-1"));

        ResponseEntity<JsonNode> link = http.exchange("/api/v1/media/msg-it-1/download-url", HttpMethod.GET,
                new HttpEntity<>(actor()), JsonNode.class);
        assertThat(link.getStatusCode()).isEqualTo(HttpStatus.OK);

        HttpResponse<byte[]> download = HttpClient.newHttpClient().send(
                HttpRequest.newBuilder(URI.create(link.getBody().get("url").asText())).build(),
                HttpResponse.BodyHandlers.ofByteArray());
        assertThat(download.statusCode()).isEqualTo(200);
        assertThat(download.body()).isEqualTo(photo);

        ResponseEntity<JsonNode> audit = http.getForEntity("/api/v1/incidents/INC-2001/audit", JsonNode.class);
        assertThat(audit.getBody().get("content").get(0).get("action").asText()).isEqualTo("ISSUE_DOWNLOAD_URL");
    }

    @Test
    void chunkedRecordingIsAssembledIntoOneObject() {
        ResponseEntity<JsonNode> started = http.postForEntity("/api/v1/recordings", Map.of(
                "recordingId", "rec-it-1",
                "agency", "metro-pd",
                "incidentId", "INC-2002",
                "callId", "call-77",
                "contentType", "audio/ogg"), JsonNode.class);
        assertThat(started.getStatusCode()).isEqualTo(HttpStatus.CREATED);

        ByteArrayOutputStream expected = new ByteArrayOutputStream();
        int chunks = 4; // 4 x 2 MiB = 8 MiB -> one 6 MiB part + one 2 MiB final part
        for (int i = 0; i < chunks; i++) {
            byte[] chunk = randomBytes(2 * 1024 * 1024);
            expected.writeBytes(chunk);
            HttpHeaders h = new HttpHeaders();
            h.setContentType(MediaType.APPLICATION_OCTET_STREAM);
            h.set("X-Content-SHA256", Checksums.sha256Hex(chunk));
            ResponseEntity<JsonNode> res = http.exchange("/api/v1/recordings/rec-it-1/chunks/" + i,
                    HttpMethod.PUT, new HttpEntity<>(chunk, h), JsonNode.class);
            assertThat(res.getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);
        }

        ResponseEntity<JsonNode> done = http.postForEntity("/api/v1/recordings/rec-it-1/complete",
                Map.of("expectedChunks", chunks), JsonNode.class);
        assertThat(done.getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);

        await().atMost(Duration.ofSeconds(60)).until(() ->
                sessions.findById("rec-it-1").map(s -> s.getStatus() == RecordingStatus.STORED).orElse(false));

        String key = records.findById("rec-it-1").orElseThrow().getObjectKey();
        byte[] stored = s3.getObject(b -> b.bucket("echoarchive-media").key(key),
                ResponseTransformer.toBytes()).asByteArray();
        assertThat(stored).isEqualTo(expected.toByteArray());
    }

    private static HttpHeaders actor() {
        HttpHeaders h = new HttpHeaders();
        h.set("X-Actor", "it-reviewer");
        return h;
    }

    private static byte[] randomBytes(int n) {
        byte[] b = new byte[n];
        new Random(n).nextBytes(b);
        return b;
    }
}
