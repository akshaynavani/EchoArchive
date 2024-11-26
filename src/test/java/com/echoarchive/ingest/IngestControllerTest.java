package com.echoarchive.ingest;

import com.echoarchive.api.ConflictException;
import com.echoarchive.api.IngestUnavailableException;
import com.echoarchive.ingest.dto.ChunkAccepted;
import com.echoarchive.ingest.dto.MessageAccepted;
import com.echoarchive.ingest.dto.RecordingStarted;
import com.echoarchive.metadata.RecordingStatus;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.mock.web.MockMultipartFile;
import org.springframework.test.web.servlet.MockMvc;

import java.time.Instant;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.multipart;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest({MediaIngestController.class, RecordingIngestController.class})
class IngestControllerTest {

    @Autowired MockMvc mvc;
    @MockBean IngestService ingest;

    @Test
    void uploadReturnsAccepted() throws Exception {
        when(ingest.ingestMessage(any(), eq("image/jpeg"), eq("scene.jpg"), any()))
                .thenReturn(new MessageAccepted("m-1", "INC-1", "abc", 3, Instant.now()));

        mvc.perform(multipart("/api/v1/messages")
                        .file(new MockMultipartFile("file", "scene.jpg", "image/jpeg", new byte[]{1, 2, 3}))
                        .param("agency", "metro-pd")
                        .param("incidentId", "INC-1")
                        .param("senderId", "unit-7")
                        .param("priority", "EMERGENCY"))
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.messageId").value("m-1"));
    }

    @Test
    void uploadWithoutIncidentIsBadRequest() throws Exception {
        mvc.perform(multipart("/api/v1/messages")
                        .file(new MockMultipartFile("file", "a.jpg", "image/jpeg", new byte[]{1}))
                        .param("agency", "metro-pd")
                        .param("senderId", "unit-7"))
                .andExpect(status().isBadRequest());
    }

    @Test
    void kafkaOutageIsServiceUnavailableWithRetryAfter() throws Exception {
        when(ingest.ingestMessage(any(), anyString(), anyString(), any()))
                .thenThrow(new IngestUnavailableException("down", new RuntimeException()));

        mvc.perform(multipart("/api/v1/messages")
                        .file(new MockMultipartFile("file", "a.jpg", "image/jpeg", new byte[]{1}))
                        .param("agency", "metro-pd")
                        .param("incidentId", "INC-1")
                        .param("senderId", "unit-7"))
                .andExpect(status().isServiceUnavailable())
                .andExpect(header().string("Retry-After", "5"));
    }

    @Test
    void startRecordingValidatesBody() throws Exception {
        mvc.perform(post("/api/v1/recordings")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"agency\":\"metro-pd\",\"incidentId\":\"INC/1\",\"callId\":\"c1\",\"contentType\":\"audio/ogg\"}"))
                .andExpect(status().isBadRequest());

        when(ingest.startRecording(any())).thenReturn(new RecordingStarted("rec-1", RecordingStatus.OPEN, 0));
        mvc.perform(post("/api/v1/recordings")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"agency\":\"metro-pd\",\"incidentId\":\"INC-1\",\"callId\":\"c1\",\"contentType\":\"audio/ogg\"}"))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.recordingId").value("rec-1"));
    }

    @Test
    void chunkUploadPassesChecksumHeader() throws Exception {
        when(ingest.appendChunk(eq("rec-1"), eq(0L), any(), eq("abc")))
                .thenReturn(new ChunkAccepted("rec-1", 0, false, 1));

        mvc.perform(put("/api/v1/recordings/rec-1/chunks/0")
                        .contentType(MediaType.APPLICATION_OCTET_STREAM)
                        .header("X-Content-SHA256", "abc")
                        .content(new byte[]{1, 2}))
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.nextSequence").value(1));
    }

    @Test
    void outOfOrderChunkIsConflict() throws Exception {
        when(ingest.appendChunk(eq("rec-1"), anyLong(), any(), isNull()))
                .thenThrow(new ConflictException("Out-of-order chunk"));

        mvc.perform(put("/api/v1/recordings/rec-1/chunks/5")
                        .contentType(MediaType.APPLICATION_OCTET_STREAM)
                        .content(new byte[]{1}))
                .andExpect(status().isConflict());
    }
}
