package com.echoarchive.consume;

import com.echoarchive.domain.RecordingChunkEvent;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Feeds recording chunks to the assembler. Records are keyed by recording ID, so every chunk of a
 * given call is consumed in order by a single listener thread.
 */
@Component
public class RecordingChunkConsumer {

    private final RecordingAssembler assembler;

    public RecordingChunkConsumer(RecordingAssembler assembler) {
        this.assembler = assembler;
    }

    @KafkaListener(
            id = "recording-chunk-persister",
            topics = "${echoarchive.topics.recording-chunks}",
            containerFactory = "recordingChunkListenerFactory")
    public void onChunk(RecordingChunkEvent event) {
        assembler.accept(event);
    }
}
