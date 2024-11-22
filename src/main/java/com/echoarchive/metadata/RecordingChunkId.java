package com.echoarchive.metadata;

import java.io.Serializable;

public record RecordingChunkId(String recordingId, long sequence) implements Serializable {
}
