package com.echoarchive.metadata;

public enum RecordingStatus {
    /** Accepting chunks from the recorder. */
    OPEN,
    /** Final chunk ingested; waiting for consumers to finish the multipart upload. */
    FINALIZING,
    /** Fully stored in object storage. */
    STORED,
    /** Abandoned or failed; multipart upload aborted. */
    ABORTED
}
