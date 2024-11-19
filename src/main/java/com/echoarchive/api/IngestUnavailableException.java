package com.echoarchive.api;

/**
 * Raised when media could not be handed to Kafka. The client must retry; nothing was accepted.
 */
public class IngestUnavailableException extends RuntimeException {
    public IngestUnavailableException(String message, Throwable cause) {
        super(message, cause);
    }
}
