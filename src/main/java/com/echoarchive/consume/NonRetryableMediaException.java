package com.echoarchive.consume;

/**
 * Failure that retrying won't fix (corrupt payload, impossible state). Sent straight to the DLT.
 */
public class NonRetryableMediaException extends RuntimeException {
    public NonRetryableMediaException(String message) {
        super(message);
    }
}
