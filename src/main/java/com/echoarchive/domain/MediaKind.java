package com.echoarchive.domain;

/**
 * Kinds of media handled by the archive. Determines the key prefix and default extension.
 */
public enum MediaKind {
    IMAGE("image", "bin"),
    VIDEO("video", "bin"),
    VOICE_NOTE("voice", "bin"),
    TEXT("text", "txt"),
    CALL_RECORDING("recording", "bin");

    private final String prefix;
    private final String defaultExtension;

    MediaKind(String prefix, String defaultExtension) {
        this.prefix = prefix;
        this.defaultExtension = defaultExtension;
    }

    public String prefix() {
        return prefix;
    }

    public String defaultExtension() {
        return defaultExtension;
    }
}
