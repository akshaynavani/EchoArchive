package com.echoarchive.util;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;

/**
 * SHA-256 helpers used for chain-of-custody checksums.
 */
public final class Checksums {

    private Checksums() {
    }

    public static String sha256Hex(byte[] data) {
        return HexFormat.of().formatHex(newSha256().digest(data));
    }

    public static MessageDigest newSha256() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 not available", e);
        }
    }

    /**
     * Compares a client-declared checksum to the computed one. A null/blank declared value is
     * treated as "not provided" and passes.
     */
    public static boolean matchesIfPresent(String declared, String computed) {
        return declared == null || declared.isBlank() || declared.equalsIgnoreCase(computed);
    }
}
