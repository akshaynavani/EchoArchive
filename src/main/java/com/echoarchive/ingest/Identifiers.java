package com.echoarchive.ingest;

import com.echoarchive.api.BadRequestException;

import java.util.regex.Pattern;

/**
 * Identifiers end up in S3 object keys, so they are restricted to a safe character set.
 */
public final class Identifiers {

    public static final String REGEX = "^[A-Za-z0-9._-]{1,64}$";
    private static final Pattern PATTERN = Pattern.compile(REGEX);

    private Identifiers() {
    }

    public static String require(String field, String value) {
        if (value == null || !PATTERN.matcher(value).matches() || value.contains("..")) {
            throw new BadRequestException(field + " must match " + REGEX + " and not contain '..'");
        }
        return value;
    }
}
