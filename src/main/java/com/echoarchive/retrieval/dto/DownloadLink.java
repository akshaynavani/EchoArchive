package com.echoarchive.retrieval.dto;

import java.time.Instant;

public record DownloadLink(String mediaId, String url, String sha256, Instant expiresAt) {
}
