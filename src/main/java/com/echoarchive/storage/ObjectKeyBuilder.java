package com.echoarchive.storage;

import com.echoarchive.domain.MediaKind;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Locale;
import java.util.Map;

/**
 * Object key layout: {@code {agency}/{incidentId}/{yyyy}/{MM}/{dd}/{kind}/{mediaId}.{ext}}.
 * Grouping by agency then incident makes post-incident export and per-agency lifecycle/IAM
 * policies a simple prefix match.
 */
@Component
public class ObjectKeyBuilder {

    private static final DateTimeFormatter DATE_PATH =
            DateTimeFormatter.ofPattern("yyyy/MM/dd").withZone(ZoneOffset.UTC);

    private static final Map<String, String> EXTENSIONS = Map.ofEntries(
            Map.entry("image/jpeg", "jpg"),
            Map.entry("image/png", "png"),
            Map.entry("image/heic", "heic"),
            Map.entry("video/mp4", "mp4"),
            Map.entry("video/quicktime", "mov"),
            Map.entry("audio/ogg", "ogg"),
            Map.entry("audio/opus", "opus"),
            Map.entry("audio/mpeg", "mp3"),
            Map.entry("audio/wav", "wav"),
            Map.entry("audio/x-wav", "wav"),
            Map.entry("audio/amr", "amr"),
            Map.entry("text/plain", "txt"));

    public String build(String agency, String incidentId, MediaKind kind, Instant timestamp,
                        String mediaId, String contentType, String fileName) {
        return String.join("/",
                agency,
                incidentId,
                DATE_PATH.format(timestamp),
                kind.prefix(),
                mediaId + "." + extension(kind, contentType, fileName));
    }

    String extension(MediaKind kind, String contentType, String fileName) {
        if (fileName != null) {
            int dot = fileName.lastIndexOf('.');
            if (dot >= 0 && dot < fileName.length() - 1) {
                String ext = fileName.substring(dot + 1).toLowerCase(Locale.ROOT);
                if (ext.matches("[a-z0-9]{1,8}")) {
                    return ext;
                }
            }
        }
        if (contentType != null) {
            String base = contentType.split(";")[0].trim().toLowerCase(Locale.ROOT);
            String mapped = EXTENSIONS.get(base);
            if (mapped != null) {
                return mapped;
            }
        }
        return kind.defaultExtension();
    }
}
