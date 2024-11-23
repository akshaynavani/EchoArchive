package com.echoarchive.retrieval.dto;

import java.util.List;

/**
 * Everything archived for one incident: stored media in capture order, plus recordings that are
 * still in flight (so reviewers know the picture may be incomplete).
 */
public record IncidentTimeline(String incidentId, List<MediaView> media,
                               List<RecordingStatusView> recordingsInProgress) {
}
