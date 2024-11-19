package com.echoarchive.metadata;

import org.springframework.data.jpa.repository.JpaRepository;

import java.time.Instant;
import java.util.Collection;
import java.util.List;

public interface RecordingSessionRepository extends JpaRepository<RecordingSession, String> {

    List<RecordingSession> findByStatusInAndLastActivityAtBefore(Collection<RecordingStatus> statuses,
                                                                  Instant cutoff);

    List<RecordingSession> findByIncidentIdOrderByStartedAtAsc(String incidentId);
}
