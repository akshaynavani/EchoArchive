package com.echoarchive.metadata;

import jakarta.persistence.LockModeType;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

public interface RecordingSessionRepository extends JpaRepository<RecordingSession, String> {

    /**
     * Row lock shared by the ingest and persistence sides. It serialises updates to a session so
     * that, for example, the consumer can't process the final marker before ingest has committed
     * the FINALIZING state that preceded it.
     */
    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("select s from RecordingSession s where s.recordingId = :id")
    Optional<RecordingSession> findForUpdate(@Param("id") String recordingId);

    List<RecordingSession> findByStatusInAndLastActivityAtBefore(Collection<RecordingStatus> statuses,
                                                                  Instant cutoff);

    List<RecordingSession> findByIncidentIdOrderByStartedAtAsc(String incidentId);
}
