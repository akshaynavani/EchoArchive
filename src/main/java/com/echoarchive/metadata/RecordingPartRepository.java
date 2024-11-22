package com.echoarchive.metadata;

import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

public interface RecordingPartRepository extends JpaRepository<RecordingPart, RecordingPart.Key> {

    List<RecordingPart> findByRecordingIdOrderByPartNumberAsc(String recordingId);

    void deleteByRecordingId(String recordingId);
}
