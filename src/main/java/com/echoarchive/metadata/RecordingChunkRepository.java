package com.echoarchive.metadata;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.util.List;

public interface RecordingChunkRepository extends JpaRepository<RecordingChunk, RecordingChunkId> {

    /** Chunks staged in the DB but not yet uploaded as part of an S3 part. */
    List<RecordingChunk> findByRecordingIdAndPartNumberIsNullOrderBySequenceAsc(String recordingId);

    @Query("select coalesce(sum(c.sizeBytes), 0) from RecordingChunk c "
            + "where c.recordingId = :recordingId and c.partNumber is null")
    long pendingBytes(@Param("recordingId") String recordingId);

    @Query("select c.sha256 from RecordingChunk c where c.recordingId = :recordingId "
            + "order by c.sequence asc")
    List<String> chunkHashesInOrder(@Param("recordingId") String recordingId);

    @Query("select coalesce(sum(c.sizeBytes), 0) from RecordingChunk c "
            + "where c.recordingId = :recordingId")
    long totalBytes(@Param("recordingId") String recordingId);
}
