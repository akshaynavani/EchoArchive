package com.echoarchive.metadata;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.JpaSpecificationExecutor;

public interface MediaRecordRepository extends JpaRepository<MediaRecord, String>,
        JpaSpecificationExecutor<MediaRecord> {
}
