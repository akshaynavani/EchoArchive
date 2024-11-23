package com.echoarchive.retrieval;

import com.echoarchive.domain.Priority;
import com.echoarchive.metadata.MediaRecord;
import com.echoarchive.retrieval.dto.MediaQuery;
import org.springframework.data.jpa.domain.Specification;

import java.util.Arrays;
import java.util.List;

final class MediaSpecifications {

    private MediaSpecifications() {
    }

    static Specification<MediaRecord> matching(MediaQuery q) {
        Specification<MediaRecord> spec = Specification.where(null);
        if (q.agency() != null) {
            spec = spec.and((root, cq, cb) -> cb.equal(root.get("agency"), q.agency()));
        }
        if (q.incidentId() != null) {
            spec = spec.and((root, cq, cb) -> cb.equal(root.get("incidentId"), q.incidentId()));
        }
        if (q.senderId() != null) {
            spec = spec.and((root, cq, cb) -> cb.equal(root.get("senderId"), q.senderId()));
        }
        if (q.kind() != null) {
            spec = spec.and((root, cq, cb) -> cb.equal(root.get("kind"), q.kind()));
        }
        if (q.minPriority() != null) {
            List<Priority> allowed = Arrays.stream(Priority.values())
                    .filter(p -> p.ordinal() >= q.minPriority().ordinal())
                    .toList();
            spec = spec.and((root, cq, cb) -> root.get("priority").in(allowed));
        }
        if (q.from() != null) {
            spec = spec.and((root, cq, cb) -> cb.greaterThanOrEqualTo(root.get("capturedAt"), q.from()));
        }
        if (q.to() != null) {
            spec = spec.and((root, cq, cb) -> cb.lessThan(root.get("capturedAt"), q.to()));
        }
        return spec;
    }
}
