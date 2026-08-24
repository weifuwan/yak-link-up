package com.link.up.server.application.port;

import com.link.up.server.runtime.JobExecutionMetadata;
import com.link.up.server.runtime.JobSnapshot;

import java.util.Objects;

/** Immutable persisted Worker job record returned through the repository port. */
public final class JobRepositoryEntry {

    private final JobSnapshot snapshot;
    private final JobExecutionMetadata metadata;

    public JobRepositoryEntry(
            JobSnapshot snapshot,
            JobExecutionMetadata metadata) {
        this.snapshot = Objects.requireNonNull(
                snapshot,
                "snapshot must not be null");
        this.metadata = metadata;
    }

    public JobSnapshot getSnapshot() {
        return snapshot;
    }

    public JobExecutionMetadata getMetadata() {
        return metadata;
    }
}
