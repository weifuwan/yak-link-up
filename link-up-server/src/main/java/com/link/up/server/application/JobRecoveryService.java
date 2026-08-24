package com.link.up.server.application;

import com.link.up.server.application.port.JobRepository;
import com.link.up.server.application.port.JobRepositoryEntry;
import com.link.up.server.runtime.JobExecutionMetadata;
import com.link.up.server.runtime.JobRecoverySnapshotFactory;
import com.link.up.server.runtime.JobSnapshot;

import java.util.List;
import java.util.Objects;

/** Restores persisted idempotency state and marks unfinished jobs as LOST. */
final class JobRecoveryService {

    private static final String RESTART_LOST_REASON =
            "Worker restarted before the job reached a terminal state";

    private final JobRepository repository;
    private final JobSubmissionRegistry submissionRegistry;

    JobRecoveryService(
            JobRepository repository,
            JobSubmissionRegistry submissionRegistry) {

        this.repository = Objects.requireNonNull(
                repository,
                "repository must not be null");
        this.submissionRegistry = Objects.requireNonNull(
                submissionRegistry,
                "submissionRegistry must not be null");
    }

    void recover() {
        List<JobRepositoryEntry> persisted = repository.listEntries();

        for (JobRepositoryEntry entry : persisted) {
            recover(entry);
        }
    }

    private void recover(JobRepositoryEntry entry) {
        JobSnapshot snapshot = entry.getSnapshot();
        JobExecutionMetadata metadata = entry.getMetadata();

        if (metadata != null) {
            submissionRegistry.restore(
                    snapshot.getJobId(),
                    metadata);
        }

        if (snapshot.getStatus().isTerminal()) {
            return;
        }

        long recoveryTimeMillis = System.currentTimeMillis();

        JobSnapshot lost =
                JobRecoverySnapshotFactory.recoverLost(
                        snapshot,
                        recoveryTimeMillis,
                        RESTART_LOST_REASON);

        JobExecutionMetadata recoveredMetadata =
                metadata == null
                        ? null
                        : metadata.recoverLost(
                                snapshot.getStatus(),
                                recoveryTimeMillis,
                                RESTART_LOST_REASON);

        repository.save(
                lost,
                recoveredMetadata);
    }
}
