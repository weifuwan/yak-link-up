package com.link.up.server.application.port;

import com.link.up.server.runtime.JobExecutionMetadata;
import com.link.up.server.runtime.JobSnapshot;

import java.util.ArrayList;
import java.util.List;

/**
 * Application persistence port for Worker checkpoints and terminal history.
 *
 * <p>Save is an upsert: non-terminal checkpoints and terminal snapshots share
 * the same job identity. Implementations may persist a reduced recovery view
 * as long as lifecycle/idempotency/attempt information survives restart.</p>
 */
public interface JobRepository {

    void save(JobSnapshot snapshot);

    default void save(
            JobSnapshot snapshot,
            JobExecutionMetadata metadata) {
        save(snapshot);
    }

    JobSnapshot get(String jobId);

    default JobExecutionMetadata getMetadata(
            String jobId) {
        return null;
    }

    List<JobSnapshot> list();

    default List<JobRepositoryEntry> listEntries() {
        List<JobRepositoryEntry> result =
                new ArrayList<JobRepositoryEntry>();
        for (JobSnapshot snapshot : list()) {
            result.add(
                    new JobRepositoryEntry(
                            snapshot,
                            getMetadata(snapshot.getJobId())));
        }
        return result;
    }

    default void delete(String jobId) {
        // Optional for legacy adapters. Durable/checkpoint adapters override.
    }
}
