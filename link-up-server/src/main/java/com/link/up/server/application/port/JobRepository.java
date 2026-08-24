package com.link.up.server.application.port;

import com.link.up.server.runtime.JobExecutionMetadata;
import com.link.up.server.runtime.JobSnapshot;

import java.util.List;

/**
 * Application persistence port for terminal Worker read models.
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
}
