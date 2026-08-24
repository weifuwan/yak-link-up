package com.link.up.server.application;

import com.link.up.framework.job.JobDefinition;
import com.link.up.server.domain.JobSubmission;
import com.link.up.server.runtime.JobExecutionMetadata;
import com.link.up.server.runtime.JobSnapshot;

import java.util.List;

/**
 * Use-case boundary exposed to HTTP/registration adapters.
 */
public interface JobApplication
        extends AutoCloseable {

    JobSnapshot submit(JobDefinition definition);

    JobSnapshot submit(JobSubmission submission);

    JobSnapshot getJob(String jobId);

    JobSnapshot getJobByExternalExecutionId(String externalExecutionId);

    JobExecutionMetadata getMetadata(String jobId);

    List<JobSnapshot> listJobs();

    JobSnapshot cancel(String jobId);

    int getRunningJobCount();

    int getQueuedJobCount();

    int getActiveJobCount();

    boolean isClosed();

    @Override
    void close();
}
