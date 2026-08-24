package com.link.up.server.application;

import com.link.up.server.domain.JobExecutionState;
import com.link.up.server.runtime.ServerJobStatus;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/** Owns the in-process index of active job execution states. */
final class ActiveJobRegistry {

    private final ConcurrentMap<String, JobExecutionState> jobs =
            new ConcurrentHashMap<String, JobExecutionState>();

    JobExecutionState putIfAbsent(JobExecutionState state) {
        return jobs.putIfAbsent(
                state.getJobId(),
                state);
    }

    JobExecutionState get(String jobId) {
        return jobs.get(jobId);
    }

    boolean contains(String jobId) {
        return jobs.containsKey(jobId);
    }

    boolean remove(
            String jobId,
            JobExecutionState state) {
        return jobs.remove(jobId, state);
    }

    List<JobExecutionState> snapshot() {
        return new ArrayList<JobExecutionState>(jobs.values());
    }

    int size() {
        return jobs.size();
    }

    int count(ServerJobStatus status) {
        int count = 0;

        for (JobExecutionState state : jobs.values()) {
            if (state.getStatus() == status) {
                count++;
            }
        }

        return count;
    }

    int countQueued() {
        int count = 0;

        for (JobExecutionState state : jobs.values()) {
            ServerJobStatus status = state.getStatus();

            if (status == ServerJobStatus.CREATED
                    || status == ServerJobStatus.SUBMITTED
                    || status == ServerJobStatus.QUEUED) {
                count++;
            }
        }

        return count;
    }
}
