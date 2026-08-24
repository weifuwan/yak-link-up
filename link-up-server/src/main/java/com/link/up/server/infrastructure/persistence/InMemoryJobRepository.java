package com.link.up.server.infrastructure.persistence;

import com.link.up.server.application.port.JobRepository;
import com.link.up.server.runtime.JobExecutionMetadata;
import com.link.up.server.runtime.JobSnapshot;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentMap;

/**
 * Bounded in-memory terminal history adapter.
 */
public final class InMemoryJobRepository
        implements JobRepository {

    private final int historyLimit;
    private final ConcurrentMap<String, JobSnapshot> snapshots =
            new ConcurrentHashMap<String, JobSnapshot>();
    private final ConcurrentMap<String, JobExecutionMetadata> metadata =
            new ConcurrentHashMap<String, JobExecutionMetadata>();
    private final ConcurrentLinkedDeque<String> orderedJobIds =
            new ConcurrentLinkedDeque<String>();

    public InMemoryJobRepository(int historyLimit) {
        if (historyLimit <= 0) {
            throw new IllegalArgumentException(
                    "historyLimit must be greater than 0");
        }
        this.historyLimit = historyLimit;
    }

    @Override
    public void save(JobSnapshot snapshot) {
        save(snapshot, null);
    }

    @Override
    public void save(
            JobSnapshot snapshot,
            JobExecutionMetadata executionMetadata) {

        JobSnapshot previous =
                snapshots.put(
                        snapshot.getJobId(),
                        snapshot);

        if (executionMetadata != null) {
            metadata.put(
                    snapshot.getJobId(),
                    executionMetadata);
        }

        if (previous == null) {
            orderedJobIds.addFirst(
                    snapshot.getJobId());
        }

        trim();
    }

    @Override
    public JobSnapshot get(String jobId) {
        return snapshots.get(jobId);
    }

    @Override
    public JobExecutionMetadata getMetadata(String jobId) {
        return metadata.get(jobId);
    }

    @Override
    public List<JobSnapshot> list() {
        List<JobSnapshot> result =
                new ArrayList<JobSnapshot>();

        for (String jobId : orderedJobIds) {
            JobSnapshot snapshot = snapshots.get(jobId);
            if (snapshot != null) {
                result.add(snapshot);
            }
        }

        Collections.sort(
                result,
                new Comparator<JobSnapshot>() {
                    @Override
                    public int compare(
                            JobSnapshot left,
                            JobSnapshot right) {
                        return Long.compare(
                                right.getCreateTimeMillis(),
                                left.getCreateTimeMillis());
                    }
                });

        return result;
    }

    private void trim() {
        while (snapshots.size() > historyLimit) {
            String oldestJobId = orderedJobIds.pollLast();
            if (oldestJobId == null) {
                return;
            }
            snapshots.remove(oldestJobId);
            metadata.remove(oldestJobId);
        }
    }
}
