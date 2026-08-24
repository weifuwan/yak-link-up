package com.link.up.server.infrastructure.persistence;

import com.link.up.server.application.port.JobRepository;
import com.link.up.server.runtime.JobExecutionMetadata;
import com.link.up.server.runtime.JobSnapshot;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentMap;

/** Bounded in-memory checkpoint/history adapter used by tests and embeddings. */
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
    public synchronized void save(
            JobSnapshot snapshot,
            JobExecutionMetadata executionMetadata) {

        JobExecutionMetadata current =
                metadata.get(snapshot.getJobId());
        if (isStale(current, executionMetadata)) {
            return;
        }

        JobSnapshot previous = snapshots.put(
                snapshot.getJobId(),
                snapshot);
        if (executionMetadata != null) {
            metadata.put(snapshot.getJobId(), executionMetadata);
        }
        if (previous == null) {
            orderedJobIds.addFirst(snapshot.getJobId());
        }
        trimTerminalHistory();
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
        List<JobSnapshot> result = new ArrayList<JobSnapshot>();
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

    @Override
    public synchronized void delete(String jobId) {
        snapshots.remove(jobId);
        metadata.remove(jobId);
        orderedJobIds.remove(jobId);
    }

    private static boolean isStale(
            JobExecutionMetadata current,
            JobExecutionMetadata candidate) {
        return current != null
                && candidate != null
                && candidate.getCheckpointVersion()
                < current.getCheckpointVersion();
    }

    private void trimTerminalHistory() {
        while (terminalCount() > historyLimit) {
            String oldestTerminal = null;
            Iterator<String> iterator = orderedJobIds.descendingIterator();
            while (iterator.hasNext()) {
                String jobId = iterator.next();
                JobSnapshot snapshot = snapshots.get(jobId);
                if (snapshot != null && snapshot.getStatus().isTerminal()) {
                    oldestTerminal = jobId;
                    break;
                }
            }
            if (oldestTerminal == null) {
                return;
            }
            delete(oldestTerminal);
        }
    }

    private int terminalCount() {
        int count = 0;
        for (JobSnapshot snapshot : snapshots.values()) {
            if (snapshot.getStatus().isTerminal()) {
                count++;
            }
        }
        return count;
    }
}
