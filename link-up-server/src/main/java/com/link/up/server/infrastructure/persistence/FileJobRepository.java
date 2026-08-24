package com.link.up.server.infrastructure.persistence;

import com.link.up.server.application.port.JobRepository;
import com.link.up.server.application.port.JobRepositoryEntry;
import com.link.up.server.runtime.JobExecutionMetadata;
import com.link.up.server.runtime.JobSnapshot;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentMap;

/**
 * Durable Worker checkpoint repository backed by versioned per-job JSON files.
 *
 * <p>The repository owns the in-memory index, checkpoint revision ordering and
 * terminal-history retention. File naming, JSON IO and atomic replacement
 * belong to {@link JobStateFileStore}.</p>
 */
public final class FileJobRepository
        implements JobRepository {

    private final JobStateFileStore fileStore;
    private final int historyLimit;
    private final ConcurrentMap<String, JobSnapshot> snapshots =
            new ConcurrentHashMap<String, JobSnapshot>();
    private final ConcurrentMap<String, JobExecutionMetadata> metadata =
            new ConcurrentHashMap<String, JobExecutionMetadata>();
    private final ConcurrentLinkedDeque<String> orderedJobIds =
            new ConcurrentLinkedDeque<String>();

    public FileJobRepository(
            Path stateDirectory,
            int historyLimit) {

        if (stateDirectory == null) {
            throw new IllegalArgumentException(
                    "stateDirectory must not be null");
        }

        if (historyLimit <= 0) {
            throw new IllegalArgumentException(
                    "historyLimit must be greater than 0");
        }

        this.fileStore =
                new JobStateFileStore(stateDirectory);
        this.historyLimit = historyLimit;

        initialize();
    }

    @Override
    public synchronized void save(JobSnapshot snapshot) {
        save(snapshot, null);
    }

    @Override
    public synchronized void save(
            JobSnapshot snapshot,
            JobExecutionMetadata executionMetadata) {

        if (snapshot == null) {
            throw new IllegalArgumentException(
                    "snapshot must not be null");
        }

        JobExecutionMetadata current =
                metadata.get(snapshot.getJobId());

        if (isStale(
                current,
                executionMetadata)) {
            return;
        }

        remember(
                snapshot,
                executionMetadata);
        persist(snapshot.getJobId());
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
        List<JobSnapshot> result =
                new ArrayList<JobSnapshot>();

        for (String jobId : orderedJobIds) {
            JobSnapshot snapshot = snapshots.get(jobId);

            if (snapshot != null) {
                result.add(snapshot);
            }
        }

        sortNewestFirst(result);
        return result;
    }

    @Override
    public synchronized void delete(String jobId) {
        if (jobId == null) {
            return;
        }

        snapshots.remove(jobId);
        metadata.remove(jobId);
        orderedJobIds.remove(jobId);

        try {
            fileStore.delete(jobId);
        } catch (IOException failure) {
            throw persistenceFailure(
                    "Could not delete Worker checkpoint for "
                            + jobId,
                    failure);
        }
    }

    public Path getStateDirectory() {
        return fileStore.getStateDirectory();
    }

    private void initialize() {
        try {
            fileStore.initialize();
            loadExisting();
            trimTerminalHistory();
        } catch (IOException failure) {
            throw persistenceFailure(
                    "Could not initialize Worker state directory "
                            + fileStore.getStateDirectory(),
                    failure);
        }
    }

    private void loadExisting()
            throws IOException {

        List<JobRepositoryEntry> loaded =
                fileStore.loadEntries();

        Collections.sort(
                loaded,
                new Comparator<JobRepositoryEntry>() {
                    @Override
                    public int compare(
                            JobRepositoryEntry left,
                            JobRepositoryEntry right) {

                        return Long.compare(
                                right.getSnapshot()
                                        .getCreateTimeMillis(),
                                left.getSnapshot()
                                        .getCreateTimeMillis());
                    }
                });

        for (JobRepositoryEntry entry : loaded) {
            JobSnapshot snapshot = entry.getSnapshot();
            String jobId = snapshot.getJobId();

            snapshots.put(
                    jobId,
                    snapshot);

            if (entry.getMetadata() != null) {
                metadata.put(
                        jobId,
                        entry.getMetadata());
            }

            orderedJobIds.addLast(jobId);
        }
    }

    private void remember(
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
    }

    private void persist(String jobId) {
        StoredJobRecord record =
                StoredJobRecord.from(
                        snapshots.get(jobId),
                        metadata.get(jobId));

        try {
            fileStore.write(record);
        } catch (IOException failure) {
            throw persistenceFailure(
                    "Could not persist Worker checkpoint for "
                            + jobId,
                    failure);
        }
    }

    private void trimTerminalHistory() {
        while (terminalCount() > historyLimit) {
            String oldestTerminal =
                    oldestTerminalJobId();

            if (oldestTerminal == null) {
                return;
            }

            delete(oldestTerminal);
        }
    }

    private String oldestTerminalJobId() {
        Iterator<String> iterator =
                orderedJobIds.descendingIterator();

        while (iterator.hasNext()) {
            String jobId = iterator.next();
            JobSnapshot snapshot = snapshots.get(jobId);

            if (snapshot != null
                    && snapshot.getStatus().isTerminal()) {
                return jobId;
            }
        }

        return null;
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

    private static boolean isStale(
            JobExecutionMetadata current,
            JobExecutionMetadata candidate) {

        return current != null
                && candidate != null
                && candidate.getCheckpointVersion()
                < current.getCheckpointVersion();
    }

    private static void sortNewestFirst(
            List<JobSnapshot> snapshots) {

        Collections.sort(
                snapshots,
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
    }

    private static IllegalStateException persistenceFailure(
            String message,
            Exception failure) {

        return new IllegalStateException(
                message,
                failure);
    }
}
