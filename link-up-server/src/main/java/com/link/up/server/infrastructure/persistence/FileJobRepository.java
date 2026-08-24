package com.link.up.server.infrastructure.persistence;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.link.up.server.application.port.JobRepository;
import com.link.up.server.application.port.JobRepositoryEntry;
import com.link.up.server.domain.JobAttemptStatus;
import com.link.up.server.runtime.JobAttemptMetadata;
import com.link.up.server.runtime.JobExecutionMetadata;
import com.link.up.server.runtime.JobRecoverySnapshotFactory;
import com.link.up.server.runtime.JobSnapshot;
import com.link.up.server.runtime.JobStateTransition;
import com.link.up.server.runtime.ServerJobStatus;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentMap;

/** Local durable Worker checkpoint repository using versioned per-job JSON files. */
public final class FileJobRepository
        implements JobRepository {

    private static final int FORMAT_VERSION = 1;
    private static final String FILE_SUFFIX = ".job.json";

    private final Path stateDirectory;
    private final int historyLimit;
    private final ObjectMapper mapper = new ObjectMapper();
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

        this.stateDirectory = stateDirectory.toAbsolutePath().normalize();
        this.historyLimit = historyLimit;

        try {
            Files.createDirectories(this.stateDirectory);
            loadExisting();
            trimTerminalHistory();
        } catch (IOException failure) {
            throw persistenceFailure(
                    "Could not initialize Worker state directory "
                            + this.stateDirectory,
                    failure);
        }
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

        writeAtomically(
                StoredJobRecord.from(
                        snapshot,
                        metadata.get(snapshot.getJobId())));
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
        if (jobId == null) {
            return;
        }
        snapshots.remove(jobId);
        metadata.remove(jobId);
        orderedJobIds.remove(jobId);
        try {
            Files.deleteIfExists(fileFor(jobId));
        } catch (IOException failure) {
            throw persistenceFailure(
                    "Could not delete Worker checkpoint for " + jobId,
                    failure);
        }
    }

    public Path getStateDirectory() {
        return stateDirectory;
    }

    private static boolean isStale(
            JobExecutionMetadata current,
            JobExecutionMetadata candidate) {
        return current != null
                && candidate != null
                && candidate.getCheckpointVersion()
                < current.getCheckpointVersion();
    }

    private void loadExisting() throws IOException {
        List<JobRepositoryEntry> loaded = new ArrayList<JobRepositoryEntry>();

        try (DirectoryStream<Path> stream =
                     Files.newDirectoryStream(
                             stateDirectory,
                             "*" + FILE_SUFFIX)) {
            for (Path file : stream) {
                StoredJobRecord record;
                try {
                    record = mapper.readValue(
                            file.toFile(),
                            StoredJobRecord.class);
                } catch (Exception failure) {
                    throw new IOException(
                            "Invalid Worker state file: " + file,
                            failure);
                }
                if (record.formatVersion != FORMAT_VERSION) {
                    throw new IOException(
                            "Unsupported Worker state formatVersion="
                                    + record.formatVersion
                                    + " in "
                                    + file);
                }
                loaded.add(record.toEntry());
            }
        }

        Collections.sort(
                loaded,
                new Comparator<JobRepositoryEntry>() {
                    @Override
                    public int compare(
                            JobRepositoryEntry left,
                            JobRepositoryEntry right) {
                        return Long.compare(
                                right.getSnapshot().getCreateTimeMillis(),
                                left.getSnapshot().getCreateTimeMillis());
                    }
                });

        for (JobRepositoryEntry entry : loaded) {
            String jobId = entry.getSnapshot().getJobId();
            snapshots.put(jobId, entry.getSnapshot());
            if (entry.getMetadata() != null) {
                metadata.put(jobId, entry.getMetadata());
            }
            orderedJobIds.addLast(jobId);
        }
    }

    private void writeAtomically(StoredJobRecord record) {
        Path target = fileFor(record.jobId);
        Path temporary = null;
        try {
            byte[] bytes = mapper.writerWithDefaultPrettyPrinter()
                    .writeValueAsBytes(record);
            temporary = Files.createTempFile(
                    stateDirectory,
                    ".job-state-",
                    ".tmp");

            try (FileOutputStream output =
                         new FileOutputStream(temporary.toFile())) {
                output.write(bytes);
                output.flush();
                output.getFD().sync();
            }

            try {
                Files.move(
                        temporary,
                        target,
                        StandardCopyOption.REPLACE_EXISTING,
                        StandardCopyOption.ATOMIC_MOVE);
            } catch (AtomicMoveNotSupportedException unsupported) {
                Files.move(
                        temporary,
                        target,
                        StandardCopyOption.REPLACE_EXISTING);
            }
        } catch (IOException failure) {
            throw persistenceFailure(
                    "Could not persist Worker checkpoint for " + record.jobId,
                    failure);
        } finally {
            if (temporary != null) {
                try {
                    Files.deleteIfExists(temporary);
                } catch (IOException ignored) {
                    // Best-effort cleanup.
                }
            }
        }
    }

    private Path fileFor(String jobId) {
        String encoded = Base64.getUrlEncoder()
                .withoutPadding()
                .encodeToString(
                        jobId.getBytes(StandardCharsets.UTF_8));
        return stateDirectory.resolve(encoded + FILE_SUFFIX);
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

    private static IllegalStateException persistenceFailure(
            String message,
            Exception failure) {
        return new IllegalStateException(message, failure);
    }

    public static final class StoredJobRecord {
        public int formatVersion;
        public String jobId;
        public String jobName;
        public String status;
        public long createTimeMillis;
        public long startTimeMillis;
        public long endTimeMillis;
        public String errorCode;
        public String errorMessage;
        public StoredMetadata metadata;

        public StoredJobRecord() {
        }

        static StoredJobRecord from(
                JobSnapshot snapshot,
                JobExecutionMetadata metadata) {
            StoredJobRecord record = new StoredJobRecord();
            record.formatVersion = FORMAT_VERSION;
            record.jobId = snapshot.getJobId();
            record.jobName = snapshot.getJobName();
            record.status = snapshot.getStatus().name();
            record.createTimeMillis = snapshot.getCreateTimeMillis();
            record.startTimeMillis = snapshot.getStartTimeMillis();
            record.endTimeMillis = snapshot.getEndTimeMillis();
            record.errorCode = snapshot.getErrorCode();
            record.errorMessage = snapshot.getErrorMessage();
            record.metadata = StoredMetadata.from(metadata);
            return record;
        }

        JobRepositoryEntry toEntry() {
            JobSnapshot restoredSnapshot =
                    JobRecoverySnapshotFactory.restoreBasic(
                            jobId,
                            jobName,
                            ServerJobStatus.valueOf(status),
                            createTimeMillis,
                            startTimeMillis,
                            endTimeMillis,
                            errorCode,
                            errorMessage);
            return new JobRepositoryEntry(
                    restoredSnapshot,
                    metadata == null ? null : metadata.toMetadata());
        }
    }

    public static final class StoredMetadata {
        public String externalExecutionId;
        public String idempotencyKey;
        public int definitionVersion;
        public String configDigest;
        public long submittedTimeMillis;
        public long queuedTimeMillis;
        public long stateVersion;
        public long checkpointVersion;
        public boolean cancellationRequested;
        public String runId;
        public String jobLogFile;
        public List<StoredTransition> transitions =
                new ArrayList<StoredTransition>();
        public List<StoredAttempt> attempts =
                new ArrayList<StoredAttempt>();

        public StoredMetadata() {
        }

        static StoredMetadata from(JobExecutionMetadata metadata) {
            if (metadata == null) {
                return null;
            }
            StoredMetadata stored = new StoredMetadata();
            stored.externalExecutionId = metadata.getExternalExecutionId();
            stored.idempotencyKey = metadata.getIdempotencyKey();
            stored.definitionVersion = metadata.getDefinitionVersion();
            stored.configDigest = metadata.getConfigDigest();
            stored.submittedTimeMillis = metadata.getSubmittedTimeMillis();
            stored.queuedTimeMillis = metadata.getQueuedTimeMillis();
            stored.stateVersion = metadata.getStateVersion();
            stored.checkpointVersion = metadata.getCheckpointVersion();
            stored.cancellationRequested = metadata.isCancellationRequested();
            stored.runId = metadata.getRunId();
            stored.jobLogFile = metadata.getJobLogFile();
            for (JobStateTransition transition : metadata.getTransitions()) {
                stored.transitions.add(StoredTransition.from(transition));
            }
            for (JobAttemptMetadata attempt : metadata.getAttempts()) {
                stored.attempts.add(StoredAttempt.from(attempt));
            }
            return stored;
        }

        JobExecutionMetadata toMetadata() {
            List<JobStateTransition> restoredTransitions =
                    new ArrayList<JobStateTransition>();
            for (StoredTransition transition : transitions) {
                restoredTransitions.add(transition.toTransition());
            }
            List<JobAttemptMetadata> restoredAttempts =
                    new ArrayList<JobAttemptMetadata>();
            for (StoredAttempt attempt : attempts) {
                restoredAttempts.add(attempt.toAttempt());
            }
            return new JobExecutionMetadata(
                    externalExecutionId,
                    idempotencyKey,
                    definitionVersion,
                    configDigest,
                    submittedTimeMillis,
                    queuedTimeMillis,
                    stateVersion,
                    checkpointVersion,
                    cancellationRequested,
                    restoredTransitions,
                    Collections.<String, com.link.up.api.sink.TableDdl>emptyMap(),
                    runId,
                    jobLogFile,
                    restoredAttempts);
        }
    }

    public static final class StoredTransition {
        public long version;
        public String fromStatus;
        public String toStatus;
        public long transitionTimeMillis;
        public String reason;

        public StoredTransition() {
        }

        static StoredTransition from(JobStateTransition transition) {
            StoredTransition stored = new StoredTransition();
            stored.version = transition.getVersion();
            stored.fromStatus = transition.getFromStatus() == null
                    ? null
                    : transition.getFromStatus().name();
            stored.toStatus = transition.getToStatus().name();
            stored.transitionTimeMillis = transition.getTransitionTimeMillis();
            stored.reason = transition.getReason();
            return stored;
        }

        JobStateTransition toTransition() {
            return new JobStateTransition(
                    version,
                    fromStatus == null
                            ? null
                            : ServerJobStatus.valueOf(fromStatus),
                    ServerJobStatus.valueOf(toStatus),
                    transitionTimeMillis,
                    reason);
        }
    }

    public static final class StoredAttempt {
        public int attemptNumber;
        public String attemptId;
        public String status;
        public long createTimeMillis;
        public long queuedTimeMillis;
        public long startTimeMillis;
        public long endTimeMillis;
        public String runId;
        public String jobLogFile;
        public String failureType;
        public String failureMessage;
        public String retryAdvice;

        public StoredAttempt() {
        }

        static StoredAttempt from(JobAttemptMetadata attempt) {
            StoredAttempt stored = new StoredAttempt();
            stored.attemptNumber = attempt.getAttemptNumber();
            stored.attemptId = attempt.getAttemptId();
            stored.status = attempt.getStatus().name();
            stored.createTimeMillis = attempt.getCreateTimeMillis();
            stored.queuedTimeMillis = attempt.getQueuedTimeMillis();
            stored.startTimeMillis = attempt.getStartTimeMillis();
            stored.endTimeMillis = attempt.getEndTimeMillis();
            stored.runId = attempt.getRunId();
            stored.jobLogFile = attempt.getJobLogFile();
            stored.failureType = attempt.getFailureType();
            stored.failureMessage = attempt.getFailureMessage();
            stored.retryAdvice = attempt.getRetryAdvice();
            return stored;
        }

        JobAttemptMetadata toAttempt() {
            return new JobAttemptMetadata(
                    attemptNumber,
                    attemptId,
                    JobAttemptStatus.valueOf(status),
                    createTimeMillis,
                    queuedTimeMillis,
                    startTimeMillis,
                    endTimeMillis,
                    runId,
                    jobLogFile,
                    failureType,
                    failureMessage,
                    retryAdvice);
        }
    }
}
