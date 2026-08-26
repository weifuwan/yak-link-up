package com.link.up.server.infrastructure.event;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.link.up.server.application.port.JobEventHistoryException;
import com.link.up.server.application.port.JobEventListener;
import com.link.up.server.application.port.JobEventReader;
import com.link.up.server.application.port.JobEventRetention;
import com.link.up.server.runtime.event.JobEventEnvelope;
import com.link.up.server.runtime.event.JobEventPage;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Per-Job append-only JSONL event journal.
 *
 * <p>One event occupies one UTF-8 line. A sequence cursor is exclusive: reading
 * after sequence 4 starts from event 5. The store is intentionally local and
 * synchronous for the first phase; dispatch failure isolation belongs to
 * {@code JobEventBus}.</p>
 */
public final class JsonLineJobEventStore
        implements JobEventListener, JobEventReader, JobEventRetention {

    private static final Logger LOG =
            LogManager.getLogger(JsonLineJobEventStore.class);

    private static final int MAX_PAGE_SIZE = 1_000;
    private static final String EVENT_DIRECTORY = "job-events";
    private static final String EVENT_FILE_SUFFIX = ".jsonl";
    private static final String SAFE_JOB_ID = "[A-Za-z0-9._-]{1,200}";

    private final Path eventDirectory;
    private final ObjectMapper mapper;
    private final Object[] jobLocks = createLocks();
    private final ConcurrentMap<String, Long> lastSequences =
            new ConcurrentHashMap<String, Long>();

    public JsonLineJobEventStore(Path stateDirectory) {
        Objects.requireNonNull(
                stateDirectory,
                "stateDirectory must not be null");

        this.eventDirectory =
                stateDirectory.resolve(EVENT_DIRECTORY);
        this.mapper = new ObjectMapper();

        try {
            Files.createDirectories(eventDirectory);
        } catch (IOException failure) {
            throw historyFailure(
                    "Could not initialize Job event directory "
                            + eventDirectory,
                    failure);
        }
    }

    @Override
    public void onEvent(JobEventEnvelope event) {
        append(
                Objects.requireNonNull(
                        event,
                        "event must not be null"));
    }

    @Override
    public void delete(String jobId) {
        String normalizedJobId = requireJobId(jobId);

        synchronized (lockFor(normalizedJobId)) {
            try {
                Files.deleteIfExists(
                        eventFile(normalizedJobId));
                lastSequences.remove(normalizedJobId);
            } catch (IOException failure) {
                throw historyFailure(
                        "Could not delete Job event history for "
                                + normalizedJobId,
                        failure);
            }
        }
    }

    @Override
    public void retain(Iterable<String> jobIds) {
        Objects.requireNonNull(
                jobIds,
                "jobIds must not be null");

        Set<String> retained = new HashSet<String>();
        for (String jobId : jobIds) {
            retained.add(requireJobId(jobId));
        }

        try (java.nio.file.DirectoryStream<Path> files =
                     Files.newDirectoryStream(
                             eventDirectory,
                             "*" + EVENT_FILE_SUFFIX)) {

            for (Path file : files) {
                String fileName =
                        file.getFileName().toString();
                String jobId = fileName.substring(
                        0,
                        fileName.length()
                                - EVENT_FILE_SUFFIX.length());

                if (!retained.contains(jobId)) {
                    delete(jobId);
                }
            }
        } catch (IOException failure) {
            throw historyFailure(
                    "Could not apply Job event history retention",
                    failure);
        }
    }

    @Override
    public JobEventPage read(
            String jobId,
            long afterSequence,
            int limit) {

        String normalizedJobId =
                requireJobId(jobId);
        validatePage(afterSequence, limit);

        synchronized (lockFor(normalizedJobId)) {
            return readLocked(
                    normalizedJobId,
                    afterSequence,
                    limit);
        }
    }

    public Path getEventDirectory() {
        return eventDirectory;
    }

    private void append(JobEventEnvelope event) {
        String jobId = requireJobId(event.getJobId());

        synchronized (lockFor(jobId)) {
            long previous = resolveLastSequence(jobId);

            if (event.getSequence() <= previous) {
                LOG.debug(
                        "Ignoring duplicate or stale Job event, jobId={}, sequence={}, lastSequence={}",
                        jobId,
                        event.getSequence(),
                        previous);
                return;
            }

            if (event.getSequence() > previous + 1L) {
                LOG.warn(
                        "Job event sequence gap detected, jobId={}, expectedSequence={}, actualSequence={}",
                        jobId,
                        previous + 1L,
                        event.getSequence());
            }

            Path file = eventFile(jobId);
            byte[] line = serialize(event);

            try {
                Files.write(
                        file,
                        line,
                        StandardOpenOption.CREATE,
                        StandardOpenOption.WRITE,
                        StandardOpenOption.APPEND);
                lastSequences.put(
                        jobId,
                        event.getSequence());
            } catch (IOException failure) {
                throw historyFailure(
                        "Could not append Job event for "
                                + jobId,
                        failure);
            }
        }
    }

    private JobEventPage readLocked(
            String jobId,
            long afterSequence,
            int limit) {

        Path file = eventFile(jobId);
        if (!Files.exists(file)) {
            return JobEventPage.empty(
                    jobId,
                    afterSequence);
        }

        List<JobEventEnvelope> items =
                new ArrayList<JobEventEnvelope>(limit);
        boolean hasMore = false;
        long previousSequence = 0L;
        long nextSequence = afterSequence;

        try (BufferedReader reader =
                     Files.newBufferedReader(
                             file,
                             StandardCharsets.UTF_8)) {

            String line;
            int lineNumber = 0;

            while ((line = reader.readLine()) != null) {
                lineNumber++;
                if (line.trim().isEmpty()) {
                    continue;
                }

                JobEventEnvelope event =
                        parse(
                                jobId,
                                line,
                                lineNumber);

                if (event.getSequence() <= previousSequence) {
                    throw historyFailure(
                            "Job event sequence is not strictly increasing for "
                                    + jobId
                                    + " at line "
                                    + lineNumber,
                            null);
                }

                previousSequence = event.getSequence();

                if (event.getSequence() <= afterSequence) {
                    continue;
                }

                if (items.size() == limit) {
                    hasMore = true;
                    break;
                }

                items.add(event);
                nextSequence = event.getSequence();
            }
        } catch (IOException failure) {
            throw historyFailure(
                    "Could not read Job event history for "
                            + jobId,
                    failure);
        }

        return new JobEventPage(
                jobId,
                items,
                nextSequence,
                hasMore);
    }

    private long resolveLastSequence(String jobId) {
        Long cached = lastSequences.get(jobId);
        if (cached != null) {
            return cached.longValue();
        }

        Path file = eventFile(jobId);
        if (!Files.exists(file)) {
            lastSequences.putIfAbsent(jobId, 0L);
            return 0L;
        }

        long last = 0L;

        try (BufferedReader reader =
                     Files.newBufferedReader(
                             file,
                             StandardCharsets.UTF_8)) {

            String line;
            int lineNumber = 0;

            while ((line = reader.readLine()) != null) {
                lineNumber++;
                if (line.trim().isEmpty()) {
                    continue;
                }

                JobEventEnvelope event =
                        parse(
                                jobId,
                                line,
                                lineNumber);

                if (event.getSequence() <= last) {
                    throw historyFailure(
                            "Job event sequence is not strictly increasing for "
                                    + jobId
                                    + " at line "
                                    + lineNumber,
                            null);
                }

                last = event.getSequence();
            }
        } catch (IOException failure) {
            throw historyFailure(
                    "Could not scan Job event history for "
                            + jobId,
                    failure);
        }

        Long previous = lastSequences.putIfAbsent(jobId, last);
        return previous == null
                ? last
                : previous.longValue();
    }

    private JobEventEnvelope parse(
            String jobId,
            String line,
            int lineNumber) {

        try {
            JobEventEnvelope event =
                    mapper.readValue(
                            line,
                            JobEventEnvelope.class);

            if (event.getSchemaVersion()
                    > JobEventEnvelope.CURRENT_SCHEMA_VERSION) {
                throw historyFailure(
                        "Unsupported Job event schema version "
                                + event.getSchemaVersion()
                                + " for "
                                + jobId
                                + " at line "
                                + lineNumber,
                        null);
            }

            if (!jobId.equals(event.getJobId())) {
                throw historyFailure(
                        "Job event identity mismatch for "
                                + jobId
                                + " at line "
                                + lineNumber,
                        null);
            }

            return event;
        } catch (JsonProcessingException failure) {
            throw historyFailure(
                    "Invalid Job event JSON for "
                            + jobId
                            + " at line "
                            + lineNumber,
                    failure);
        }
    }

    private byte[] serialize(JobEventEnvelope event) {
        try {
            byte[] json = mapper.writeValueAsBytes(event);
            byte[] line = new byte[json.length + 1];
            System.arraycopy(
                    json,
                    0,
                    line,
                    0,
                    json.length);
            line[line.length - 1] = (byte) '\n';
            return line;
        } catch (JsonProcessingException failure) {
            throw historyFailure(
                    "Could not serialize Job event for "
                            + event.getJobId(),
                    failure);
        }
    }

    private Object lockFor(String jobId) {
        int index = (jobId.hashCode() & Integer.MAX_VALUE)
                % jobLocks.length;
        return jobLocks[index];
    }

    private static Object[] createLocks() {
        Object[] locks = new Object[64];
        for (int index = 0; index < locks.length; index++) {
            locks[index] = new Object();
        }
        return locks;
    }

    private Path eventFile(String jobId) {
        return eventDirectory.resolve(
                jobId + EVENT_FILE_SUFFIX);
    }

    private static void validatePage(
            long afterSequence,
            int limit) {

        if (afterSequence < 0L) {
            throw new IllegalArgumentException(
                    "afterSequence must not be negative");
        }
        if (limit <= 0 || limit > MAX_PAGE_SIZE) {
            throw new IllegalArgumentException(
                    "limit must be between 1 and "
                            + MAX_PAGE_SIZE);
        }
    }

    private static String requireJobId(String jobId) {
        if (jobId == null
                || !jobId.matches(SAFE_JOB_ID)) {
            throw new IllegalArgumentException(
                    "jobId contains unsupported characters");
        }
        return jobId;
    }

    private static JobEventHistoryException historyFailure(
            String message,
            Throwable cause) {
        return new JobEventHistoryException(
                message,
                cause);
    }
}
