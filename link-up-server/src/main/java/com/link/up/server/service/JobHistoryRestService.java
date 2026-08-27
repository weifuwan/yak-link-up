package com.link.up.server.service;

import com.link.up.server.application.JobApplication;
import com.link.up.server.application.port.JobEventHistoryException;
import com.link.up.server.application.port.JobEventReader;
import com.link.up.server.dto.JobHistoryResponse;
import com.link.up.server.runtime.JobAttemptMetadata;
import com.link.up.server.runtime.JobExecutionMetadata;
import com.link.up.server.runtime.JobSnapshot;
import com.link.up.server.runtime.event.JobEventEnvelope;
import com.link.up.server.runtime.event.JobEventPage;
import com.link.up.server.runtime.event.JobExecutionFacts;

import java.util.List;
import java.util.Objects;

/** Read-only composition boundary for Spark-style Job execution history. */
public final class JobHistoryRestService {

    private static final int MAX_PAGE_SIZE = 1_000;

    private final JobApplication jobApplication;
    private final JobEventReader eventReader;

    public JobHistoryRestService(
            JobApplication jobApplication,
            JobEventReader eventReader) {

        this.jobApplication = Objects.requireNonNull(
                jobApplication,
                "jobApplication must not be null");
        this.eventReader = Objects.requireNonNull(
                eventReader,
                "eventReader must not be null");
    }

    public JobHistoryResponse history(
            String jobId,
            long afterSequence,
            int limit) {

        String normalizedJobId = requireText(jobId, "jobId");
        validatePage(afterSequence, limit);

        JobSnapshot snapshot =
                jobApplication.getJob(normalizedJobId);
        JobExecutionMetadata metadata =
                jobApplication.getMetadata(normalizedJobId);
        JobEventPage events = eventReader.read(
                normalizedJobId,
                afterSequence,
                limit);

        JobExecutionFacts execution =
                JobExecutionFacts.from(snapshot);
        String currentAttemptId = currentAttemptId(metadata);

        if (!execution.hasExecutionDetails()
                && snapshot.getStatus().isTerminal()
                && currentAttemptId != null) {
            JobExecutionFacts retained = retainedExecution(
                    normalizedJobId,
                    currentAttemptId);
            if (retained != null) {
                execution = retained;
            }
        }

        return new JobHistoryResponse(
                snapshot,
                metadata,
                events,
                execution);
    }

    private JobExecutionFacts retainedExecution(
            String jobId,
            String attemptId) {

        long cursor = 0L;
        JobExecutionFacts latest = null;

        while (true) {
            JobEventPage page = eventReader.read(
                    jobId,
                    cursor,
                    MAX_PAGE_SIZE);

            for (JobEventEnvelope envelope : page.getItems()) {
                if (!attemptId.equals(envelope.getAttemptId())) {
                    continue;
                }

                JobExecutionFacts execution =
                        envelope.getEvent().getExecution();
                if (execution != null
                        && execution.hasExecutionDetails()) {
                    latest = execution;
                }
            }

            if (!page.isHasMore()) {
                return latest;
            }

            long next = page.getNextSequence();
            if (next <= cursor) {
                throw new JobEventHistoryException(
                        "Job event history cursor did not advance",
                        null);
            }
            cursor = next;
        }
    }

    private static String currentAttemptId(
            JobExecutionMetadata metadata) {

        if (metadata == null) {
            return null;
        }

        List<JobAttemptMetadata> attempts = metadata.getAttempts();
        if (attempts.isEmpty()) {
            return null;
        }

        return attempts.get(attempts.size() - 1).getAttemptId();
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

    private static String requireText(
            String value,
            String name) {

        if (value == null || value.trim().isEmpty()) {
            throw new IllegalArgumentException(
                    name + " must not be blank");
        }
        return value.trim();
    }
}
