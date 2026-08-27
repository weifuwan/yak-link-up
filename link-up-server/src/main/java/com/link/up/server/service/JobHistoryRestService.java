package com.link.up.server.service;

import com.link.up.server.application.JobApplication;
import com.link.up.server.application.port.JobEventReader;
import com.link.up.server.dto.JobHistoryResponse;
import com.link.up.server.runtime.JobExecutionMetadata;
import com.link.up.server.runtime.JobSnapshot;
import com.link.up.server.runtime.event.JobEventPage;

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

        return new JobHistoryResponse(
                snapshot,
                metadata,
                events);
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
