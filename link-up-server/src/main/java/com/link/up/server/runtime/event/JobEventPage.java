package com.link.up.server.runtime.event;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/** Immutable sequence-based page from one Job event journal. */
public final class JobEventPage {

    private final String jobId;
    private final List<JobEventEnvelope> items;
    private final long nextSequence;
    private final boolean hasMore;

    public JobEventPage(
            String jobId,
            List<JobEventEnvelope> items,
            long nextSequence,
            boolean hasMore) {

        if (nextSequence < 0L) {
            throw new IllegalArgumentException(
                    "nextSequence must not be negative");
        }

        this.jobId = Objects.requireNonNull(
                jobId,
                "jobId must not be null");
        this.items = Collections.unmodifiableList(
                new ArrayList<JobEventEnvelope>(
                        Objects.requireNonNull(
                                items,
                                "items must not be null")));
        this.nextSequence = nextSequence;
        this.hasMore = hasMore;
    }

    public static JobEventPage empty(
            String jobId,
            long afterSequence) {
        return new JobEventPage(
                jobId,
                Collections.<JobEventEnvelope>emptyList(),
                afterSequence,
                false);
    }

    public String getJobId() {
        return jobId;
    }

    public List<JobEventEnvelope> getItems() {
        return items;
    }

    public long getNextSequence() {
        return nextSequence;
    }

    public boolean isHasMore() {
        return hasMore;
    }
}
