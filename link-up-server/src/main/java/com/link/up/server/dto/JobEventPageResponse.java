package com.link.up.server.dto;

import com.link.up.server.runtime.event.JobEventEnvelope;
import com.link.up.server.runtime.event.JobEventPage;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** Sequence-based REST page for one Job's runtime lifecycle events. */
public final class JobEventPageResponse {

    private final String jobId;
    private final List<JobEventEnvelope> items;
    private final long nextSequence;
    private final boolean hasMore;
    private final boolean completed;

    public JobEventPageResponse(
            JobEventPage page,
            boolean completed) {

        this.jobId = page.getJobId();
        this.items = Collections.unmodifiableList(
                new ArrayList<JobEventEnvelope>(
                        page.getItems()));
        this.nextSequence = page.getNextSequence();
        this.hasMore = page.isHasMore();
        this.completed = completed;
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

    public boolean isCompleted() {
        return completed;
    }
}
