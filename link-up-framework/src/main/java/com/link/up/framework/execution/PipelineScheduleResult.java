package com.link.up.framework.execution;

import com.link.up.framework.job.PipelineResult;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Immutable outcome returned by {@link PipelineScheduler}.
 */
final class PipelineScheduleResult {

    private final List<PipelineResult> pipelineResults;
    private final Throwable failure;

    PipelineScheduleResult(
            List<PipelineResult> pipelineResults,
            Throwable failure) {

        Objects.requireNonNull(
                pipelineResults,
                "pipelineResults must not be null");

        List<PipelineResult> copy =
                new ArrayList<PipelineResult>(
                        pipelineResults.size());

        for (PipelineResult result : pipelineResults) {
            copy.add(
                    Objects.requireNonNull(
                            result,
                            "pipelineResults must not contain null values"));
        }

        this.pipelineResults =
                Collections.unmodifiableList(copy);
        this.failure = failure;
    }

    static PipelineScheduleResult empty() {
        return new PipelineScheduleResult(
                Collections.<PipelineResult>emptyList(),
                null);
    }

    List<PipelineResult> getPipelineResults() {
        return pipelineResults;
    }

    Throwable getFailure() {
        return failure;
    }
}
