package com.link.up.framework.planner;

import com.link.up.framework.job.ExecutionConfig;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Immutable physical graph produced by {@link JobPlanner}.
 *
 * <p>A JobGraph describes what the local runtime should execute. It contains
 * no mutable runtime state such as executors, cancellation tokens, channels,
 * metrics, or split queues.
 */
public final class JobGraph {

    private final String jobName;
    private final ExecutionConfig executionConfig;
    private final List<PipelineGraph<?>> pipelineGraphs;

    public JobGraph(
            String jobName,
            ExecutionConfig executionConfig,
            List<PipelineGraph<?>> pipelineGraphs) {

        this.jobName = requireText(jobName, "jobName");
        this.executionConfig = Objects.requireNonNull(
                executionConfig,
                "executionConfig must not be null");
        this.pipelineGraphs = Collections.unmodifiableList(
                new ArrayList<PipelineGraph<?>>(
                        Objects.requireNonNull(
                                pipelineGraphs,
                                "pipelineGraphs must not be null")));
    }

    private static String requireText(String value, String name) {
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
        return value.trim();
    }

    public String getJobName() {
        return jobName;
    }

    public ExecutionConfig getExecutionConfig() {
        return executionConfig;
    }

    public List<PipelineGraph<?>> getPipelineGraphs() {
        return pipelineGraphs;
    }

    public List<SourceTaskPlan<?>> getSourceTaskPlans() {
        List<SourceTaskPlan<?>> result =
                new ArrayList<SourceTaskPlan<?>>();
        for (PipelineGraph<?> pipeline : pipelineGraphs) {
            result.addAll(pipeline.getSourceTaskPlans());
        }
        return Collections.unmodifiableList(result);
    }

    public List<SinkTaskPlan> getSinkTaskPlans() {
        List<SinkTaskPlan> result =
                new ArrayList<SinkTaskPlan>();
        for (PipelineGraph<?> pipeline : pipelineGraphs) {
            result.addAll(pipeline.getSinkTaskPlans());
        }
        return Collections.unmodifiableList(result);
    }

    public boolean isEmpty() {
        return pipelineGraphs.isEmpty();
    }
}
