package com.link.up.framework.execution;

import com.link.up.framework.planner.PipelineGraph;

import java.util.List;

/**
 * Schedules physical pipelines for one local execution.
 *
 * <p>A scheduler owns concurrency and completion-order collection. It must not
 * execute connector I/O itself; executable work is delegated to a
 * {@link PipelineExecutor}.
 */
interface PipelineScheduler {

    PipelineScheduleResult schedule(
            List<PipelineGraph<?>> pipelineGraphs,
            int parallelism,
            CancellationToken cancellationToken,
            PipelineExecutor pipelineExecutor);
}
