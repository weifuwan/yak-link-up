package com.link.up.framework.execution;

import com.link.up.framework.job.PipelineResult;
import com.link.up.framework.planner.PipelineGraph;

/**
 * Executes one already-planned pipeline.
 *
 * <p>The executor does not decide when the pipeline runs or how many pipelines
 * may run concurrently. Those decisions belong to {@link PipelineScheduler}.
 */
interface PipelineExecutor {

    PipelineResult execute(PipelineGraph<?> pipelineGraph);
}
