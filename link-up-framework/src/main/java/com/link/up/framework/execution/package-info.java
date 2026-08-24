/**
 * Runtime lifecycle, execution state, coordination, scheduling, and task
 * execution.
 *
 * <p>This package consumes immutable graphs produced by
 * {@code framework.planner}. Mutable per-run ownership belongs here, rooted at
 * {@link com.link.up.framework.execution.ExecutionGraph}. Runtime roles are
 * layered deliberately: {@code JobExecution} is the facade,
 * {@code JobCoordinator} owns job lifecycle, {@code PipelineScheduler} owns
 * pipeline concurrency, {@code PipelineExecutor} executes one pipeline, and
 * {@code TaskExecutor} executes concrete tasks. New source/sink work belongs
 * under {@code framework.execution.task}; do not introduce a parallel
 * processor/task hierarchy beside the active runtime.
 */
package com.link.up.framework.execution;
