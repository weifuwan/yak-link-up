/**
 * Runtime lifecycle, execution state, cancellation, coordination, and task
 * execution.
 *
 * <p>This package consumes immutable graphs produced by
 * {@code framework.planner}. Mutable per-run ownership belongs here, rooted at
 * {@link com.link.up.framework.execution.ExecutionGraph}. New executable
 * source/sink work belongs under {@code framework.execution.task}; do not
 * introduce a parallel processor/task hierarchy beside the active runtime.
 */
package com.link.up.framework.execution;
