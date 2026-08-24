/**
 * Runtime lifecycle, cancellation, coordination, and task execution.
 *
 * <p>This package consumes plans produced by {@code framework.planner}. New
 * executable source/sink work belongs under {@code framework.execution.task};
 * do not introduce a parallel processor/task hierarchy beside the active
 * runtime path.
 */
package com.link.up.framework.execution;
