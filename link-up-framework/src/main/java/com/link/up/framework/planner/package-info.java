/**
 * Physical job planning for the local Link-Up runtime.
 *
 * <p>Planner components transform prepared connector/job inputs and validated
 * Source splits into immutable {@link com.link.up.framework.planner.JobGraph}
 * and {@link com.link.up.framework.planner.PipelineGraph} models. They may
 * calculate topology, parallelism, and split assignment, but Source Enumerator
 * lifecycle belongs to {@code com.link.up.framework.source} and runtime
 * ownership such as threads, channels, cancellation tokens, metrics, or split
 * providers belongs to execution.</p>
 */
package com.link.up.framework.planner;
