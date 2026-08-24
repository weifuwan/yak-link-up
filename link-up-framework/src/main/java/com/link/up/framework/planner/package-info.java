/**
 * Physical job planning for the local Link-Up runtime.
 *
 * <p>Planner components transform prepared connector/job inputs into immutable
 * {@link com.link.up.framework.planner.JobGraph} and
 * {@link com.link.up.framework.planner.PipelineGraph} models. They may
 * calculate topology, parallelism, and split assignment, but must not create
 * runtime ownership such as threads, channels, cancellation tokens, metrics,
 * or split providers.
 */
package com.link.up.framework.planner;
