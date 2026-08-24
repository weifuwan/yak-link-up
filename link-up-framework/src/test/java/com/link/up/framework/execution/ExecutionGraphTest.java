package com.link.up.framework.execution;

import com.link.up.framework.job.ExecutionConfig;
import com.link.up.framework.job.JobResult;
import com.link.up.framework.job.JobStatus;
import com.link.up.framework.planner.JobGraph;
import org.junit.Test;

import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class ExecutionGraphTest {

    @Test
    public void shouldTrackRuntimeStateSeparatelyFromJobGraph() {
        JobGraph jobGraph = emptyGraph();
        ExecutionGraph executionGraph =
                new ExecutionGraph(
                        jobGraph,
                        100L,
                        "run-1",
                        "job-run-1.log");

        assertSame(jobGraph, executionGraph.getJobGraph());
        assertEquals(JobStatus.CREATED, executionGraph.getStatus());
        assertFalse(executionGraph.isTerminal());
        assertNull(executionGraph.getFailure());

        executionGraph.markRunning();
        assertEquals(JobStatus.RUNNING, executionGraph.getStatus());

        JobResult result =
                new JobResult(
                        "phase-2-test",
                        JobStatus.SUCCEEDED,
                        100L,
                        250L,
                        executionGraph.getMetrics(),
                        null);

        executionGraph.complete(result);

        assertEquals(JobStatus.SUCCEEDED, executionGraph.getStatus());
        assertEquals(250L, executionGraph.getEndTimeMillis());
        assertSame(result, executionGraph.getResult());
        assertTrue(executionGraph.isTerminal());
    }

    @Test
    public void shouldKeepCancellationInExecutionState() {
        ExecutionGraph executionGraph =
                new ExecutionGraph(
                        emptyGraph(),
                        100L,
                        "run-2",
                        "job-run-2.log");

        assertFalse(executionGraph.isCancellationRequested());

        executionGraph.requestCancellation(
                new java.util.concurrent.CancellationException(
                        "test cancellation"));

        assertTrue(executionGraph.isCancellationRequested());
        assertEquals(JobStatus.CREATED, executionGraph.getStatus());
    }

    @Test
    public void shouldIsolateRuntimeStateAcrossExecutionsOfSameJobGraph() {
        JobGraph jobGraph = emptyGraph();
        ExecutionGraph first =
                new ExecutionGraph(
                        jobGraph,
                        100L,
                        "run-a",
                        "job-run-a.log");
        ExecutionGraph second =
                new ExecutionGraph(
                        jobGraph,
                        200L,
                        "run-b",
                        "job-run-b.log");

        assertSame(jobGraph, first.getJobGraph());
        assertSame(jobGraph, second.getJobGraph());
        assertNotSame(first.getMetrics(), second.getMetrics());

        first.requestCancellation(
                new java.util.concurrent.CancellationException(
                        "cancel first run"));

        assertTrue(first.isCancellationRequested());
        assertFalse(second.isCancellationRequested());
        assertEquals(JobStatus.CREATED, second.getStatus());
    }

    @Test
    public void shouldIgnoreLateCancellationAfterTerminalCompletion() {
        ExecutionGraph executionGraph =
                new ExecutionGraph(
                        emptyGraph(),
                        100L,
                        "run-4",
                        "job-run-4.log");
        executionGraph.markRunning();
        executionGraph.complete(
                new JobResult(
                        "phase-2-test",
                        JobStatus.SUCCEEDED,
                        100L,
                        250L,
                        executionGraph.getMetrics(),
                        null));

        executionGraph.requestCancellation(
                new java.util.concurrent.CancellationException(
                        "too late"));

        assertEquals(JobStatus.SUCCEEDED, executionGraph.getStatus());
        assertFalse(executionGraph.isCancellationRequested());
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldRejectTerminalResultForDifferentJob() {
        ExecutionGraph executionGraph =
                new ExecutionGraph(
                        emptyGraph(),
                        100L,
                        "run-5",
                        "job-run-5.log");
        executionGraph.markRunning();

        executionGraph.complete(
                new JobResult(
                        "different-job",
                        JobStatus.SUCCEEDED,
                        100L,
                        250L,
                        executionGraph.getMetrics(),
                        null));
    }

    @Test(expected = IllegalStateException.class)
    public void shouldRejectSecondExecutionStart() {
        ExecutionGraph executionGraph =
                new ExecutionGraph(
                        emptyGraph(),
                        100L,
                        "run-3",
                        "job-run-3.log");

        executionGraph.markRunning();
        executionGraph.markRunning();
    }

    private static JobGraph emptyGraph() {
        return new JobGraph(
                "phase-2-test",
                new ExecutionConfig(
                        100,
                        1,
                        1,
                        32),
                Collections.emptyList());
    }
}
