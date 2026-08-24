package com.link.up.framework.execution;

import com.link.up.framework.job.ExecutionConfig;
import com.link.up.framework.job.JobResult;
import com.link.up.framework.job.JobStatus;
import com.link.up.framework.planner.JobGraph;
import org.junit.Test;

import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
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
