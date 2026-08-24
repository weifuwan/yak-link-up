package com.link.up.framework.execution;

import com.link.up.api.sink.CommitScope;
import com.link.up.framework.job.CommitSummary;
import com.link.up.framework.job.JobResult;
import com.link.up.framework.job.JobStatus;
import com.link.up.framework.job.PipelineResult;
import com.link.up.framework.job.PipelineStatus;
import com.link.up.framework.planner.JobGraph;
import com.link.up.framework.planner.PipelineGraph;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CancellationException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class JobCoordinatorTest {

    @Test
    public void shouldOwnLifecycleAndAggregateScheduledResults() {
        final PipelineGraph<?> first =
                RuntimeRoleTestSupport.pipelineGraph("pipeline-first");
        final PipelineGraph<?> second =
                RuntimeRoleTestSupport.pipelineGraph("pipeline-second");
        JobGraph jobGraph =
                RuntimeRoleTestSupport.jobGraph(
                        Arrays.<PipelineGraph<?>>asList(
                                first,
                                second),
                        2);
        final ExecutionGraph executionGraph =
                new ExecutionGraph(
                        jobGraph,
                        100L,
                        "run-phase-3",
                        "phase-3.log");

        final CommitSummary firstSummary =
                new CommitSummary(
                        1,
                        1,
                        1,
                        0,
                        0,
                        10L,
                        10L,
                        10L,
                        0L,
                        0L,
                        CommitScope.TASK_LOCAL,
                        "first");
        final CommitSummary secondSummary =
                new CommitSummary(
                        1,
                        1,
                        1,
                        1,
                        0,
                        0L,
                        0L,
                        0L,
                        0L,
                        0L,
                        CommitScope.TASK_LOCAL,
                        "second");

        PipelineScheduler scheduler =
                new PipelineScheduler() {
                    @Override
                    public PipelineScheduleResult schedule(
                            List<PipelineGraph<?>> pipelineGraphs,
                            int parallelism,
                            CancellationToken cancellationToken,
                            PipelineExecutor pipelineExecutor) {

                        assertSame(
                                executionGraph.getJobGraph().getPipelineGraphs(),
                                pipelineGraphs);
                        assertEquals(2, parallelism);
                        assertSame(
                                executionGraph.getCancellationToken(),
                                cancellationToken);

                        return new PipelineScheduleResult(
                                Arrays.asList(
                                        RuntimeRoleTestSupport.result(
                                                first,
                                                PipelineStatus.SUCCEEDED,
                                                firstSummary,
                                                null),
                                        RuntimeRoleTestSupport.result(
                                                second,
                                                PipelineStatus.SUCCEEDED,
                                                secondSummary,
                                                null)),
                                null);
                    }
                };

        JobResult result =
                new JobCoordinator(
                        executionGraph,
                        scheduler,
                        unusedExecutor())
                        .execute();

        assertEquals(JobStatus.SUCCEEDED, result.getStatus());
        assertEquals(JobStatus.SUCCEEDED, executionGraph.getStatus());
        assertTrue(executionGraph.isTerminal());
        assertSame(result, executionGraph.getResult());
        assertEquals(2, result.getPipelineResults().size());
        assertEquals(2, result.getCommitSummary().getTotalTaskCount());
        assertEquals(2, result.getCommitSummary().getCommittedTaskCount());
        assertEquals(1, result.getCommitSummary().getEmptyCommittedTaskCount());
        assertEquals(10L, result.getCommitSummary().getSuccessfullyCommittedRecordCount());
    }

    @Test
    public void shouldReturnCanceledWhenCancellationWasRequestedBeforeScheduling() {
        ExecutionGraph executionGraph =
                new ExecutionGraph(
                        RuntimeRoleTestSupport.jobGraph(
                                Collections.<PipelineGraph<?>>emptyList(),
                                1),
                        100L,
                        "run-canceled",
                        "canceled.log");

        executionGraph.requestCancellation(
                new CancellationException("cancel before run"));

        JobResult result =
                new JobCoordinator(
                        executionGraph,
                        emptyScheduler(),
                        unusedExecutor())
                        .execute();

        assertEquals(JobStatus.CANCELED, result.getStatus());
        assertEquals(JobStatus.CANCELED, executionGraph.getStatus());
        assertNull(result.getFailure());
    }

    @Test
    public void shouldFailExecutionGraphWhenSchedulerThrows() {
        final RuntimeException failure =
                new RuntimeException("scheduler failed");
        ExecutionGraph executionGraph =
                new ExecutionGraph(
                        RuntimeRoleTestSupport.jobGraph(
                                Collections.<PipelineGraph<?>>emptyList(),
                                1),
                        100L,
                        "run-failed",
                        "failed.log");

        PipelineScheduler scheduler =
                new PipelineScheduler() {
                    @Override
                    public PipelineScheduleResult schedule(
                            List<PipelineGraph<?>> pipelineGraphs,
                            int parallelism,
                            CancellationToken cancellationToken,
                            PipelineExecutor pipelineExecutor) {
                        throw failure;
                    }
                };

        try {
            new JobCoordinator(
                    executionGraph,
                    scheduler,
                    unusedExecutor())
                    .execute();
        } catch (RuntimeException actual) {
            assertSame(failure, actual);
        }

        assertEquals(JobStatus.FAILED, executionGraph.getStatus());
        assertSame(failure, executionGraph.getFailure());
        assertTrue(executionGraph.isTerminal());
        assertNull(executionGraph.getResult());
    }

    private static PipelineScheduler emptyScheduler() {
        return new PipelineScheduler() {
            @Override
            public PipelineScheduleResult schedule(
                    List<PipelineGraph<?>> pipelineGraphs,
                    int parallelism,
                    CancellationToken cancellationToken,
                    PipelineExecutor pipelineExecutor) {
                return PipelineScheduleResult.empty();
            }
        };
    }

    private static PipelineExecutor unusedExecutor() {
        return new PipelineExecutor() {
            @Override
            public PipelineResult execute(
                    PipelineGraph<?> pipelineGraph) {
                throw new AssertionError(
                        "PipelineExecutor should not be invoked by this test scheduler");
            }
        };
    }
}
