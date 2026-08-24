package com.link.up.framework.execution;

import com.link.up.framework.job.PipelineResult;
import com.link.up.framework.planner.PipelineGraph;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class LocalPipelineSchedulerTest {

    @Test
    public void shouldRespectConfiguredPipelineParallelism() {
        final List<PipelineGraph<?>> graphs =
                Arrays.<PipelineGraph<?>>asList(
                        RuntimeRoleTestSupport.pipelineGraph("pipeline-a"),
                        RuntimeRoleTestSupport.pipelineGraph("pipeline-b"),
                        RuntimeRoleTestSupport.pipelineGraph("pipeline-c"));
        final CountDownLatch firstWave =
                new CountDownLatch(2);
        final AtomicInteger active =
                new AtomicInteger();
        final AtomicInteger maxActive =
                new AtomicInteger();

        PipelineExecutor executor =
                new PipelineExecutor() {
                    @Override
                    public PipelineResult execute(
                            PipelineGraph<?> pipelineGraph) {

                        int running = active.incrementAndGet();
                        updateMaximum(maxActive, running);
                        firstWave.countDown();

                        try {
                            if (!firstWave.await(
                                    3L,
                                    TimeUnit.SECONDS)) {
                                throw new AssertionError(
                                        "Expected two pipelines to run concurrently");
                            }
                            return RuntimeRoleTestSupport.success(
                                    pipelineGraph);
                        } catch (InterruptedException interrupted) {
                            Thread.currentThread().interrupt();
                            throw new RuntimeException(interrupted);
                        } finally {
                            active.decrementAndGet();
                        }
                    }
                };

        CancellationToken token = new CancellationToken();
        PipelineScheduleResult result =
                new LocalPipelineScheduler().schedule(
                        graphs,
                        2,
                        token,
                        executor);

        assertNull(result.getFailure());
        assertEquals(3, result.getPipelineResults().size());
        assertEquals(2, maxActive.get());
        assertFalse(token.isCancelled());
    }

    @Test
    public void shouldPropagateFirstPipelineFailureToCancellation() {
        final PipelineGraph<?> failing =
                RuntimeRoleTestSupport.pipelineGraph("pipeline-failing");
        PipelineGraph<?> succeeding =
                RuntimeRoleTestSupport.pipelineGraph("pipeline-succeeding");
        final RuntimeException failure =
                new RuntimeException("pipeline failed");

        PipelineExecutor executor =
                new PipelineExecutor() {
                    @Override
                    public PipelineResult execute(
                            PipelineGraph<?> pipelineGraph) {
                        if (pipelineGraph == failing) {
                            throw failure;
                        }
                        return RuntimeRoleTestSupport.success(
                                pipelineGraph);
                    }
                };

        CancellationToken token = new CancellationToken();
        PipelineScheduleResult result =
                new LocalPipelineScheduler().schedule(
                        Arrays.asList(failing, succeeding),
                        1,
                        token,
                        executor);

        assertSame(failure, result.getFailure());
        assertTrue(token.isCancelled());
        assertSame(failure, token.getCause());
    }

    @Test
    public void shouldReturnEmptyWithoutInvokingExecutor() {
        final AtomicInteger calls = new AtomicInteger();

        PipelineScheduleResult result =
                new LocalPipelineScheduler().schedule(
                        Collections.<PipelineGraph<?>>emptyList(),
                        1,
                        new CancellationToken(),
                        new PipelineExecutor() {
                            @Override
                            public PipelineResult execute(
                                    PipelineGraph<?> pipelineGraph) {
                                calls.incrementAndGet();
                                return null;
                            }
                        });

        assertEquals(0, calls.get());
        assertTrue(result.getPipelineResults().isEmpty());
        assertNull(result.getFailure());
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldRejectNonPositiveParallelism() {
        new LocalPipelineScheduler().schedule(
                Collections.<PipelineGraph<?>>emptyList(),
                0,
                new CancellationToken(),
                new PipelineExecutor() {
                    @Override
                    public PipelineResult execute(
                            PipelineGraph<?> pipelineGraph) {
                        return null;
                    }
                });
    }

    private static void updateMaximum(
            AtomicInteger maximum,
            int candidate) {

        while (true) {
            int current = maximum.get();
            if (candidate <= current
                    || maximum.compareAndSet(
                            current,
                            candidate)) {
                return;
            }
        }
    }
}
