package com.link.up.framework.execution;

import com.link.up.framework.job.PipelineResult;
import com.link.up.framework.planner.PipelineGraph;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Local bounded-concurrency scheduler for {@link PipelineGraph} instances.
 *
 * <p>The scheduler decides how many pipelines may run concurrently and collects
 * their outcomes in completion order. The actual pipeline work is delegated to
 * {@link PipelineExecutor}.
 */
final class LocalPipelineScheduler
        implements PipelineScheduler {

    @Override
    public PipelineScheduleResult schedule(
            List<PipelineGraph<?>> pipelineGraphs,
            int parallelism,
            CancellationToken cancellationToken,
            final PipelineExecutor pipelineExecutor) {

        Objects.requireNonNull(
                pipelineGraphs,
                "pipelineGraphs must not be null");
        Objects.requireNonNull(
                cancellationToken,
                "cancellationToken must not be null");
        Objects.requireNonNull(
                pipelineExecutor,
                "pipelineExecutor must not be null");

        if (parallelism <= 0) {
            throw new IllegalArgumentException(
                    "parallelism must be greater than 0");
        }

        if (pipelineGraphs.isEmpty()) {
            return PipelineScheduleResult.empty();
        }

        for (PipelineGraph<?> pipelineGraph : pipelineGraphs) {
            Objects.requireNonNull(
                    pipelineGraph,
                    "pipelineGraphs must not contain null values");
        }

        int threadCount =
                Math.min(
                        parallelism,
                        pipelineGraphs.size());

        ExecutorService pool =
                Executors.newFixedThreadPool(threadCount);
        CompletionService<PipelineResult> completionService =
                new ExecutorCompletionService<PipelineResult>(pool);
        List<Future<PipelineResult>> submitted =
                new ArrayList<Future<PipelineResult>>(
                        pipelineGraphs.size());
        List<PipelineResult> results =
                new ArrayList<PipelineResult>();
        Throwable firstFailure = null;

        try {
            for (final PipelineGraph<?> pipelineGraph : pipelineGraphs) {
                submitted.add(
                        completionService.submit(
                                new java.util.concurrent.Callable<PipelineResult>() {
                                    @Override
                                    public PipelineResult call() {
                                        return pipelineExecutor.execute(
                                                pipelineGraph);
                                    }
                                }));
            }

            for (int index = 0;
                 index < submitted.size();
                 index++) {

                try {
                    PipelineResult result =
                            completionService
                                    .take()
                                    .get();

                    if (result == null) {
                        throw new IllegalStateException(
                                "PipelineExecutor returned a null result");
                    }

                    results.add(result);

                    if (result.getFailure() != null
                            && firstFailure == null) {
                        firstFailure = result.getFailure();
                        cancellationToken.cancel(firstFailure);
                    }

                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();

                    if (firstFailure == null) {
                        firstFailure = interrupted;
                        cancellationToken.cancel(interrupted);
                    }

                    cancelSubmitted(submitted);
                    break;

                } catch (ExecutionException executionFailure) {
                    Throwable failure =
                            executionFailure.getCause() == null
                                    ? executionFailure
                                    : executionFailure.getCause();

                    if (firstFailure == null) {
                        firstFailure = failure;
                        cancellationToken.cancel(failure);
                    }
                }
            }

        } finally {
            pool.shutdownNow();
        }

        return new PipelineScheduleResult(
                results,
                firstFailure);
    }

    private void cancelSubmitted(
            List<Future<PipelineResult>> submitted) {

        for (Future<PipelineResult> future : submitted) {
            future.cancel(true);
        }
    }
}
