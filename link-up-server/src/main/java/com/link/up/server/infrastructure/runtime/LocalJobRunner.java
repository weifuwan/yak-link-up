package com.link.up.server.infrastructure.runtime;

import com.link.up.framework.execution.JobExecution;
import com.link.up.framework.execution.JobExecutionListener;
import com.link.up.framework.job.JobDefinition;
import com.link.up.framework.job.JobResult;
import com.link.up.server.application.port.JobExecutor;

import java.util.Objects;
import java.util.concurrent.CancellationException;

/** Executes one accepted local job and bridges Framework callbacks. */
final class LocalJobRunner {

    private final JobExecutor jobExecutor;

    LocalJobRunner(JobExecutor jobExecutor) {
        this.jobExecutor = Objects.requireNonNull(
                jobExecutor,
                "jobExecutor must not be null");
    }

    void run(
            final ActiveJobExecution active,
            JobDefinition definition) {

        Objects.requireNonNull(
                active,
                "active must not be null");
        Objects.requireNonNull(
                definition,
                "definition must not be null");

        try {
            if (!active.getListener().onStarting()) {
                active.getListener().onCompleted(
                        null,
                        null,
                        true);
                return;
            }

            JobResult result =
                    jobExecutor.execute(
                            definition,
                            executionListener(active));

            active.getListener().onCompleted(
                    result,
                    null,
                    false);

        } catch (Throwable failure) {
            boolean cancellationLike =
                    active.isCancellationRequested()
                            || failure instanceof CancellationException
                            || failure instanceof InterruptedException;

            if (failure instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }

            active.getListener().onCompleted(
                    null,
                    failure,
                    cancellationLike);

            if (failure instanceof Error) {
                throw (Error) failure;
            }
        } finally {
            active.clearExecution();
        }
    }

    private JobExecutionListener executionListener(
            final ActiveJobExecution active) {

        return new JobExecutionListener() {
            @Override
            public void onJobLogCreated(
                    String runId,
                    String jobLogFile) {

                active.getListener().onJobLogCreated(
                        runId,
                        jobLogFile);
            }

            @Override
            public void onJobExecutionCreated(
                    JobExecution execution) {

                active.bindExecution(execution);
            }
        };
    }
}
