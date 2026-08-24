package com.link.up.server.application.port;

import com.link.up.framework.job.JobDefinition;
import com.link.up.framework.job.JobResult;
import com.link.up.framework.metrics.JobMetrics;

/**
 * Application port for local Worker admission, thread ownership and shutdown.
 */
public interface JobRuntimeScheduler
        extends AutoCloseable {

    interface Listener {
        void onQueued();

        boolean onStarting();

        void onJobLogCreated(
                String runId,
                String jobLogFile);

        void onCompleted(
                JobResult result,
                Throwable failure,
                boolean cancellationLike);

        void onLost();
    }

    void schedule(
            String jobId,
            JobDefinition definition,
            Listener listener);

    void cancel(String jobId);

    JobMetrics getMetrics(String jobId);

    boolean isClosed();

    @Override
    void close();
}
