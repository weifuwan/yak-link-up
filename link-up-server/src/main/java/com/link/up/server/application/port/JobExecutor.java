package com.link.up.server.application.port;

import com.link.up.framework.execution.JobExecutionListener;
import com.link.up.framework.job.JobDefinition;
import com.link.up.framework.job.JobResult;

/**
 * Application port for invoking the framework execution engine.
 */
public interface JobExecutor {

    JobResult execute(
            JobDefinition definition,
            JobExecutionListener listener)
            throws Exception;
}
