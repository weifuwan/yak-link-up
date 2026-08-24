package com.link.up.framework.execution;

import com.link.up.framework.connector.ConnectorPreparer;
import com.link.up.framework.connector.FactoryRegistry;
import com.link.up.framework.connector.PreparedJob;
import com.link.up.framework.job.JobDefinition;
import com.link.up.framework.job.JobResult;
import com.link.up.framework.planner.JobGraph;
import com.link.up.framework.planner.JobPlanner;
import org.apache.logging.log4j.CloseableThreadContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.file.Path;
import java.util.Objects;

/**
 * Local offline Link-Up execution engine.
 */
public final class LocalFluxEngine
        implements FluxEngine {

    private static final Logger LOG =
            LogManager.getLogger(LocalFluxEngine.class);

    private final ClassLoader classLoader;
    private final ConnectorPreparer connectorPreparer;
    private final JobPlanner jobPlanner;
    private final FactoryRegistry registry;

    public LocalFluxEngine(
            ClassLoader classLoader,
            ConnectorPreparer connectorPreparer,
            JobPlanner jobPlanner) {
        this(
                classLoader,
                connectorPreparer,
                jobPlanner,
                null);
    }

    private LocalFluxEngine(
            ClassLoader classLoader,
            ConnectorPreparer connectorPreparer,
            JobPlanner jobPlanner,
            FactoryRegistry registry) {

        this.classLoader = Objects.requireNonNull(
                classLoader,
                "classLoader must not be null");
        this.connectorPreparer = Objects.requireNonNull(
                connectorPreparer,
                "connectorPreparer must not be null");
        this.jobPlanner = Objects.requireNonNull(
                jobPlanner,
                "jobPlanner must not be null");
        this.registry = registry;
    }

    public static LocalFluxEngine create(
            ClassLoader classLoader) {

        ClassLoader effectiveClassLoader =
                classLoader == null
                        ? Thread.currentThread()
                                .getContextClassLoader()
                        : classLoader;

        FactoryRegistry registry =
                FactoryRegistry.discover(
                        effectiveClassLoader);
        ConnectorPreparer preparer =
                new ConnectorPreparer(
                        registry,
                        effectiveClassLoader);
        JobPlanner planner = new JobPlanner();

        return new LocalFluxEngine(
                effectiveClassLoader,
                preparer,
                planner,
                registry);
    }

    public static LocalFluxEngine create(
            ClassLoader classLoader,
            Path... pluginDirectories) {

        ClassLoader effectiveClassLoader =
                classLoader == null
                        ? Thread.currentThread()
                                .getContextClassLoader()
                        : classLoader;

        FactoryRegistry registry =
                FactoryRegistry.discover(
                        effectiveClassLoader,
                        pluginDirectories);

        return new LocalFluxEngine(
                effectiveClassLoader,
                new ConnectorPreparer(
                        registry,
                        effectiveClassLoader),
                new JobPlanner(),
                registry);
    }

    @Override
    public JobResult execute(
            JobDefinition definition)
            throws Exception {
        return execute(definition, null);
    }

    /**
     * Exposes the active JobExecution to the local server so cancellation can
     * be propagated after planning and before/during task execution.
     */
    public JobResult execute(
            JobDefinition definition,
            JobExecutionListener listener)
            throws Exception {

        Objects.requireNonNull(
                definition,
                "definition must not be null");

        long logIdentityTimeMillis =
                System.currentTimeMillis();
        String runId =
                JobLogFileName.createJobId(
                        definition.getName(),
                        logIdentityTimeMillis);
        String jobLogFile =
                JobLogFileName.create(
                        definition.getName(),
                        logIdentityTimeMillis);

        try (CloseableThreadContext.Instance ignored =
                     CloseableThreadContext
                             .put("runId", runId)
                             .put("jobId", runId)
                             .put("jobName", definition.getName())
                             .put("jobLogFile", jobLogFile)) {

            if (listener != null) {
                listener.onJobLogCreated(
                        runId,
                        jobLogFile);
            }

            LOG.info(
                    "Job preparation started: jobName={}, runId={}",
                    definition.getName(),
                    runId);

            PreparedJob preparedJob;
            JobGraph jobGraph;

            try {
                preparedJob = connectorPreparer.prepare(definition);
                jobGraph = jobPlanner.plan(preparedJob);
            } catch (Exception failure) {
                LOG.error(
                        "Job preparation failed: jobName={}, runId={}",
                        definition.getName(),
                        runId,
                        failure);
                throw failure;
            } catch (Error failure) {
                LOG.error(
                        "Job preparation failed: jobName={}, runId={}",
                        definition.getName(),
                        runId,
                        failure);
                throw failure;
            }

            JobExecution jobExecution =
                    new JobExecution(
                            jobGraph,
                            classLoader,
                            System.currentTimeMillis(),
                            runId,
                            jobLogFile);

            if (listener != null) {
                listener.onJobExecutionCreated(jobExecution);
            }

            return jobExecution.execute();

        } finally {
            if (registry != null) {
                registry.close();
            }
        }
    }

    @Override
    public void close() {
        if (registry != null) {
            registry.close();
        }
    }
}
