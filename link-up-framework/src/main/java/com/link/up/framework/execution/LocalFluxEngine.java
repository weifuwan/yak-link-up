package com.link.up.framework.execution;

import com.link.up.api.exception.FluxRuntimeException;
import com.link.up.framework.connector.ConnectorPreparer;
import com.link.up.framework.connector.FactoryRegistry;
import com.link.up.framework.connector.PreparedJob;
import com.link.up.framework.connector.PreparedSource;
import com.link.up.framework.job.JobDefinition;
import com.link.up.framework.job.JobResult;
import com.link.up.framework.planner.JobGraph;
import com.link.up.framework.planner.JobPlanner;
import com.link.up.framework.planning.CapabilityNegotiation;
import com.link.up.framework.planning.CapabilityNegotiator;
import com.link.up.framework.planning.PlanningException;
import org.apache.logging.log4j.CloseableThreadContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.file.Path;
import java.util.Objects;

/** Local offline Link-Up execution engine and composition boundary. */
public final class LocalFluxEngine
        implements FluxEngine {

    private static final Logger LOG =
            LogManager.getLogger(LocalFluxEngine.class);

    private final ClassLoader classLoader;
    private final ConnectorPreparer connectorPreparer;
    private final JobPlanner jobPlanner;
    private final CapabilityNegotiator capabilityNegotiator;
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
        this.capabilityNegotiator =
                new CapabilityNegotiator(
                        this.connectorPreparer);
        this.registry = registry;
    }

    public static LocalFluxEngine create(
            ClassLoader classLoader) {

        ClassLoader effective =
                effectiveClassLoader(classLoader);

        return assemble(
                effective,
                FactoryRegistry.discover(effective));
    }

    public static LocalFluxEngine create(
            ClassLoader classLoader,
            Path... pluginDirectories) {

        ClassLoader effective =
                effectiveClassLoader(classLoader);

        return assemble(
                effective,
                FactoryRegistry.discover(
                        effective,
                        pluginDirectories));
    }

    @Override
    public JobResult execute(JobDefinition definition)
            throws Exception {

        return execute(
                definition,
                null);
    }

    /**
     * Executes one prepared Job and exposes the active execution to an optional
     * listener used by the local Worker control plane.
     */
    public JobResult execute(
            JobDefinition definition,
            JobExecutionListener listener)
            throws Exception {

        JobDefinition job = Objects.requireNonNull(
                definition,
                "definition must not be null");

        long logIdentityTimeMillis =
                System.currentTimeMillis();
        String runId = JobLogFileName.createJobId(
                job.getName(),
                logIdentityTimeMillis);
        String jobLogFile = JobLogFileName.create(
                job.getName(),
                logIdentityTimeMillis);

        try (CloseableThreadContext.Instance ignored =
                     openLogContext(
                             job,
                             runId,
                             jobLogFile)) {

            notifyJobLogCreated(
                    listener,
                    runId,
                    jobLogFile);

            JobGraph jobGraph = prepareAndPlan(
                    job,
                    runId);

            JobExecution execution = new JobExecution(
                    jobGraph,
                    classLoader,
                    System.currentTimeMillis(),
                    runId,
                    jobLogFile);

            notifyJobExecutionCreated(
                    listener,
                    execution);

            return execution.execute();

        } finally {
            closeRegistry();
        }
    }

    @Override
    public void close() {
        closeRegistry();
    }

    private JobGraph prepareAndPlan(
            JobDefinition definition,
            String runId)
            throws Exception {

        LOG.info(
                "Job preparation started: jobName={}, runId={}",
                definition.getName(),
                runId);

        try {
            CapabilityNegotiation initial =
                    capabilityNegotiator.negotiate(
                            definition);
            capabilityNegotiator.requireSatisfied(initial);

            PreparedSource<?> preparedSource =
                    prepareSource(definition);
            CapabilityNegotiation prepared =
                    capabilityNegotiator.negotiate(
                            definition,
                            preparedSource);
            capabilityNegotiator.requireSatisfied(prepared);

            PreparedJob preparedJob = prepareSink(
                    definition,
                    preparedSource);
            return plan(
                    definition,
                    preparedJob);

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
    }

    private PreparedSource<?> prepareSource(
            JobDefinition definition)
            throws Exception {

        try {
            return connectorPreparer.prepareSource(
                    definition);
        } catch (FluxRuntimeException failure) {
            throw failure;
        } catch (Exception failure) {
            throw PlanningException.sourcePreparationFailed(
                    definition.getSource().getType(),
                    failure);
        }
    }

    private PreparedJob prepareSink(
            JobDefinition definition,
            PreparedSource<?> preparedSource)
            throws Exception {

        try {
            return connectorPreparer.prepare(
                    definition,
                    preparedSource);
        } catch (FluxRuntimeException failure) {
            throw failure;
        } catch (Exception failure) {
            throw PlanningException.sinkPreparationFailed(
                    definition.getSink().getType(),
                    failure);
        }
    }

    private JobGraph plan(
            JobDefinition definition,
            PreparedJob preparedJob)
            throws Exception {

        try {
            return jobPlanner.plan(preparedJob);
        } catch (FluxRuntimeException failure) {
            throw failure;
        } catch (RuntimeException failure) {
            throw PlanningException.physicalPlanningFailed(
                    failure);
        } catch (Exception failure) {
            throw PlanningException.splitDiscoveryFailed(
                    definition.getSource().getType(),
                    failure);
        }
    }

    private static LocalFluxEngine assemble(
            ClassLoader classLoader,
            FactoryRegistry registry) {

        return new LocalFluxEngine(
                classLoader,
                new ConnectorPreparer(
                        registry,
                        classLoader),
                new JobPlanner(),
                registry);
    }

    private static ClassLoader effectiveClassLoader(
            ClassLoader classLoader) {

        return classLoader == null
                ? Thread.currentThread()
                        .getContextClassLoader()
                : classLoader;
    }

    private static CloseableThreadContext.Instance openLogContext(
            JobDefinition definition,
            String runId,
            String jobLogFile) {

        return CloseableThreadContext
                .put("runId", runId)
                .put("jobId", runId)
                .put("jobName", definition.getName())
                .put("jobLogFile", jobLogFile);
    }

    private static void notifyJobLogCreated(
            JobExecutionListener listener,
            String runId,
            String jobLogFile) {

        if (listener != null) {
            listener.onJobLogCreated(
                    runId,
                    jobLogFile);
        }
    }

    private static void notifyJobExecutionCreated(
            JobExecutionListener listener,
            JobExecution execution) {

        if (listener != null) {
            listener.onJobExecutionCreated(execution);
        }
    }

    private void closeRegistry() {
        if (registry != null) {
            registry.close();
        }
    }
}
