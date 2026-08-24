package com.link.up.server;

import com.link.up.framework.connector.FactoryRegistry;
import com.link.up.framework.connector.schema.ConnectorSchemaCatalog;
import com.link.up.server.application.JobApplication;
import com.link.up.server.application.JobApplicationService;
import com.link.up.server.application.port.JobExecutor;
import com.link.up.server.application.port.JobIdGenerator;
import com.link.up.server.application.port.JobRepository;
import com.link.up.server.application.port.JobRuntimeScheduler;
import com.link.up.server.config.FluxServerConfig;
import com.link.up.server.http.JettyServer;
import com.link.up.server.infrastructure.execution.LocalJobExecutor;
import com.link.up.server.infrastructure.identity.LocalJobIdGenerator;
import com.link.up.server.infrastructure.persistence.FileJobRepository;
import com.link.up.server.infrastructure.runtime.LocalJobRuntimeScheduler;
import com.link.up.server.registration.ControlPlaneRegistrationAgent;
import com.link.up.server.registration.ControlPlaneRegistrationConfig;
import com.link.up.server.runtime.WorkerIdentity;
import com.link.up.server.service.ConnectorRestService;
import com.link.up.server.service.JobRestService;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/** Link-Up single-node offline Worker composition root. */
public final class FluxServer {

    private static final String LOG_FILE_PROPERTY = "link.up.log.file";

    static {
        configureDefaultLogFile();
    }

    private static final Logger LOG = LogManager.getLogger(FluxServer.class);

    private FluxServer() {
    }

    public static void main(String[] args)
            throws Exception {

        final FluxServerConfig config = FluxServerConfig.fromArgs(args);

        List<Path> pluginDirectories = config.getPluginDirectories();
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        Path[] pluginPaths = pluginDirectories.toArray(
                new Path[pluginDirectories.size()]);

        JobExecutor jobExecutor =
                new LocalJobExecutor(classLoader, pluginPaths);
        JobRepository repository =
                new FileJobRepository(
                        config.getStateDirectory(),
                        config.getHistoryLimit());
        JobIdGenerator jobIdGenerator = new LocalJobIdGenerator();
        JobRuntimeScheduler runtimeScheduler =
                new LocalJobRuntimeScheduler(
                        config.getMaxQueuedJobs(),
                        config.getShutdownTimeoutMillis(),
                        jobExecutor);

        final JobApplication jobApplication =
                new JobApplicationService(
                        runtimeScheduler,
                        repository,
                        jobIdGenerator);

        WorkerIdentity workerIdentity =
                new WorkerIdentity(
                        config.getNodeId(),
                        config.getNodeName(),
                        WorkerIdentity.implementationVersion());

        JobRestService jobService =
                new JobRestService(
                        jobApplication,
                        workerIdentity,
                        config.getJobThreads(),
                        config.getMaxQueuedJobs());

        final FactoryRegistry connectorRegistry =
                FactoryRegistry.discover(classLoader, pluginPaths);
        ConnectorSchemaCatalog connectorCatalog =
                ConnectorSchemaCatalog.fromRegistry(connectorRegistry);
        ConnectorRestService connectorService =
                new ConnectorRestService(
                        connectorCatalog,
                        connectorRegistry);

        final JettyServer server =
                new JettyServer(config, jobService, connectorService);

        final ControlPlaneRegistrationConfig registrationConfig =
                ControlPlaneRegistrationConfig.load();
        final ControlPlaneRegistrationAgent registrationAgent =
                new ControlPlaneRegistrationAgent(
                        registrationConfig,
                        jobService,
                        connectorCatalog);
        final AtomicBoolean shutdown = new AtomicBoolean(false);

        final Runnable shutdownAction =
                new Runnable() {
                    @Override
                    public void run() {
                        if (!shutdown.compareAndSet(false, true)) {
                            return;
                        }
                        try {
                            registrationAgent.close();
                        } catch (RuntimeException exception) {
                            LOG.warn(
                                    "Failed to close control-plane registration agent",
                                    exception);
                        }
                        try {
                            server.stop();
                        } catch (Exception exception) {
                            LOG.warn(
                                    "Failed to stop HTTP server",
                                    exception);
                        }
                        jobApplication.close();
                        try {
                            connectorRegistry.close();
                        } catch (RuntimeException exception) {
                            LOG.warn(
                                    "Failed to close connector registry",
                                    exception);
                        }
                    }
                };

        Thread shutdownHook =
                new Thread(shutdownAction, "link-up-shutdown");
        Runtime.getRuntime().addShutdownHook(shutdownHook);

        try {
            server.start();
            registrationAgent.start();

            LOG.info(
                    "Link-Up Offline Worker started, nodeId={}, instanceId={}, host={}, port={}, jobThreads={}, stateDirectory={}, connectorSchemas={}, pluginDirectories={}, dynamicRegistration={}",
                    workerIdentity.getNodeId(),
                    workerIdentity.getInstanceId(),
                    config.getHost(),
                    server.getLocalPort(),
                    config.getJobThreads(),
                    config.getStateDirectory(),
                    connectorCatalog.list().size(),
                    config.getPluginDirectories(),
                    registrationConfig.isEnabled());

            server.join();
        } finally {
            shutdownAction.run();
            try {
                Runtime.getRuntime().removeShutdownHook(shutdownHook);
            } catch (IllegalStateException ignored) {
                // JVM shutdown already started.
            }
        }
    }

    private static void configureDefaultLogFile() {
        if (hasText(System.getProperty(LOG_FILE_PROPERTY))
                || hasText(System.getenv("LOGFILE"))) {
            return;
        }
        String logDirectory = System.getProperty("link.up.log.dir");
        if (!hasText(logDirectory)) {
            logDirectory = System.getenv("LINK_UP_LOG_DIR");
        }
        if (!hasText(logDirectory)) {
            logDirectory = "logs";
        }
        System.setProperty(
                LOG_FILE_PROPERTY,
                Paths.get(logDirectory, "link-up-server.log").toString());
    }

    private static boolean hasText(String value) {
        return value != null && !value.trim().isEmpty();
    }
}
