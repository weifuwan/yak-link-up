package com.link.up.server.http;

import com.link.up.framework.connector.schema.ConnectorSchemaCatalog;
import com.link.up.server.config.FluxServerConfig;
import com.link.up.server.http.servlet.ConnectorsServlet;
import com.link.up.server.http.servlet.HealthServlet;
import com.link.up.server.http.servlet.JobResourceServlet;
import com.link.up.server.http.servlet.JobsServlet;
import com.link.up.server.http.servlet.NodeServlet;
import com.link.up.server.http.servlet.NotFoundServlet;
import com.link.up.server.service.ConnectorRestService;
import com.link.up.server.service.JobRestService;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.servlet.FilterHolder;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.util.thread.QueuedThreadPool;

import javax.servlet.DispatcherType;
import java.util.Collections;
import java.util.EnumSet;
import java.util.concurrent.atomic.AtomicBoolean;

/** Embedded Jetty lifecycle and route composition. */
public final class JettyServer implements AutoCloseable {

    private final Server server;
    private final ServerConnector connector;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    public JettyServer(
            FluxServerConfig config,
            JobRestService jobService) {
        this(
                config,
                jobService,
                new ConnectorRestService(
                        new ConnectorSchemaCatalog(Collections.emptyList())));
    }

    public JettyServer(
            FluxServerConfig config,
            JobRestService jobService,
            ConnectorRestService connectorService) {

        int minimumThreads = Math.min(4, config.getHttpThreads());
        QueuedThreadPool threadPool = new QueuedThreadPool(
                config.getHttpThreads(),
                minimumThreads,
                60_000);
        threadPool.setName("link-up-http");

        this.server = new Server(threadPool);
        this.server.setStopTimeout(config.getShutdownTimeoutMillis());

        this.connector = new ServerConnector(server);
        connector.setHost(config.getHost());
        connector.setPort(config.getPort());
        connector.setIdleTimeout(30_000L);
        connector.setAcceptQueueSize(128);
        server.addConnector(connector);

        ServletContextHandler context = new ServletContextHandler(
                ServletContextHandler.NO_SESSIONS);
        context.setContextPath("/");

        context.addFilter(
                new FilterHolder(new ExceptionHandlingFilter()),
                "/*",
                EnumSet.of(DispatcherType.REQUEST));

        context.addServlet(
                new ServletHolder(new HealthServlet()),
                RestConstants.HEALTH);
        context.addServlet(
                new ServletHolder(new NodeServlet(jobService)),
                RestConstants.NODE);
        context.addServlet(
                new ServletHolder(new ConnectorsServlet(connectorService)),
                RestConstants.CONNECTORS + "/*");
        context.addServlet(
                new ServletHolder(
                        new JobsServlet(
                                jobService,
                                config.getMaxRequestBytes())),
                RestConstants.JOBS);
        context.addServlet(
                new ServletHolder(
                        new JobResourceServlet(
                                jobService,
                                config.getMaxRequestBytes())),
                RestConstants.JOBS + "/*");
        context.addServlet(
                new ServletHolder(new NotFoundServlet()),
                "/");

        server.setHandler(context);
    }

    public void start() throws Exception {
        server.start();
    }

    public void join() throws InterruptedException {
        server.join();
    }

    public int getLocalPort() {
        return connector.getLocalPort();
    }

    public boolean isStarted() {
        return server.isStarted();
    }

    public void stop() throws Exception {
        if (closed.compareAndSet(false, true)) {
            server.stop();
        }
    }

    public void close() {
        try {
            stop();
        } catch (Exception exception) {
            throw new IllegalStateException(
                    "Failed to stop Jetty server",
                    exception);
        }
    }
}
