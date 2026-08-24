package com.link.up.server.config;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** Link-Up Server startup configuration. */
public final class FluxServerConfig {

    public static final String DEFAULT_HOST = "0.0.0.0";
    public static final int DEFAULT_PORT = 18080;
    public static final int DEFAULT_HTTP_THREADS = 32;
    public static final int DEFAULT_MAX_QUEUED_JOBS = 100;
    public static final int DEFAULT_HISTORY_LIMIT = 1_000;
    public static final int DEFAULT_MAX_REQUEST_BYTES = 1024 * 1024;
    public static final long DEFAULT_SHUTDOWN_TIMEOUT_MILLIS = 10_000L;
    public static final String DEFAULT_NODE_ID = "link-up-node-1";
    public static final String DEFAULT_NODE_NAME = "Link-Up Offline Worker";
    public static final String DEFAULT_STATE_DIRECTORY = "data/worker-state";

    private final String host;
    private final int port;
    private final int jobThreads;
    private final int httpThreads;
    private final int maxQueuedJobs;
    private final int historyLimit;
    private final int maxRequestBytes;
    private final long shutdownTimeoutMillis;
    private final List<Path> pluginDirectories;
    private final String nodeId;
    private final String nodeName;
    private final Path stateDirectory;

    public FluxServerConfig(
            String host,
            int port,
            int jobThreads,
            int httpThreads,
            int maxQueuedJobs,
            int historyLimit,
            int maxRequestBytes,
            long shutdownTimeoutMillis,
            List<Path> pluginDirectories) {

        this(
                host,
                port,
                jobThreads,
                httpThreads,
                maxQueuedJobs,
                historyLimit,
                maxRequestBytes,
                shutdownTimeoutMillis,
                pluginDirectories,
                DEFAULT_NODE_ID,
                DEFAULT_NODE_NAME,
                defaultStateDirectory());
    }

    public FluxServerConfig(
            String host,
            int port,
            int jobThreads,
            int httpThreads,
            int maxQueuedJobs,
            int historyLimit,
            int maxRequestBytes,
            long shutdownTimeoutMillis,
            List<Path> pluginDirectories,
            String nodeId,
            String nodeName) {

        this(
                host,
                port,
                jobThreads,
                httpThreads,
                maxQueuedJobs,
                historyLimit,
                maxRequestBytes,
                shutdownTimeoutMillis,
                pluginDirectories,
                nodeId,
                nodeName,
                defaultStateDirectory());
    }

    public FluxServerConfig(
            String host,
            int port,
            int jobThreads,
            int httpThreads,
            int maxQueuedJobs,
            int historyLimit,
            int maxRequestBytes,
            long shutdownTimeoutMillis,
            List<Path> pluginDirectories,
            String nodeId,
            String nodeName,
            Path stateDirectory) {

        this.host = requireText(host, "host");
        this.port = requireRange(port, 1, 65535, "port");
        this.jobThreads = requireRange(jobThreads, 1, Integer.MAX_VALUE, "jobThreads");
        this.httpThreads = requireRange(httpThreads, 4, Integer.MAX_VALUE, "httpThreads");
        this.maxQueuedJobs = requireRange(maxQueuedJobs, 1, Integer.MAX_VALUE, "maxQueuedJobs");
        this.historyLimit = requireRange(historyLimit, 1, Integer.MAX_VALUE, "historyLimit");
        this.maxRequestBytes = requireRange(maxRequestBytes, 1, Integer.MAX_VALUE, "maxRequestBytes");

        if (shutdownTimeoutMillis < 0L) {
            throw new IllegalArgumentException(
                    "shutdownTimeoutMillis must not be negative");
        }
        if (stateDirectory == null) {
            throw new IllegalArgumentException(
                    "stateDirectory must not be null");
        }

        this.shutdownTimeoutMillis = shutdownTimeoutMillis;
        List<Path> directories =
                pluginDirectories == null
                        ? Collections.<Path>emptyList()
                        : pluginDirectories;
        this.pluginDirectories = Collections.unmodifiableList(
                new ArrayList<Path>(directories));
        this.nodeId = requireText(nodeId, "nodeId");
        this.nodeName = requireText(nodeName, "nodeName");
        this.stateDirectory =
                stateDirectory.toAbsolutePath().normalize();
    }

    public static FluxServerConfig defaults() {
        int processors = Runtime.getRuntime().availableProcessors();
        return new FluxServerConfig(
                DEFAULT_HOST,
                DEFAULT_PORT,
                Math.max(2, processors),
                DEFAULT_HTTP_THREADS,
                DEFAULT_MAX_QUEUED_JOBS,
                DEFAULT_HISTORY_LIMIT,
                DEFAULT_MAX_REQUEST_BYTES,
                DEFAULT_SHUTDOWN_TIMEOUT_MILLIS,
                Collections.<Path>emptyList(),
                DEFAULT_NODE_ID,
                DEFAULT_NODE_NAME,
                defaultStateDirectory());
    }

    public static FluxServerConfig fromArgs(String[] args) {
        FluxServerConfig defaults = defaults();

        String host = defaults.getHost();
        int port = defaults.getPort();
        int jobThreads = defaults.getJobThreads();
        int httpThreads = defaults.getHttpThreads();
        int maxQueuedJobs = defaults.getMaxQueuedJobs();
        int historyLimit = defaults.getHistoryLimit();
        int maxRequestBytes = defaults.getMaxRequestBytes();
        long shutdownTimeout = defaults.getShutdownTimeoutMillis();
        String nodeId = defaults.getNodeId();
        String nodeName = defaults.getNodeName();
        Path stateDirectory = defaults.getStateDirectory();

        List<Path> pluginDirectories = new ArrayList<Path>();

        for (int index = 0; index < args.length; index++) {
            String argument = args[index];
            if (argument == null || !argument.startsWith("--")) {
                throw new IllegalArgumentException(
                        "Unknown argument: " + argument);
            }

            String option;
            String value;
            int equalIndex = argument.indexOf('=');
            if (equalIndex > 0) {
                option = argument.substring(0, equalIndex);
                value = argument.substring(equalIndex + 1);
            } else {
                option = argument;
                if (index + 1 >= args.length) {
                    throw new IllegalArgumentException(
                            "Missing value for " + option);
                }
                value = args[++index];
            }

            if ("--host".equals(option)) {
                host = value;
            } else if ("--port".equals(option)) {
                port = parseInt(option, value);
            } else if ("--job-threads".equals(option)) {
                jobThreads = parseInt(option, value);
            } else if ("--http-threads".equals(option)) {
                httpThreads = parseInt(option, value);
            } else if ("--max-queued-jobs".equals(option)) {
                maxQueuedJobs = parseInt(option, value);
            } else if ("--history-limit".equals(option)) {
                historyLimit = parseInt(option, value);
            } else if ("--max-request-bytes".equals(option)) {
                maxRequestBytes = parseInt(option, value);
            } else if ("--shutdown-timeout-millis".equals(option)) {
                shutdownTimeout = parseLong(option, value);
            } else if ("--plugin-dir".equals(option)) {
                addPluginDirectories(pluginDirectories, value);
            } else if ("--node-id".equals(option)) {
                nodeId = value;
            } else if ("--node-name".equals(option)) {
                nodeName = value;
            } else if ("--state-dir".equals(option)) {
                stateDirectory = path(option, value);
            } else {
                throw new IllegalArgumentException(
                        "Unknown option: " + option);
            }
        }

        return new FluxServerConfig(
                host,
                port,
                jobThreads,
                httpThreads,
                maxQueuedJobs,
                historyLimit,
                maxRequestBytes,
                shutdownTimeout,
                pluginDirectories,
                nodeId,
                nodeName,
                stateDirectory);
    }

    private static Path defaultStateDirectory() {
        return Paths.get(DEFAULT_STATE_DIRECTORY)
                .toAbsolutePath()
                .normalize();
    }

    private static Path path(String option, String value) {
        String normalized = requireText(value, option);
        return Paths.get(normalized)
                .toAbsolutePath()
                .normalize();
    }

    private static void addPluginDirectories(
            List<Path> directories,
            String value) {
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalArgumentException(
                    "plugin directory must not be blank");
        }
        String[] paths = value.split(",");
        for (String path : paths) {
            String normalized = path.trim();
            if (!normalized.isEmpty()) {
                directories.add(
                        Paths.get(normalized)
                                .toAbsolutePath()
                                .normalize());
            }
        }
    }

    private static int parseInt(String option, String value) {
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException exception) {
            throw new IllegalArgumentException(
                    option + " must be an integer: " + value);
        }
    }

    private static long parseLong(String option, String value) {
        try {
            return Long.parseLong(value);
        } catch (NumberFormatException exception) {
            throw new IllegalArgumentException(
                    option + " must be a long integer: " + value);
        }
    }

    private static String requireText(String value, String name) {
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalArgumentException(
                    name + " must not be blank");
        }
        return value.trim();
    }

    private static int requireRange(
            int value,
            int minimum,
            int maximum,
            String name) {
        if (value < minimum || value > maximum) {
            throw new IllegalArgumentException(
                    name + " must be between " + minimum + " and " + maximum);
        }
        return value;
    }

    public String getHost() { return host; }
    public int getPort() { return port; }
    public int getJobThreads() { return jobThreads; }
    public int getHttpThreads() { return httpThreads; }
    public int getMaxQueuedJobs() { return maxQueuedJobs; }
    public int getHistoryLimit() { return historyLimit; }
    public int getMaxRequestBytes() { return maxRequestBytes; }
    public long getShutdownTimeoutMillis() { return shutdownTimeoutMillis; }
    public List<Path> getPluginDirectories() { return pluginDirectories; }
    public String getNodeId() { return nodeId; }
    public String getNodeName() { return nodeName; }
    public Path getStateDirectory() { return stateDirectory; }
}
