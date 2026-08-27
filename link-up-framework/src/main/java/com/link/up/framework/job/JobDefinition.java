package com.link.up.framework.job;

import java.util.Objects;

/** User-submitted, normalized synchronization Job definition. */
public final class JobDefinition {

    private final String name;
    private final SourceDefinition source;
    private final SinkDefinition sink;
    private final ExecutionConfig executionConfig;
    private final ColumnMapping columnMapping;
    private final JobCapabilityRequirements capabilityRequirements;

    public JobDefinition(
            String name,
            SourceDefinition source,
            SinkDefinition sink,
            ExecutionConfig executionConfig) {

        this(
                name,
                source,
                sink,
                executionConfig,
                ColumnMapping.empty(),
                JobCapabilityRequirements.empty());
    }

    public JobDefinition(
            String name,
            SourceDefinition source,
            SinkDefinition sink,
            ExecutionConfig executionConfig,
            ColumnMapping columnMapping) {

        this(
                name,
                source,
                sink,
                executionConfig,
                columnMapping,
                JobCapabilityRequirements.empty());
    }

    public JobDefinition(
            String name,
            SourceDefinition source,
            SinkDefinition sink,
            ExecutionConfig executionConfig,
            ColumnMapping columnMapping,
            JobCapabilityRequirements capabilityRequirements) {

        this.name = requireName(name);
        this.source = Objects.requireNonNull(
                source,
                "source must not be null");
        this.sink = Objects.requireNonNull(
                sink,
                "sink must not be null");
        this.executionConfig = Objects.requireNonNull(
                executionConfig,
                "executionConfig must not be null");
        this.columnMapping = columnMapping == null
                ? ColumnMapping.empty()
                : columnMapping;
        this.capabilityRequirements =
                capabilityRequirements == null
                        ? JobCapabilityRequirements.empty()
                        : capabilityRequirements;
    }

    private static String requireName(String name) {
        Objects.requireNonNull(
                name,
                "name must not be null");

        String normalized = name.trim();
        if (normalized.isEmpty()) {
            throw new IllegalArgumentException(
                    "name must not be blank");
        }
        return normalized;
    }

    public String getName() {
        return name;
    }

    public SourceDefinition getSource() {
        return source;
    }

    public SinkDefinition getSink() {
        return sink;
    }

    public ExecutionConfig getExecutionConfig() {
        return executionConfig;
    }

    public ColumnMapping getColumnMapping() {
        return columnMapping;
    }

    public JobCapabilityRequirements getCapabilityRequirements() {
        return capabilityRequirements;
    }
}
