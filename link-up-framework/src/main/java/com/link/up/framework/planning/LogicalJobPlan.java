package com.link.up.framework.planning;

import com.link.up.api.connector.schema.ConnectorCapability;
import com.link.up.framework.job.ColumnMapping;
import com.link.up.framework.job.ExecutionConfig;
import com.link.up.framework.job.JobCapabilityRequirements;
import com.link.up.framework.job.JobDefinition;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Immutable, Secret-safe representation of the submitted Job intent.
 *
 * <p>Connector option values deliberately remain outside this model. Their
 * complete canonical form contributes to the fingerprint, so configuration
 * changes are still detectable without making credentials observable.</p>
 */
public final class LogicalJobPlan {

    private final String jobName;
    private final String sourceConnectorId;
    private final String sinkConnectorId;
    private final RuntimeSettings runtime;
    private final List<Column> columns;
    private final CapabilityIntent capabilities;
    private final String fingerprint;

    private LogicalJobPlan(
            String jobName,
            String sourceConnectorId,
            String sinkConnectorId,
            RuntimeSettings runtime,
            List<Column> columns,
            CapabilityIntent capabilities,
            String fingerprint) {

        this.jobName = requireText(jobName, "jobName");
        this.sourceConnectorId = requireText(
                sourceConnectorId,
                "sourceConnectorId");
        this.sinkConnectorId = requireText(
                sinkConnectorId,
                "sinkConnectorId");
        this.runtime = Objects.requireNonNull(
                runtime,
                "runtime must not be null");
        this.columns = Collections.unmodifiableList(
                new ArrayList<Column>(
                        Objects.requireNonNull(
                                columns,
                                "columns must not be null")));
        this.capabilities = Objects.requireNonNull(
                capabilities,
                "capabilities must not be null");
        this.fingerprint = requireText(
                fingerprint,
                "fingerprint");
    }

    public static LogicalJobPlan from(
            JobDefinition definition) {

        JobDefinition job = Objects.requireNonNull(
                definition,
                "definition must not be null");
        List<Column> columns =
                new ArrayList<Column>();
        ColumnMapping mapping =
                job.getColumnMapping();

        if (mapping != null) {
            for (ColumnMapping.Item item :
                    mapping.getColumns()) {
                columns.add(
                        new Column(
                                item.getSource(),
                                item.getTarget()));
            }
        }

        return new LogicalJobPlan(
                job.getName(),
                job.getSource().getType(),
                job.getSink().getType(),
                RuntimeSettings.from(
                        job.getExecutionConfig()),
                columns,
                CapabilityIntent.from(
                        job.getCapabilityRequirements()),
                PlanFingerprint.create(job));
    }

    public String getJobName() {
        return jobName;
    }

    public String getSourceConnectorId() {
        return sourceConnectorId;
    }

    public String getSinkConnectorId() {
        return sinkConnectorId;
    }

    public RuntimeSettings getRuntime() {
        return runtime;
    }

    public List<Column> getColumns() {
        return columns;
    }

    public CapabilityIntent getCapabilities() {
        return capabilities;
    }

    public String getFingerprint() {
        return fingerprint;
    }

    private static String requireText(
            String value,
            String name) {

        if (value == null || value.trim().isEmpty()) {
            throw new IllegalArgumentException(
                    name + " must not be blank");
        }
        return value.trim();
    }

    /** Secret-free runtime intent after defaults have been resolved. */
    public static final class RuntimeSettings {

        private final int batchSize;
        private final int sourceParallelism;
        private final int sinkParallelism;
        private final int pipelineParallelism;
        private final int maxBufferedBatches;
        private final long maxBufferedRecords;
        private final long maxBufferedBytes;
        private final long maxRecordsPerSecond;
        private final long maxBytesPerSecond;
        private final String sinkPartitionStrategy;
        private final String splitAssignmentMode;

        private RuntimeSettings(
                int batchSize,
                int sourceParallelism,
                int sinkParallelism,
                int pipelineParallelism,
                int maxBufferedBatches,
                long maxBufferedRecords,
                long maxBufferedBytes,
                long maxRecordsPerSecond,
                long maxBytesPerSecond,
                String sinkPartitionStrategy,
                String splitAssignmentMode) {

            this.batchSize = batchSize;
            this.sourceParallelism = sourceParallelism;
            this.sinkParallelism = sinkParallelism;
            this.pipelineParallelism = pipelineParallelism;
            this.maxBufferedBatches = maxBufferedBatches;
            this.maxBufferedRecords = maxBufferedRecords;
            this.maxBufferedBytes = maxBufferedBytes;
            this.maxRecordsPerSecond = maxRecordsPerSecond;
            this.maxBytesPerSecond = maxBytesPerSecond;
            this.sinkPartitionStrategy = sinkPartitionStrategy;
            this.splitAssignmentMode = splitAssignmentMode;
        }

        private static RuntimeSettings from(
                ExecutionConfig config) {

            return new RuntimeSettings(
                    config.getBatchSize(),
                    config.getSourceParallelism(),
                    config.getSinkParallelism(),
                    config.getPipelineParallelism(),
                    config.getMaxBufferedBatches(),
                    config.getMaxBufferedRecords(),
                    config.getMaxBufferedBytes(),
                    config.getMaxRecordsPerSecond(),
                    config.getMaxBytesPerSecond(),
                    config.getSinkPartitionStrategy().name(),
                    config.getSplitAssignmentMode().name());
        }

        public int getBatchSize() {
            return batchSize;
        }

        public int getSourceParallelism() {
            return sourceParallelism;
        }

        public int getSinkParallelism() {
            return sinkParallelism;
        }

        public int getPipelineParallelism() {
            return pipelineParallelism;
        }

        public int getMaxBufferedBatches() {
            return maxBufferedBatches;
        }

        public long getMaxBufferedRecords() {
            return maxBufferedRecords;
        }

        public long getMaxBufferedBytes() {
            return maxBufferedBytes;
        }

        public long getMaxRecordsPerSecond() {
            return maxRecordsPerSecond;
        }

        public long getMaxBytesPerSecond() {
            return maxBytesPerSecond;
        }

        public String getSinkPartitionStrategy() {
            return sinkPartitionStrategy;
        }

        public String getSplitAssignmentMode() {
            return splitAssignmentMode;
        }
    }

    public static final class CapabilityIntent {

        private final Endpoint source;
        private final Endpoint sink;

        private CapabilityIntent(
                Endpoint source,
                Endpoint sink) {
            this.source = source;
            this.sink = sink;
        }

        private static CapabilityIntent from(
                JobCapabilityRequirements requirements) {

            JobCapabilityRequirements safe =
                    requirements == null
                            ? JobCapabilityRequirements.empty()
                            : requirements;

            return new CapabilityIntent(
                    new Endpoint(
                            safe.getSourceRequired(),
                            safe.getSourcePreferred()),
                    new Endpoint(
                            safe.getSinkRequired(),
                            safe.getSinkPreferred()));
        }

        public Endpoint getSource() {
            return source;
        }

        public Endpoint getSink() {
            return sink;
        }
    }

    public static final class Endpoint {

        private final List<ConnectorCapability> required;
        private final List<ConnectorCapability> preferred;

        private Endpoint(
                Collection<ConnectorCapability> required,
                Collection<ConnectorCapability> preferred) {

            this.required = immutable(required);
            this.preferred = immutable(preferred);
        }

        public List<ConnectorCapability> getRequired() {
            return required;
        }

        public List<ConnectorCapability> getPreferred() {
            return preferred;
        }

        private static List<ConnectorCapability> immutable(
                Collection<ConnectorCapability> values) {
            return Collections.unmodifiableList(
                    new ArrayList<ConnectorCapability>(values));
        }
    }

    public static final class Column {

        private final String source;
        private final String target;

        private Column(
                String source,
                String target) {
            this.source = requireText(source, "source");
            this.target = requireText(target, "target");
        }

        public String getSource() {
            return source;
        }

        public String getTarget() {
            return target;
        }
    }
}
