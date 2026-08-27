package com.link.up.framework.planning;

import com.link.up.framework.planner.JobGraph;
import com.link.up.framework.planner.PipelineGraph;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

/**
 * Secret-safe serializable projection of the runtime JobGraph.
 *
 * <p>This class intentionally stores only identities and counts. Prepared
 * connectors, options, ClassLoaders and runtime ownership objects remain in
 * the execution graph.</p>
 */
public final class PhysicalJobPlan {

    private final String jobName;
    private final String fingerprint;
    private final int pipelineCount;
    private final int sourceTaskCount;
    private final int sinkTaskCount;
    private final List<Pipeline> pipelines;

    private PhysicalJobPlan(
            String jobName,
            String fingerprint,
            List<Pipeline> pipelines) {

        this.jobName = requireText(jobName, "jobName");
        this.fingerprint = requireText(
                fingerprint,
                "fingerprint");
        this.pipelines = Collections.unmodifiableList(
                new ArrayList<Pipeline>(pipelines));
        this.pipelineCount = pipelines.size();

        int sources = 0;
        int sinks = 0;
        for (Pipeline pipeline : pipelines) {
            sources += pipeline.getSourceTaskCount();
            sinks += pipeline.getSinkTaskCount();
        }
        this.sourceTaskCount = sources;
        this.sinkTaskCount = sinks;
    }

    public static PhysicalJobPlan from(
            JobGraph jobGraph,
            String fingerprint) {

        JobGraph graph = Objects.requireNonNull(
                jobGraph,
                "jobGraph must not be null");
        List<Pipeline> pipelines =
                new ArrayList<Pipeline>();

        for (PipelineGraph<?> pipeline :
                graph.getPipelineGraphs()) {
            pipelines.add(Pipeline.from(pipeline));
        }

        Collections.sort(
                pipelines,
                new Comparator<Pipeline>() {
                    @Override
                    public int compare(
                            Pipeline left,
                            Pipeline right) {
                        return left.getPipelineId()
                                .compareTo(right.getPipelineId());
                    }
                });

        return new PhysicalJobPlan(
                graph.getJobName(),
                fingerprint,
                pipelines);
    }

    public String getJobName() {
        return jobName;
    }

    public String getFingerprint() {
        return fingerprint;
    }

    public int getPipelineCount() {
        return pipelineCount;
    }

    public int getSourceTaskCount() {
        return sourceTaskCount;
    }

    public int getSinkTaskCount() {
        return sinkTaskCount;
    }

    public List<Pipeline> getPipelines() {
        return pipelines;
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

    public static final class Pipeline {

        private final String pipelineId;
        private final String dataSetId;
        private final String sourceConnectorId;
        private final String sinkConnectorId;
        private final int sourceSplitCount;
        private final int sourceTaskCount;
        private final int sinkTaskCount;
        private final int outputColumnCount;
        private final String splitAssignmentMode;

        private Pipeline(
                String pipelineId,
                String dataSetId,
                String sourceConnectorId,
                String sinkConnectorId,
                int sourceSplitCount,
                int sourceTaskCount,
                int sinkTaskCount,
                int outputColumnCount,
                String splitAssignmentMode) {

            this.pipelineId = requireText(
                    pipelineId,
                    "pipelineId");
            this.dataSetId = requireText(
                    dataSetId,
                    "dataSetId");
            this.sourceConnectorId = requireText(
                    sourceConnectorId,
                    "sourceConnectorId");
            this.sinkConnectorId = requireText(
                    sinkConnectorId,
                    "sinkConnectorId");
            this.sourceSplitCount = sourceSplitCount;
            this.sourceTaskCount = sourceTaskCount;
            this.sinkTaskCount = sinkTaskCount;
            this.outputColumnCount = outputColumnCount;
            this.splitAssignmentMode = requireText(
                    splitAssignmentMode,
                    "splitAssignmentMode");
        }

        private static Pipeline from(
                PipelineGraph<?> graph) {

            if (graph.getSourceTaskPlans().isEmpty()
                    || graph.getSinkTaskPlans().isEmpty()) {
                throw new IllegalArgumentException(
                        "Pipeline tasks must not be empty");
            }

            return new Pipeline(
                    graph.getPipelineId(),
                    graph.getDataSetId(),
                    graph.getSourceTaskPlans()
                            .get(0)
                            .getPreparedSource()
                            .getFactoryIdentifier(),
                    graph.getSinkTaskPlans()
                            .get(0)
                            .getPreparedSink()
                            .getFactoryIdentifier(),
                    graph.getSourceSplits().size(),
                    graph.getSourceTaskPlans().size(),
                    graph.getSinkTaskPlans().size(),
                    graph.getCatalogTable()
                            .getTableSchema()
                            .getColumnCount(),
                    graph.getSplitAssignmentMode().name());
        }

        public String getPipelineId() {
            return pipelineId;
        }

        public String getDataSetId() {
            return dataSetId;
        }

        public String getSourceConnectorId() {
            return sourceConnectorId;
        }

        public String getSinkConnectorId() {
            return sinkConnectorId;
        }

        public int getSourceSplitCount() {
            return sourceSplitCount;
        }

        public int getSourceTaskCount() {
            return sourceTaskCount;
        }

        public int getSinkTaskCount() {
            return sinkTaskCount;
        }

        public int getOutputColumnCount() {
            return outputColumnCount;
        }

        public String getSplitAssignmentMode() {
            return splitAssignmentMode;
        }
    }
}
