package com.link.up.framework.planner;

import com.link.up.api.source.SourceSplit;
import com.link.up.api.table.catalog.CatalogTable;
import com.link.up.api.table.catalog.TablePath;
import com.link.up.framework.connector.PreparedJob;
import com.link.up.framework.connector.PreparedSink;
import com.link.up.framework.connector.PreparedSource;
import com.link.up.framework.execution.TaskId;
import com.link.up.framework.execution.TaskType;
import com.link.up.framework.job.ExecutionConfig;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Builds an immutable {@link JobGraph} grouped by SourceSplit.dataSetId.
 *
 * <p>The planner calculates topology and split assignment only. Runtime
 * ownership objects such as split queues are created later by the execution
 * layer.
 */
public final class JobPlanner {

    private final SplitAssigner splitAssigner;

    public JobPlanner() {
        this(new SplitAssigner());
    }

    public JobPlanner(SplitAssigner splitAssigner) {
        this.splitAssigner = Objects.requireNonNull(
                splitAssigner,
                "splitAssigner must not be null");
    }

    public JobGraph plan(PreparedJob preparedJob) throws Exception {
        PreparedJob job = Objects.requireNonNull(
                preparedJob,
                "preparedJob must not be null");
        return createGraph(job, job.getSource());
    }

    private <SplitT extends SourceSplit> JobGraph createGraph(
            PreparedJob job,
            PreparedSource<SplitT> preparedSource)
            throws Exception {

        ExecutionConfig config = job.getExecutionConfig();
        List<SplitT> splits = preparedSource
                .getSource()
                .createSplits(
                        preparedSource.getTables(),
                        config.getSourceParallelism());

        if (splits == null) {
            throw new IllegalStateException(
                    "Source returned null splits");
        }

        Map<String, List<SplitT>> byDataSet =
                new LinkedHashMap<String, List<SplitT>>();

        for (SplitT split : splits) {
            if (split == null) {
                throw new IllegalStateException(
                        "Source returned a null split");
            }

            String id = split.dataSetId();
            if (id == null || id.trim().isEmpty()) {
                throw new IllegalStateException(
                        "SourceSplit dataSetId must not be blank");
            }

            List<SplitT> group = byDataSet.get(id);
            if (group == null) {
                group = new ArrayList<SplitT>();
                byDataSet.put(id, group);
            }
            group.add(split);
        }

        List<PipelineGraph<?>> pipelines =
                new ArrayList<PipelineGraph<?>>();

        for (Map.Entry<String, List<SplitT>> entry :
                byDataSet.entrySet()) {

            String dataSetId = entry.getKey();
            TablePath path = TablePath.parse(dataSetId);
            CatalogTable table =
                    preparedSource.getOutputTables().get(path);

            if (table == null) {
                throw new IllegalStateException(
                        "No output catalog table for data set: "
                                + dataSetId);
            }

            List<List<SplitT>> assignments =
                    splitAssigner.assign(
                            entry.getValue(),
                            config.getSourceParallelism());

            String pipelineId = "pipeline-" + dataSetId;
            List<SourceTaskPlan<SplitT>> sources =
                    new ArrayList<SourceTaskPlan<SplitT>>();

            for (int i = 0; i < assignments.size(); i++) {
                sources.add(
                        new SourceTaskPlan<SplitT>(
                                new TaskId(
                                        pipelineId,
                                        TaskType.SOURCE,
                                        i,
                                        assignments.size()),
                                preparedSource,
                                assignments.get(i),
                                config.getBatchSize()));
            }

            List<PreparedSink> sinks = job.getSinks(dataSetId);
            int sinkCount = Math.min(
                    Math.min(
                            config.getSinkParallelism(),
                            sinks.size()),
                    Math.max(1, entry.getValue().size()));

            List<SinkTaskPlan> sinkPlans =
                    new ArrayList<SinkTaskPlan>();

            for (int i = 0; i < sinkCount; i++) {
                sinkPlans.add(
                        new SinkTaskPlan(
                                new TaskId(
                                        pipelineId,
                                        TaskType.SINK,
                                        i,
                                        sinkCount),
                                sinks.get(i)));
            }

            pipelines.add(
                    new PipelineGraph<SplitT>(
                            pipelineId,
                            dataSetId,
                            table,
                            entry.getValue(),
                            sources,
                            sinkPlans,
                            config.getSplitAssignmentMode()));
        }

        return new JobGraph(
                job.getJobName(),
                config,
                pipelines);
    }
}
