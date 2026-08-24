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
import java.util.List;
import java.util.Objects;

/** Builds one immutable physical pipeline for a single logical data set. */
final class PipelinePlanner {

    private final SplitAssigner splitAssigner;

    PipelinePlanner(SplitAssigner splitAssigner) {
        this.splitAssigner = Objects.requireNonNull(
                splitAssigner,
                "splitAssigner must not be null");
    }

    <SplitT extends SourceSplit> PipelineGraph<SplitT> plan(
            String dataSetId,
            List<SplitT> splits,
            PreparedJob job,
            PreparedSource<SplitT> preparedSource) {

        Objects.requireNonNull(
                dataSetId,
                "dataSetId must not be null");
        Objects.requireNonNull(
                splits,
                "splits must not be null");
        Objects.requireNonNull(
                job,
                "job must not be null");
        Objects.requireNonNull(
                preparedSource,
                "preparedSource must not be null");

        ExecutionConfig config = job.getExecutionConfig();
        TablePath dataSetPath = TablePath.parse(dataSetId);
        CatalogTable table =
                preparedSource.getOutputTables()
                        .get(dataSetPath);

        if (table == null) {
            throw new IllegalStateException(
                    "No output catalog table for data set: "
                            + dataSetId);
        }

        String pipelineId = "pipeline-" + dataSetId;

        return new PipelineGraph<SplitT>(
                pipelineId,
                dataSetId,
                table,
                splits,
                createSourceTaskPlans(
                        pipelineId,
                        splits,
                        preparedSource,
                        config),
                createSinkTaskPlans(
                        pipelineId,
                        dataSetId,
                        splits.size(),
                        job,
                        config),
                config.getSplitAssignmentMode());
    }

    private <SplitT extends SourceSplit>
    List<SourceTaskPlan<SplitT>> createSourceTaskPlans(
            String pipelineId,
            List<SplitT> splits,
            PreparedSource<SplitT> preparedSource,
            ExecutionConfig config) {

        List<List<SplitT>> assignments =
                splitAssigner.assign(
                        splits,
                        config.getSourceParallelism());

        List<SourceTaskPlan<SplitT>> sourcePlans =
                new ArrayList<SourceTaskPlan<SplitT>>(
                        assignments.size());

        for (int index = 0;
             index < assignments.size();
             index++) {

            sourcePlans.add(
                    new SourceTaskPlan<SplitT>(
                            new TaskId(
                                    pipelineId,
                                    TaskType.SOURCE,
                                    index,
                                    assignments.size()),
                            preparedSource,
                            assignments.get(index),
                            config.getBatchSize()));
        }

        return sourcePlans;
    }

    private List<SinkTaskPlan> createSinkTaskPlans(
            String pipelineId,
            String dataSetId,
            int splitCount,
            PreparedJob job,
            ExecutionConfig config) {

        List<PreparedSink> sinks = job.getSinks(dataSetId);

        int sinkCount =
                Math.min(
                        Math.min(
                                config.getSinkParallelism(),
                                sinks.size()),
                        Math.max(1, splitCount));

        List<SinkTaskPlan> sinkPlans =
                new ArrayList<SinkTaskPlan>(sinkCount);

        for (int index = 0;
             index < sinkCount;
             index++) {

            sinkPlans.add(
                    new SinkTaskPlan(
                            new TaskId(
                                    pipelineId,
                                    TaskType.SINK,
                                    index,
                                    sinkCount),
                            sinks.get(index)));
        }

        return sinkPlans;
    }
}
