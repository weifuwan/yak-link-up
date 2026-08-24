package com.link.up.framework.execution;

import com.link.up.api.sink.TableDdl;
import com.link.up.api.source.SourceSplit;
import com.link.up.api.table.catalog.CatalogTable;
import com.link.up.framework.connector.PreparedSink;
import com.link.up.framework.job.CommitSummary;
import com.link.up.framework.job.PipelineResult;
import com.link.up.framework.job.PipelineStatus;
import com.link.up.framework.planner.PipelineGraph;
import com.link.up.framework.planner.SinkTaskPlan;
import com.link.up.framework.planner.SourceTaskPlan;

import java.util.Objects;

/** Converts one pipeline execution outcome into the stable result model. */
final class PipelineResultFactory {

    private PipelineResultFactory() {
    }

    static <SplitT extends SourceSplit> PipelineResult create(
            PipelineGraph<SplitT> graph,
            PipelineStatus status,
            CommitSummary commitSummary,
            Throwable failure) {

        Objects.requireNonNull(graph, "graph must not be null");
        Objects.requireNonNull(status, "status must not be null");
        Objects.requireNonNull(
                commitSummary,
                "commitSummary must not be null");

        SourceTaskPlan<SplitT> sourceTaskPlan =
                graph.getSourceTaskPlans().get(0);
        SinkTaskPlan sinkTaskPlan =
                graph.getSinkTaskPlans().get(0);
        PreparedSink preparedSink =
                sinkTaskPlan.getPreparedSink();

        CatalogTable targetTable =
                preparedSink.getMetadata()
                        .getTargetTable(graph.getDataSetPath());
        TableDdl tableDdl =
                preparedSink.getMetadata()
                        .getTableDdl(graph.getDataSetPath());

        return new PipelineResult(
                graph.getPipelineId(),
                graph.getDataSetId(),
                sourceTaskPlan.getPreparedSource()
                        .getFactoryIdentifier(),
                graph.getDataSetPath().toString(),
                graph.getSourceTaskPlans().size(),
                preparedSink.getFactoryIdentifier(),
                targetTablePath(targetTable),
                graph.getSinkTaskPlans().size(),
                tableDdl,
                status,
                commitSummary,
                failure);
    }

    private static String targetTablePath(CatalogTable targetTable) {
        if (targetTable == null || targetTable.getTablePath() == null) {
            return "-";
        }

        return targetTable.getTablePath().toString();
    }
}
