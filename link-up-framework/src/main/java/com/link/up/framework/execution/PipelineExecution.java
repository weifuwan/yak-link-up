package com.link.up.framework.execution;

import com.link.up.api.sink.TableDdl;
import com.link.up.api.source.SourceSplit;
import com.link.up.api.table.catalog.CatalogTable;
import com.link.up.api.table.type.FluxRow;
import com.link.up.framework.channel.ChannelWriter;
import com.link.up.framework.channel.DataChannel;
import com.link.up.framework.channel.InputGate;
import com.link.up.framework.channel.LocalDataChannel;
import com.link.up.framework.channel.OutputGate;
import com.link.up.framework.channel.RecordEnvelope;
import com.link.up.framework.connector.PreparedSink;
import com.link.up.framework.execution.split.LocalSplitQueue;
import com.link.up.framework.execution.split.SplitProvider;
import com.link.up.framework.execution.task.ExecutionTask;
import com.link.up.framework.execution.task.SinkTask;
import com.link.up.framework.execution.task.SourceTask;
import com.link.up.framework.job.CommitSummary;
import com.link.up.framework.job.ExecutionConfig;
import com.link.up.framework.job.PipelineResult;
import com.link.up.framework.job.PipelineStatus;
import com.link.up.framework.job.SplitAssignmentMode;
import com.link.up.framework.metrics.JobMetrics;
import com.link.up.framework.planner.PipelineGraph;
import com.link.up.framework.planner.SinkTaskPlan;
import com.link.up.framework.planner.SourceTaskPlan;
import com.link.up.framework.routing.Partitioner;

import java.util.ArrayList;
import java.util.List;

/**
 * Runtime execution of one immutable PipelineGraph.
 *
 * <p>Mutable ownership state belongs here, not in the planner model. Dynamic
 * split assignment therefore creates one runtime-local SplitProvider shared by
 * the source tasks of this pipeline.
 */
final class PipelineExecution<SplitT extends SourceSplit> {

    private final PipelineGraph<SplitT> graph;
    private final ExecutionConfig config;
    private final CancellationToken token;
    private final JobMetrics metrics;
    private final ClassLoader loader;
    private final String jobName;
    private final long runId;
    private final SplitProvider<SplitT> splitProvider;

    PipelineExecution(
            PipelineGraph<SplitT> graph,
            ExecutionConfig config,
            CancellationToken token,
            JobMetrics metrics,
            ClassLoader loader,
            String jobName,
            long runId) {

        this.graph = graph;
        this.config = config;
        this.token = token;
        this.metrics = metrics;
        this.loader = loader;
        this.jobName = jobName;
        this.runId = runId;
        this.splitProvider =
                graph.getSplitAssignmentMode()
                        == SplitAssignmentMode.DYNAMIC
                        ? new LocalSplitQueue<SplitT>(
                                graph.getSourceSplits())
                        : null;
    }

    private static void fail(
            List<DataChannel<RecordEnvelope<FluxRow>>> channels,
            Throwable cause) {
        for (DataChannel<RecordEnvelope<FluxRow>> channel : channels) {
            channel.fail(
                    cause == null
                            ? new IllegalStateException(
                                    "Pipeline cancelled")
                            : cause);
        }
    }

    PipelineResult execute() {
        List<DataChannel<RecordEnvelope<FluxRow>>> channels =
                new ArrayList<DataChannel<RecordEnvelope<FluxRow>>>();

        try {
            if (splitProvider != null) {
                metrics.registerSplitProvider(splitProvider);
            }

            for (int i = 0;
                 i < graph.getSinkTaskPlans().size();
                 i++) {

                LocalDataChannel<RecordEnvelope<FluxRow>> channel =
                        new LocalDataChannel<RecordEnvelope<FluxRow>>(
                                graph.getPipelineId()
                                        + "-source-to-sink-"
                                        + i,
                                config.getMaxBufferedBatches(),
                                config.getMaxBufferedRecords(),
                                config.getMaxBufferedBytes(),
                                config.getMaxRecordsPerSecond(),
                                config.getMaxBytesPerSecond(),
                                graph.getSourceTaskPlans().size());

                channels.add(channel);
                metrics.registerChannel(channel.getMetrics());
            }

            List<ExecutionTask> sinks =
                    new ArrayList<ExecutionTask>();

            for (int i = 0; i < channels.size(); i++) {
                sinks.add(
                        new SinkTask(
                                graph.getSinkTaskPlans().get(i),
                                new InputGate<RecordEnvelope<FluxRow>>(
                                        channels.get(i).openReader())));
            }

            List<ExecutionTask> sources =
                    new ArrayList<ExecutionTask>();

            for (SourceTaskPlan<SplitT> source :
                    graph.getSourceTaskPlans()) {
                sources.add(
                        createSource(
                                source,
                                channels));
            }

            try (TaskExecutor executor =
                         new TaskExecutor(
                                 sinks.size() + sources.size(),
                                 "link-up-" + graph.getPipelineId(),
                                 jobName,
                                 runId)) {

                ExecutionCoordinator outcomeCoordinator =
                        new ExecutionCoordinator(
                                executor,
                                token,
                                metrics,
                                loader,
                                new Runnable() {
                                    public void run() {
                                        fail(
                                                channels,
                                                token.getCause());
                                        cancelSplitProvider();
                                    }
                                });

                ExecutionCoordinator.ExecutionOutcome outcome =
                        outcomeCoordinator.execute(
                                sinks,
                                sources);

                if (outcome.getFailure() != null) {
                    return createPipelineResult(
                            token.isCancelled()
                                    ? PipelineStatus.CANCELED
                                    : PipelineStatus.FAILED,
                            outcome.getCommitSummary(),
                            outcome.getFailure());
                }

                return createPipelineResult(
                        PipelineStatus.SUCCEEDED,
                        outcome.getCommitSummary(),
                        null);
            }

        } finally {
            for (DataChannel<RecordEnvelope<FluxRow>> channel :
                    channels) {
                try {
                    channel.close();
                } catch (Throwable ignored) {
                    // Best-effort close after execution outcome is known.
                }
            }
        }
    }

    private PipelineResult createPipelineResult(
            PipelineStatus status,
            CommitSummary commitSummary,
            Throwable failure) {

        SourceTaskPlan<SplitT> sourceTaskPlan =
                graph.getSourceTaskPlans().get(0);
        SinkTaskPlan sinkTaskPlan =
                graph.getSinkTaskPlans().get(0);

        String sourceIdentifier =
                sourceTaskPlan
                        .getPreparedSource()
                        .getFactoryIdentifier();

        PreparedSink preparedSink =
                sinkTaskPlan.getPreparedSink();
        String sinkIdentifier =
                preparedSink.getFactoryIdentifier();

        CatalogTable targetTable =
                preparedSink
                        .getMetadata()
                        .getTargetTable(
                                graph.getDataSetPath());
        TableDdl tableDdl =
                preparedSink
                        .getMetadata()
                        .getTableDdl(
                                graph.getDataSetPath());

        String sinkTablePath = "-";
        if (targetTable != null
                && targetTable.getTablePath() != null) {
            sinkTablePath =
                    targetTable.getTablePath().toString();
        }

        return new PipelineResult(
                graph.getPipelineId(),
                graph.getDataSetId(),
                sourceIdentifier,
                graph.getDataSetPath().toString(),
                graph.getSourceTaskPlans().size(),
                sinkIdentifier,
                sinkTablePath,
                graph.getSinkTaskPlans().size(),
                tableDdl,
                status,
                commitSummary,
                failure);
    }

    private void cancelSplitProvider() {
        if (splitProvider != null) {
            splitProvider.cancel(token.getCause());
        }
    }

    private ExecutionTask createSource(
            SourceTaskPlan<SplitT> source,
            List<DataChannel<RecordEnvelope<FluxRow>>> channels) {

        List<ChannelWriter<RecordEnvelope<FluxRow>>> writers =
                new ArrayList<ChannelWriter<RecordEnvelope<FluxRow>>>();

        for (DataChannel<RecordEnvelope<FluxRow>> channel : channels) {
            writers.add(channel.openWriter());
        }

        Partitioner<RecordEnvelope<FluxRow>> partitioner =
                new Partitioner<RecordEnvelope<FluxRow>>() {
                    public int selectChannel(
                            RecordEnvelope<FluxRow> value,
                            int count) {
                        return count == 1
                                ? 0
                                : Math.floorMod(
                                        value.getBatch()
                                                .getSplitId()
                                                .hashCode(),
                                        count);
                    }
                };

        return new SourceTask<SplitT>(
                source,
                new OutputGate<RecordEnvelope<FluxRow>>(
                        writers,
                        partitioner),
                splitProvider);
    }
}
