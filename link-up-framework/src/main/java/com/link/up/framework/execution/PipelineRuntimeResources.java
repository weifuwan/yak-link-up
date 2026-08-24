package com.link.up.framework.execution;

import com.link.up.api.source.SourceSplit;
import com.link.up.api.table.type.FluxRow;
import com.link.up.framework.channel.DataChannel;
import com.link.up.framework.channel.LocalDataChannel;
import com.link.up.framework.channel.RecordEnvelope;
import com.link.up.framework.execution.split.LocalSplitQueue;
import com.link.up.framework.execution.split.SplitProvider;
import com.link.up.framework.job.ExecutionConfig;
import com.link.up.framework.job.SplitAssignmentMode;
import com.link.up.framework.metrics.JobMetrics;
import com.link.up.framework.planner.PipelineGraph;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Owns mutable resources for one local pipeline execution.
 *
 * <p>Channels and dynamic split queues are created here and released with the
 * pipeline lifecycle. Planner models remain immutable data only.</p>
 */
final class PipelineRuntimeResources<SplitT extends SourceSplit>
        implements AutoCloseable {

    private final List<DataChannel<RecordEnvelope<FluxRow>>> channels;
    private final SplitProvider<SplitT> splitProvider;

    private PipelineRuntimeResources(
            List<DataChannel<RecordEnvelope<FluxRow>>> channels,
            SplitProvider<SplitT> splitProvider) {

        this.channels = Collections.unmodifiableList(
                new ArrayList<DataChannel<RecordEnvelope<FluxRow>>>(channels));
        this.splitProvider = splitProvider;
    }

    static <SplitT extends SourceSplit> PipelineRuntimeResources<SplitT> open(
            PipelineGraph<SplitT> graph,
            ExecutionConfig config,
            JobMetrics metrics) {

        Objects.requireNonNull(graph, "graph must not be null");
        Objects.requireNonNull(config, "config must not be null");
        Objects.requireNonNull(metrics, "metrics must not be null");

        SplitProvider<SplitT> splitProvider = createSplitProvider(graph);
        if (splitProvider != null) {
            metrics.registerSplitProvider(splitProvider);
        }

        List<DataChannel<RecordEnvelope<FluxRow>>> channels =
                createChannels(graph, config, metrics);

        return new PipelineRuntimeResources<SplitT>(
                channels,
                splitProvider);
    }

    List<DataChannel<RecordEnvelope<FluxRow>>> getChannels() {
        return channels;
    }

    SplitProvider<SplitT> getSplitProvider() {
        return splitProvider;
    }

    void cancel(Throwable cause) {
        Throwable failure = cause == null
                ? new IllegalStateException("Pipeline cancelled")
                : cause;

        for (DataChannel<RecordEnvelope<FluxRow>> channel : channels) {
            channel.fail(failure);
        }

        if (splitProvider != null) {
            splitProvider.cancel(cause);
        }
    }

    @Override
    public void close() {
        for (DataChannel<RecordEnvelope<FluxRow>> channel : channels) {
            try {
                channel.close();
            } catch (Throwable ignored) {
                // Best-effort close after the pipeline outcome is known.
            }
        }
    }

    private static <SplitT extends SourceSplit> SplitProvider<SplitT>
    createSplitProvider(PipelineGraph<SplitT> graph) {

        if (graph.getSplitAssignmentMode() != SplitAssignmentMode.DYNAMIC) {
            return null;
        }

        return new LocalSplitQueue<SplitT>(graph.getSourceSplits());
    }

    private static <SplitT extends SourceSplit>
    List<DataChannel<RecordEnvelope<FluxRow>>> createChannels(
            PipelineGraph<SplitT> graph,
            ExecutionConfig config,
            JobMetrics metrics) {

        List<DataChannel<RecordEnvelope<FluxRow>>> channels =
                new ArrayList<DataChannel<RecordEnvelope<FluxRow>>>(
                        graph.getSinkTaskPlans().size());

        for (int index = 0;
             index < graph.getSinkTaskPlans().size();
             index++) {

            LocalDataChannel<RecordEnvelope<FluxRow>> channel =
                    new LocalDataChannel<RecordEnvelope<FluxRow>>(
                            graph.getPipelineId()
                                    + "-source-to-sink-"
                                    + index,
                            config.getMaxBufferedBatches(),
                            config.getMaxBufferedRecords(),
                            config.getMaxBufferedBytes(),
                            config.getMaxRecordsPerSecond(),
                            config.getMaxBytesPerSecond(),
                            graph.getSourceTaskPlans().size());

            channels.add(channel);
            metrics.registerChannel(channel.getMetrics());
        }

        return channels;
    }
}
