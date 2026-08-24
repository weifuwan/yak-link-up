package com.link.up.framework.execution;

import com.link.up.api.source.SourceSplit;
import com.link.up.api.table.type.FluxRow;
import com.link.up.framework.channel.ChannelWriter;
import com.link.up.framework.channel.DataChannel;
import com.link.up.framework.channel.InputGate;
import com.link.up.framework.channel.OutputGate;
import com.link.up.framework.channel.RecordEnvelope;
import com.link.up.framework.execution.split.SplitProvider;
import com.link.up.framework.execution.task.ExecutionTask;
import com.link.up.framework.execution.task.SinkTask;
import com.link.up.framework.execution.task.SourceTask;
import com.link.up.framework.planner.PipelineGraph;
import com.link.up.framework.planner.SourceTaskPlan;
import com.link.up.framework.routing.Partitioner;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/** Materializes runtime tasks from one immutable pipeline graph. */
final class PipelineTaskFactory<SplitT extends SourceSplit> {

    private final PipelineGraph<SplitT> graph;
    private final List<DataChannel<RecordEnvelope<FluxRow>>> channels;
    private final SplitProvider<SplitT> splitProvider;

    PipelineTaskFactory(
            PipelineGraph<SplitT> graph,
            List<DataChannel<RecordEnvelope<FluxRow>>> channels,
            SplitProvider<SplitT> splitProvider) {

        this.graph = Objects.requireNonNull(
                graph,
                "graph must not be null");
        this.channels = Objects.requireNonNull(
                channels,
                "channels must not be null");
        this.splitProvider = splitProvider;
    }

    List<ExecutionTask> createSinkTasks() {
        if (channels.size() != graph.getSinkTaskPlans().size()) {
            throw new IllegalStateException(
                    "Channel count must match sink task count");
        }

        List<ExecutionTask> sinkTasks =
                new ArrayList<ExecutionTask>(channels.size());

        for (int index = 0; index < channels.size(); index++) {
            sinkTasks.add(
                    new SinkTask(
                            graph.getSinkTaskPlans().get(index),
                            new InputGate<RecordEnvelope<FluxRow>>(
                                    channels.get(index).openReader())));
        }

        return sinkTasks;
    }

    List<ExecutionTask> createSourceTasks() {
        List<ExecutionTask> sourceTasks =
                new ArrayList<ExecutionTask>(
                        graph.getSourceTaskPlans().size());

        for (SourceTaskPlan<SplitT> sourceTaskPlan :
                graph.getSourceTaskPlans()) {
            sourceTasks.add(createSourceTask(sourceTaskPlan));
        }

        return sourceTasks;
    }

    private ExecutionTask createSourceTask(
            SourceTaskPlan<SplitT> sourceTaskPlan) {

        List<ChannelWriter<RecordEnvelope<FluxRow>>> writers =
                new ArrayList<ChannelWriter<RecordEnvelope<FluxRow>>>(
                        channels.size());

        for (DataChannel<RecordEnvelope<FluxRow>> channel : channels) {
            writers.add(channel.openWriter());
        }

        return new SourceTask<SplitT>(
                sourceTaskPlan,
                new OutputGate<RecordEnvelope<FluxRow>>(
                        writers,
                        splitPartitioner()),
                splitProvider);
    }

    private Partitioner<RecordEnvelope<FluxRow>> splitPartitioner() {
        return new Partitioner<RecordEnvelope<FluxRow>>() {
            @Override
            public int selectChannel(
                    RecordEnvelope<FluxRow> value,
                    int count) {

                if (count == 1) {
                    return 0;
                }

                return Math.floorMod(
                        value.getBatch().getSplitId().hashCode(),
                        count);
            }
        };
    }
}
