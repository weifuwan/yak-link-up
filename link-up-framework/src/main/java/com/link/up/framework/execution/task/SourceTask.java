package com.link.up.framework.execution.task;

import com.link.up.api.source.RecordBatch;
import com.link.up.api.source.SourceReader;
import com.link.up.api.source.SourceSplit;
import com.link.up.api.table.catalog.CatalogTable;
import com.link.up.api.table.catalog.TablePath;
import com.link.up.api.table.type.FluxRow;
import com.link.up.framework.channel.OutputGate;
import com.link.up.framework.channel.RecordEnvelope;
import com.link.up.framework.connector.PreparedSource;
import com.link.up.framework.execution.TaskContext;
import com.link.up.framework.execution.TaskId;
import com.link.up.framework.execution.split.SplitProvider;
import com.link.up.framework.mapping.ColumnMappingPlan;
import com.link.up.framework.planner.SourceTaskPlan;

import java.util.Objects;

/**
 * Source data-reading task.
 *
 * <p>The immutable task plan is separate from runtime split ownership. A
 * shared SplitProvider may be injected by PipelineExecution for dynamic split
 * assignment.
 */
public final class SourceTask<
        SplitT extends SourceSplit>
        implements ExecutionTask {

    private final SourceTaskPlan<SplitT> plan;
    private final OutputGate<RecordEnvelope<FluxRow>> outputGate;
    private final SplitProvider<SplitT> splitProvider;

    public SourceTask(
            SourceTaskPlan<SplitT> plan,
            OutputGate<RecordEnvelope<FluxRow>> outputGate) {
        this(plan, outputGate, null);
    }

    public SourceTask(
            SourceTaskPlan<SplitT> plan,
            OutputGate<RecordEnvelope<FluxRow>> outputGate,
            SplitProvider<SplitT> splitProvider) {

        this.plan = Objects.requireNonNull(
                plan,
                "plan must not be null");
        this.outputGate = Objects.requireNonNull(
                outputGate,
                "outputGate must not be null");
        this.splitProvider = splitProvider;
    }

    private static RuntimeException propagate(
            Throwable throwable)
            throws Exception {

        if (throwable instanceof Exception) {
            throw (Exception) throwable;
        }

        if (throwable instanceof Error) {
            throw (Error) throwable;
        }

        return new RuntimeException(throwable);
    }

    @Override
    public TaskId getTaskId() {
        return plan.getTaskId();
    }

    @Override
    public void execute(TaskContext context)
            throws Exception {

        PreparedSource<SplitT> preparedSource =
                plan.getPreparedSource();

        try (com.link.up.framework.classloading.ClassLoaderScope ignored =
                     com.link.up.framework.classloading.ClassLoaderScope.open(
                             preparedSource.getClassLoader())) {

            SourceReader<FluxRow, SplitT> reader = null;
            Throwable failure = null;

            try {
                reader = preparedSource
                        .getSource()
                        .createReader(
                                preparedSource.getTables(),
                                plan.getBatchSize());

                if (reader == null) {
                    throw new IllegalStateException(
                            "Source returned a null reader");
                }

                if (splitProvider != null) {
                    executeDynamically(
                            reader,
                            splitProvider,
                            context,
                            preparedSource);
                    return;
                }

                context.getMetrics()
                        .setTotalSplitCount(
                                plan.getSplits().size());
                reader.open(plan.getSplits());

                while (!context.getCancellationToken().isCancelled()) {
                    RecordBatch<FluxRow> batch = reader.readBatch();

                    if (batch == null) {
                        throw new IllegalStateException(
                                "SourceReader returned a null RecordBatch");
                    }

                    if (batch.isEndOfInput()) {
                        for (SplitT split : plan.getSplits()) {
                            context.getMetrics()
                                    .markSplitCompleted(
                                            split.splitId());
                        }
                        break;
                    }

                    context.getMetrics()
                            .setCurrentPosition(
                                    batch.getDataSetId(),
                                    batch.getSplitId());

                    RecordEnvelope<FluxRow> envelope =
                            createEnvelope(
                                    batch,
                                    preparedSource);

                    context.getMetrics()
                            .incrementBatchCount();
                    context.getMetrics()
                            .addSourceReadRecords(
                                    batch.getRecords().size());

                    outputGate.write(envelope);
                }

            } catch (Throwable throwable) {
                failure = throwable;
                outputGate.fail(throwable);
                throw propagate(throwable);

            } finally {
                Throwable closeFailure = null;

                if (reader != null) {
                    try {
                        reader.close();
                    } catch (Throwable throwable) {
                        closeFailure = throwable;
                    }
                }

                try {
                    outputGate.close();
                } catch (Throwable throwable) {
                    if (closeFailure == null) {
                        closeFailure = throwable;
                    } else {
                        closeFailure.addSuppressed(throwable);
                    }
                }

                if (failure != null
                        && closeFailure != null) {
                    failure.addSuppressed(closeFailure);
                } else if (failure == null
                        && closeFailure != null) {
                    throw propagate(closeFailure);
                }
            }
        }
    }

    private void executeDynamically(
            SourceReader<FluxRow, SplitT> reader,
            SplitProvider<SplitT> provider,
            TaskContext context,
            PreparedSource<SplitT> preparedSource)
            throws Exception {

        reader.open();

        while (!context.getCancellationToken().isCancelled()) {
            SplitT split = provider.acquire(
                    context.getCancellationToken());

            if (split == null) {
                return;
            }

            context.getMetrics().markSplitRunning();
            boolean completed = false;

            try {
                reader.openSplit(split);

                while (!context.getCancellationToken().isCancelled()) {
                    RecordBatch<FluxRow> batch = reader.readBatch();

                    if (batch == null) {
                        throw new IllegalStateException(
                                "SourceReader returned a null RecordBatch");
                    }

                    if (batch.isEndOfInput()) {
                        completed = true;
                        break;
                    }

                    context.getMetrics()
                            .setCurrentPosition(
                                    batch.getDataSetId(),
                                    batch.getSplitId());
                    context.getMetrics().incrementBatchCount();
                    context.getMetrics()
                            .addSourceReadRecords(
                                    batch.getRecords().size());
                    outputGate.write(
                            createEnvelope(
                                    batch,
                                    preparedSource));
                }

            } catch (Throwable failure) {
                provider.fail(split, failure);
                context.getMetrics().markSplitFailed();

                try {
                    reader.closeSplit();
                } catch (Throwable closeFailure) {
                    failure.addSuppressed(closeFailure);
                }

                throw failure;

            } finally {
                if (completed) {
                    reader.closeSplit();
                }
            }

            if (completed) {
                provider.complete(split);
                context.getMetrics().markSplitFinished();
                context.getMetrics()
                        .markSplitCompleted(
                                split.splitId());
            } else {
                provider.returnSplit(split);
                context.getMetrics().markSplitFinished();
                return;
            }
        }
    }

    private RecordEnvelope<FluxRow> createEnvelope(
            RecordBatch<FluxRow> batch,
            PreparedSource<SplitT> preparedSource) {

        String dataSetId = batch.getDataSetId();

        if (dataSetId == null
                || dataSetId.trim().isEmpty()) {
            throw new IllegalStateException(
                    "RecordBatch dataSetId must not be blank");
        }

        TablePath tablePath = TablePath.parse(dataSetId);
        CatalogTable catalogTable =
                preparedSource.getOutputTables().get(tablePath);

        if (catalogTable == null) {
            throw new IllegalStateException(
                    "No output source table for batch: "
                            + dataSetId);
        }

        RecordBatch<FluxRow> outputBatch = batch;
        ColumnMappingPlan mappingPlan =
                preparedSource.getColumnMappingPlan(tablePath);

        if (mappingPlan != null) {
            outputBatch = RecordBatch.of(
                    batch.getDataSetId(),
                    batch.getSplitId(),
                    mappingPlan.project(
                            batch.getRecords()));
            catalogTable = mappingPlan.getOutputTable();
        }

        return new RecordEnvelope<FluxRow>(
                tablePath,
                catalogTable,
                outputBatch);
    }
}
