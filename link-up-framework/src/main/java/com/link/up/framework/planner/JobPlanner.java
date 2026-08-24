package com.link.up.framework.planner;

import com.link.up.api.source.SourceSplit;
import com.link.up.framework.connector.PreparedJob;
import com.link.up.framework.connector.PreparedSource;
import com.link.up.framework.job.ExecutionConfig;
import com.link.up.framework.source.SourceCoordinator;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Builds immutable job topology from prepared connectors.
 *
 * <p>Split discovery belongs to {@link SourceCoordinator}; per-data-set physical
 * planning belongs to {@link PipelinePlanner}.</p>
 */
public final class JobPlanner {

    private final SourceCoordinator sourceCoordinator;
    private final PipelinePlanner pipelinePlanner;

    public JobPlanner() {
        this(
                new SourceCoordinator(),
                new SplitAssigner());
    }

    public JobPlanner(SplitAssigner splitAssigner) {
        this(
                new SourceCoordinator(),
                splitAssigner);
    }

    public JobPlanner(
            SourceCoordinator sourceCoordinator,
            SplitAssigner splitAssigner) {

        this.sourceCoordinator = Objects.requireNonNull(
                sourceCoordinator,
                "sourceCoordinator must not be null");
        this.pipelinePlanner =
                new PipelinePlanner(
                        Objects.requireNonNull(
                                splitAssigner,
                                "splitAssigner must not be null"));
    }

    public JobGraph plan(PreparedJob preparedJob)
            throws Exception {

        PreparedJob job = Objects.requireNonNull(
                preparedJob,
                "preparedJob must not be null");

        return createGraph(
                job,
                job.getSource());
    }

    private <SplitT extends SourceSplit> JobGraph createGraph(
            PreparedJob job,
            PreparedSource<SplitT> preparedSource)
            throws Exception {

        ExecutionConfig config = job.getExecutionConfig();

        List<SplitT> splits =
                sourceCoordinator.enumerateSplits(
                        preparedSource,
                        config.getSourceParallelism());

        Map<String, List<SplitT>> byDataSet =
                DataSetSplitGrouper.group(splits);

        List<PipelineGraph<?>> pipelines =
                new ArrayList<PipelineGraph<?>>(
                        byDataSet.size());

        for (Map.Entry<String, List<SplitT>> entry :
                byDataSet.entrySet()) {

            pipelines.add(
                    pipelinePlanner.plan(
                            entry.getKey(),
                            entry.getValue(),
                            job,
                            preparedSource));
        }

        return new JobGraph(
                job.getJobName(),
                config,
                pipelines);
    }
}
