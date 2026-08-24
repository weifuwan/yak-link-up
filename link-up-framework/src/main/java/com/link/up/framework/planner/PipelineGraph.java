package com.link.up.framework.planner;

import com.link.up.api.source.SourceSplit;
import com.link.up.api.table.catalog.CatalogTable;
import com.link.up.api.table.catalog.TablePath;
import com.link.up.framework.job.SplitAssignmentMode;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Immutable physical graph for one logical data set.
 *
 * <p>The graph keeps split assignment as data only. Mutable dynamic split
 * queues are created later by the execution layer.
 */
public final class PipelineGraph<SplitT extends SourceSplit> {

    private final String pipelineId;
    private final String dataSetId;
    private final TablePath dataSetPath;
    private final CatalogTable catalogTable;
    private final List<SourceTaskPlan<SplitT>> sourceTaskPlans;
    private final List<SinkTaskPlan> sinkTaskPlans;
    private final List<SplitT> sourceSplits;
    private final SplitAssignmentMode splitAssignmentMode;

    public PipelineGraph(
            String pipelineId,
            String dataSetId,
            CatalogTable catalogTable,
            List<SourceTaskPlan<SplitT>> sourceTaskPlans,
            List<SinkTaskPlan> sinkTaskPlans,
            SplitAssignmentMode splitAssignmentMode) {

        this.pipelineId = requireText(pipelineId, "pipelineId");
        this.dataSetId = requireText(dataSetId, "dataSetId");
        this.catalogTable = Objects.requireNonNull(
                catalogTable,
                "catalogTable must not be null");
        this.dataSetPath = Objects.requireNonNull(
                catalogTable.getTablePath(),
                "catalogTable.tablePath must not be null");
        this.sourceTaskPlans = immutable(
                sourceTaskPlans,
                "sourceTaskPlans");
        this.sinkTaskPlans = immutable(
                sinkTaskPlans,
                "sinkTaskPlans");
        this.splitAssignmentMode = Objects.requireNonNull(
                splitAssignmentMode,
                "splitAssignmentMode must not be null");

        if (this.sourceTaskPlans.isEmpty()
                || this.sinkTaskPlans.isEmpty()) {
            throw new IllegalArgumentException(
                    "pipeline tasks must not be empty");
        }

        List<SplitT> splits = new ArrayList<SplitT>();
        for (SourceTaskPlan<SplitT> sourceTaskPlan :
                this.sourceTaskPlans) {
            splits.addAll(sourceTaskPlan.getSplits());
        }
        this.sourceSplits = Collections.unmodifiableList(splits);
    }

    private static String requireText(String value, String name) {
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
        return value.trim();
    }

    private static <T> List<T> immutable(
            List<T> values,
            String name) {
        return Collections.unmodifiableList(
                new ArrayList<T>(
                        Objects.requireNonNull(
                                values,
                                name + " must not be null")));
    }

    public String getPipelineId() {
        return pipelineId;
    }

    public String getDataSetId() {
        return dataSetId;
    }

    public TablePath getDataSetPath() {
        return dataSetPath;
    }

    public CatalogTable getCatalogTable() {
        return catalogTable;
    }

    public List<SourceTaskPlan<SplitT>> getSourceTaskPlans() {
        return sourceTaskPlans;
    }

    public List<SinkTaskPlan> getSinkTaskPlans() {
        return sinkTaskPlans;
    }

    /**
     * All source splits represented by this pipeline graph.
     *
     * <p>Static execution consumes the per-task assignments. Dynamic execution
     * uses this flattened immutable list to create a runtime split queue.
     */
    public List<SplitT> getSourceSplits() {
        return sourceSplits;
    }

    public SplitAssignmentMode getSplitAssignmentMode() {
        return splitAssignmentMode;
    }
}
