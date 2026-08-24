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
    private final List<SplitT> sourceSplits;
    private final List<SourceTaskPlan<SplitT>> sourceTaskPlans;
    private final List<SinkTaskPlan> sinkTaskPlans;
    private final SplitAssignmentMode splitAssignmentMode;

    public PipelineGraph(
            String pipelineId,
            String dataSetId,
            CatalogTable catalogTable,
            List<SplitT> sourceSplits,
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
        this.sourceSplits = immutable(
                sourceSplits,
                "sourceSplits");
        this.sourceTaskPlans = immutable(
                sourceTaskPlans,
                "sourceTaskPlans");
        this.sinkTaskPlans = immutable(
                sinkTaskPlans,
                "sinkTaskPlans");
        this.splitAssignmentMode = Objects.requireNonNull(
                splitAssignmentMode,
                "splitAssignmentMode must not be null");

        if (this.sourceSplits.isEmpty()) {
            throw new IllegalArgumentException(
                    "sourceSplits must not be empty");
        }

        if (this.sourceTaskPlans.isEmpty()
                || this.sinkTaskPlans.isEmpty()) {
            throw new IllegalArgumentException(
                    "pipeline tasks must not be empty");
        }

        int assignedSplitCount = 0;
        for (SourceTaskPlan<SplitT> sourceTaskPlan :
                this.sourceTaskPlans) {
            assignedSplitCount += sourceTaskPlan.getSplits().size();
        }

        if (assignedSplitCount != this.sourceSplits.size()) {
            throw new IllegalArgumentException(
                    "source task assignments must cover all source splits: assigned="
                            + assignedSplitCount
                            + ", total="
                            + this.sourceSplits.size());
        }
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

        Objects.requireNonNull(
                values,
                name + " must not be null");

        List<T> copy = new ArrayList<T>(values.size());
        for (T value : values) {
            copy.add(
                    Objects.requireNonNull(
                            value,
                            name + " must not contain null values"));
        }

        return Collections.unmodifiableList(copy);
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

    /**
     * Source splits in the original enumeration order returned by the Source.
     *
     * <p>This list is intentionally independent from per-task round-robin
     * assignments. Dynamic execution uses this order when materializing its
     * runtime-local split queue.
     */
    public List<SplitT> getSourceSplits() {
        return sourceSplits;
    }

    public List<SourceTaskPlan<SplitT>> getSourceTaskPlans() {
        return sourceTaskPlans;
    }

    public List<SinkTaskPlan> getSinkTaskPlans() {
        return sinkTaskPlans;
    }

    public SplitAssignmentMode getSplitAssignmentMode() {
        return splitAssignmentMode;
    }
}
