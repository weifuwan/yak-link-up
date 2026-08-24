package com.link.up.framework.execution;

import com.link.up.api.configuration.ReadonlyConfig;
import com.link.up.api.configuration.util.OptionRule;
import com.link.up.api.factory.SinkFactory;
import com.link.up.api.sink.PreparedSinkMetadata;
import com.link.up.api.source.Source;
import com.link.up.api.source.SourceReader;
import com.link.up.api.source.SourceSplit;
import com.link.up.api.table.catalog.CatalogTable;
import com.link.up.api.table.catalog.Column;
import com.link.up.api.table.catalog.TablePath;
import com.link.up.api.table.catalog.TableSchema;
import com.link.up.api.table.type.BasicType;
import com.link.up.api.table.type.FluxRow;
import com.link.up.framework.connector.PreparedSink;
import com.link.up.framework.connector.PreparedSource;
import com.link.up.framework.job.CommitSummary;
import com.link.up.framework.job.ExecutionConfig;
import com.link.up.framework.job.PipelineResult;
import com.link.up.framework.job.PipelineStatus;
import com.link.up.framework.job.SinkPartitionStrategy;
import com.link.up.framework.job.SplitAssignmentMode;
import com.link.up.framework.planner.JobGraph;
import com.link.up.framework.planner.PipelineGraph;
import com.link.up.framework.planner.SinkTaskPlan;
import com.link.up.framework.planner.SourceTaskPlan;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

final class RuntimeRoleTestSupport {

    private RuntimeRoleTestSupport() {
    }

    static PipelineGraph<TestSplit> pipelineGraph(
            String pipelineId) {

        TablePath path = TablePath.of(
                "demo",
                pipelineId.replace('-', '_'));
        CatalogTable table = table(path);
        Map<TablePath, CatalogTable> tables =
                new LinkedHashMap<TablePath, CatalogTable>();
        tables.put(path, table);

        PreparedSource<TestSplit> preparedSource =
                preparedSource(tables);
        TestSplit split =
                new TestSplit(
                        pipelineId + "-split",
                        path.toString());

        return new PipelineGraph<TestSplit>(
                pipelineId,
                path.toString(),
                table,
                Collections.singletonList(split),
                Collections.singletonList(
                        new SourceTaskPlan<TestSplit>(
                                new TaskId(
                                        pipelineId,
                                        TaskType.SOURCE,
                                        0,
                                        1),
                                preparedSource,
                                Collections.singletonList(split),
                                100)),
                Collections.singletonList(
                        new SinkTaskPlan(
                                new TaskId(
                                        pipelineId,
                                        TaskType.SINK,
                                        0,
                                        1),
                                preparedSink(tables))),
                SplitAssignmentMode.STATIC_ROUND_ROBIN);
    }

    static JobGraph jobGraph(
            List<PipelineGraph<?>> pipelines,
            int pipelineParallelism) {

        return new JobGraph(
                "phase-3-test",
                new ExecutionConfig(
                        100,
                        1,
                        1,
                        pipelineParallelism,
                        32,
                        -1L,
                        -1L,
                        -1L,
                        -1L,
                        SinkPartitionStrategy.TABLE_AFFINITY,
                        SplitAssignmentMode.STATIC_ROUND_ROBIN),
                pipelines);
    }

    static PipelineResult result(
            PipelineGraph<?> graph,
            PipelineStatus status,
            CommitSummary summary,
            Throwable failure) {

        return new PipelineResult(
                graph.getPipelineId(),
                graph.getDataSetId(),
                status,
                summary,
                failure);
    }

    static PipelineResult success(
            PipelineGraph<?> graph) {

        return result(
                graph,
                PipelineStatus.SUCCEEDED,
                CommitSummary.empty(),
                null);
    }

    private static CatalogTable table(
            TablePath path) {

        TableSchema schema = TableSchema.builder()
                .column(
                        Column.builder(
                                "id",
                                BasicType.LONG_TYPE)
                                .nullable(false)
                                .build())
                .build();

        return CatalogTable.builder(
                path,
                schema)
                .build();
    }

    private static PreparedSource<TestSplit> preparedSource(
            final Map<TablePath, CatalogTable> tables) {

        Source<TestSplit> source = new Source<TestSplit>() {
            @Override
            public List<TestSplit> createSplits(
                    Map<TablePath, CatalogTable> ignored) {
                return Collections.emptyList();
            }

            @Override
            public SourceReader<FluxRow, TestSplit> createReader(
                    Map<TablePath, CatalogTable> ignored,
                    int batchSize) {
                return null;
            }
        };

        return new PreparedSource<TestSplit>(
                "test-source",
                source,
                tables);
    }

    private static PreparedSink preparedSink(
            Map<TablePath, CatalogTable> tables) {

        SinkFactory factory = new SinkFactory() {
            @Override
            public String factoryIdentifier() {
                return "test-sink";
            }

            @Override
            public OptionRule optionRule() {
                return null;
            }
        };

        return new PreparedSink(
                "test-sink",
                factory,
                ReadonlyConfig.fromMap(
                        Collections.<String, Object>emptyMap()),
                new PreparedSinkMetadata(tables));
    }

    static final class TestSplit
            implements SourceSplit {

        private static final long serialVersionUID = 1L;

        private final String splitId;
        private final String dataSetId;

        private TestSplit(
                String splitId,
                String dataSetId) {
            this.splitId = splitId;
            this.dataSetId = dataSetId;
        }

        @Override
        public String splitId() {
            return splitId;
        }

        @Override
        public String dataSetId() {
            return dataSetId;
        }
    }
}
