package com.link.up.framework.planner;

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
import com.link.up.framework.execution.TaskId;
import com.link.up.framework.execution.TaskType;
import com.link.up.framework.job.SplitAssignmentMode;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class PipelineGraphTest {

    @Test
    public void shouldPreserveOriginalSplitOrderForDynamicExecution() {
        TablePath path = TablePath.of("demo", "users");
        CatalogTable table = table(path);
        Map<TablePath, CatalogTable> tables = tables(path, table);
        PreparedSource<TestSplit> preparedSource =
                preparedSource(tables);

        TestSplit s1 = new TestSplit("s1", path.toString());
        TestSplit s2 = new TestSplit("s2", path.toString());
        TestSplit s3 = new TestSplit("s3", path.toString());
        TestSplit s4 = new TestSplit("s4", path.toString());

        List<TestSplit> originalSplits =
                new ArrayList<TestSplit>(
                        Arrays.asList(s1, s2, s3, s4));

        List<SourceTaskPlan<TestSplit>> sourcePlans =
                Arrays.asList(
                        new SourceTaskPlan<TestSplit>(
                                new TaskId(
                                        "pipeline-users",
                                        TaskType.SOURCE,
                                        0,
                                        2),
                                preparedSource,
                                Arrays.asList(s1, s3),
                                100),
                        new SourceTaskPlan<TestSplit>(
                                new TaskId(
                                        "pipeline-users",
                                        TaskType.SOURCE,
                                        1,
                                        2),
                                preparedSource,
                                Arrays.asList(s2, s4),
                                100));

        PipelineGraph<TestSplit> graph =
                new PipelineGraph<TestSplit>(
                        "pipeline-users",
                        path.toString(),
                        table,
                        originalSplits,
                        sourcePlans,
                        Collections.singletonList(
                                new SinkTaskPlan(
                                        new TaskId(
                                                "pipeline-users",
                                                TaskType.SINK,
                                                0,
                                                1),
                                        preparedSink(tables))),
                        SplitAssignmentMode.DYNAMIC);

        originalSplits.clear();

        assertEquals(
                Arrays.asList(s1, s2, s3, s4),
                graph.getSourceSplits());
        assertEquals(
                Arrays.asList(s1, s3),
                graph.getSourceTaskPlans().get(0).getSplits());
        assertEquals(
                Arrays.asList(s2, s4),
                graph.getSourceTaskPlans().get(1).getSplits());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void sourceSplitViewShouldBeImmutable() {
        TablePath path = TablePath.of("demo", "users");
        CatalogTable table = table(path);
        Map<TablePath, CatalogTable> tables = tables(path, table);
        PreparedSource<TestSplit> preparedSource =
                preparedSource(tables);
        TestSplit split = new TestSplit("s1", path.toString());

        PipelineGraph<TestSplit> graph =
                new PipelineGraph<TestSplit>(
                        "pipeline-users",
                        path.toString(),
                        table,
                        Collections.singletonList(split),
                        Collections.singletonList(
                                new SourceTaskPlan<TestSplit>(
                                        new TaskId(
                                                "pipeline-users",
                                                TaskType.SOURCE,
                                                0,
                                                1),
                                        preparedSource,
                                        Collections.singletonList(split),
                                        100)),
                        Collections.singletonList(
                                new SinkTaskPlan(
                                        new TaskId(
                                                "pipeline-users",
                                                TaskType.SINK,
                                                0,
                                                1),
                                        preparedSink(tables))),
                        SplitAssignmentMode.DYNAMIC);

        graph.getSourceSplits().add(
                new TestSplit("s2", path.toString()));
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldRejectIncompleteTaskAssignments() {
        TablePath path = TablePath.of("demo", "users");
        CatalogTable table = table(path);
        Map<TablePath, CatalogTable> tables = tables(path, table);
        PreparedSource<TestSplit> preparedSource =
                preparedSource(tables);
        TestSplit s1 = new TestSplit("s1", path.toString());
        TestSplit s2 = new TestSplit("s2", path.toString());

        new PipelineGraph<TestSplit>(
                "pipeline-users",
                path.toString(),
                table,
                Arrays.asList(s1, s2),
                Collections.singletonList(
                        new SourceTaskPlan<TestSplit>(
                                new TaskId(
                                        "pipeline-users",
                                        TaskType.SOURCE,
                                        0,
                                        1),
                                preparedSource,
                                Collections.singletonList(s1),
                                100)),
                Collections.singletonList(
                        new SinkTaskPlan(
                                new TaskId(
                                        "pipeline-users",
                                        TaskType.SINK,
                                        0,
                                        1),
                                preparedSink(tables))),
                SplitAssignmentMode.DYNAMIC);
    }

    private static CatalogTable table(TablePath path) {
        TableSchema schema = TableSchema.builder()
                .column(
                        Column.builder(
                                "id",
                                BasicType.LONG_TYPE)
                                .nullable(false)
                                .build())
                .build();
        return CatalogTable.builder(path, schema).build();
    }

    private static Map<TablePath, CatalogTable> tables(
            TablePath path,
            CatalogTable table) {
        Map<TablePath, CatalogTable> tables =
                new LinkedHashMap<TablePath, CatalogTable>();
        tables.put(path, table);
        return tables;
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

    private static final class TestSplit implements SourceSplit {
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
