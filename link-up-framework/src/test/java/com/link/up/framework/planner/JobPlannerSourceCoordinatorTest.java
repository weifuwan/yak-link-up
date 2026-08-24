package com.link.up.framework.planner;

import com.link.up.api.configuration.ReadonlyConfig;
import com.link.up.api.configuration.util.OptionRule;
import com.link.up.api.factory.SinkFactory;
import com.link.up.api.sink.PreparedSinkMetadata;
import com.link.up.api.source.Source;
import com.link.up.api.source.SourceEnumeratorContext;
import com.link.up.api.source.SourceReader;
import com.link.up.api.source.SourceSplit;
import com.link.up.api.source.SourceSplitEnumerator;
import com.link.up.api.table.catalog.CatalogTable;
import com.link.up.api.table.catalog.Column;
import com.link.up.api.table.catalog.TablePath;
import com.link.up.api.table.catalog.TableSchema;
import com.link.up.api.table.type.BasicType;
import com.link.up.api.table.type.FluxRow;
import com.link.up.framework.connector.PreparedJob;
import com.link.up.framework.connector.PreparedSink;
import com.link.up.framework.connector.PreparedSource;
import com.link.up.framework.job.ExecutionConfig;
import org.junit.Test;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class JobPlannerSourceCoordinatorTest {

    @Test
    public void plannerShouldUseNativeEnumeratorInsteadOfLegacyCreateSplits()
            throws Exception {

        final TablePath path = TablePath.of("demo", "users");
        final CatalogTable table = table(path);
        final Map<TablePath, CatalogTable> tables =
                new LinkedHashMap<TablePath, CatalogTable>();
        tables.put(path, table);

        final boolean[] enumeratorCreated = {false};
        Source<TestSplit> source = new Source<TestSplit>() {
            @Override
            public SourceSplitEnumerator<TestSplit> createEnumerator(
                    Map<TablePath, CatalogTable> preparedTables,
                    SourceEnumeratorContext context) {

                enumeratorCreated[0] = true;
                assertSame(table, preparedTables.get(path));
                assertEquals(2, context.getParallelism());

                return new SourceSplitEnumerator<TestSplit>() {
                    @Override
                    public List<TestSplit> enumerateSplits() {
                        return Collections.singletonList(
                                new TestSplit(
                                        "split-1",
                                        path.toString()));
                    }
                };
            }

            @Override
            public SourceReader<FluxRow, TestSplit> createReader(
                    Map<TablePath, CatalogTable> preparedTables,
                    int batchSize) {
                return null;
            }
        };

        PreparedSource<TestSplit> preparedSource =
                new PreparedSource<TestSplit>(
                        "native-source",
                        source,
                        tables);

        PreparedSink preparedSink = preparedSink(tables);
        Map<String, List<PreparedSink>> sinks =
                new LinkedHashMap<String, List<PreparedSink>>();
        sinks.put(
                path.toString(),
                Collections.singletonList(preparedSink));

        PreparedJob preparedJob =
                new PreparedJob(
                        "phase-4-planner-test",
                        preparedSource,
                        sinks,
                        new ExecutionConfig(
                                100,
                                2,
                                1,
                                32));

        JobGraph graph = new JobPlanner().plan(preparedJob);

        assertTrue(enumeratorCreated[0]);
        assertEquals(1, graph.getPipelineGraphs().size());
        PipelineGraph<?> pipeline = graph.getPipelineGraphs().get(0);
        assertEquals(path.toString(), pipeline.getDataSetId());
        assertEquals(1, pipeline.getSourceSplits().size());
        assertEquals("split-1", pipeline.getSourceSplits().get(0).splitId());
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
