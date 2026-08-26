package com.link.up.framework.planning;

import com.link.up.api.configuration.ReadonlyConfig;
import com.link.up.api.configuration.util.OptionRule;
import com.link.up.api.factory.SinkFactory;
import com.link.up.api.sink.SinkPreparer;
import com.link.up.api.source.Source;
import com.link.up.api.source.SourceEnumeratorContext;
import com.link.up.api.source.SourceFactoryContext;
import com.link.up.api.source.SourceReader;
import com.link.up.api.source.SourceSplit;
import com.link.up.api.source.SourceSplitEnumerator;
import com.link.up.api.table.catalog.CatalogTable;
import com.link.up.api.table.catalog.Column;
import com.link.up.api.table.catalog.TablePath;
import com.link.up.api.table.catalog.TableSchema;
import com.link.up.api.table.factory.TableSourceFactory;
import com.link.up.api.table.type.BasicType;
import com.link.up.api.table.type.FluxRow;
import com.link.up.framework.connector.ConnectorPreparer;
import com.link.up.framework.connector.FactoryRegistry;
import com.link.up.framework.job.ExecutionConfig;
import com.link.up.framework.job.JobDefinition;
import com.link.up.framework.job.SinkDefinition;
import com.link.up.framework.job.SourceDefinition;
import com.link.up.framework.planner.JobPlanner;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class JobPlanExplainerTest {

    @Test
    public void validateMustNotAccessConnectorRuntimeOrMetadata()
            throws Exception {

        Fixture fixture = new Fixture();
        try {
            JobPlanResult result =
                    fixture.explainer.validate(
                            fixture.definition());

            assertEquals(
                    JobPlanResult.Mode.VALIDATE,
                    result.getMode());
            assertNull(result.getPhysicalPlan());
            assertEquals(0, fixture.sourceCreateCalls.get());
            assertEquals(0, fixture.schemaDiscoveryCalls.get());
            assertEquals(0, fixture.splitEnumerationCalls.get());
            assertEquals(0, fixture.sinkPreparationCalls.get());
        } finally {
            fixture.close();
        }
    }

    @Test
    public void explainMustPlanSourceSplitsWithoutPreparingSink()
            throws Exception {

        Fixture fixture = new Fixture();
        try {
            JobPlanResult result =
                    fixture.explainer.explain(
                            fixture.definition());

            assertEquals(
                    JobPlanResult.Mode.EXPLAIN,
                    result.getMode());
            assertEquals(1, fixture.sourceCreateCalls.get());
            assertEquals(1, fixture.schemaDiscoveryCalls.get());
            assertEquals(1, fixture.splitEnumerationCalls.get());
            assertEquals(0, fixture.sinkPreparationCalls.get());
            assertEquals(
                    1,
                    result.getPhysicalPlan()
                            .getPipelineCount());
            assertEquals(
                    2,
                    result.getPhysicalPlan()
                            .getSourceTaskCount());
            assertEquals(
                    2,
                    result.getPhysicalPlan()
                            .getSinkTaskCount());
        } finally {
            fixture.close();
        }
    }

    @Test
    public void physicalProjectionMustExposeCountsButNotPreparedObjects()
            throws Exception {

        Fixture fixture = new Fixture();
        try {
            PhysicalJobPlan physical =
                    fixture.explainer.explain(
                                    fixture.definition())
                            .getPhysicalPlan();
            PhysicalJobPlan.Pipeline pipeline =
                    physical.getPipelines().get(0);

            assertEquals("demo.orders", pipeline.getDataSetId());
            assertEquals(2, pipeline.getSourceSplitCount());
            assertEquals(1, pipeline.getOutputColumnCount());
            assertEquals("test-source", pipeline.getSourceConnectorId());
            assertEquals("test-sink", pipeline.getSinkConnectorId());

            for (Field field : PhysicalJobPlan.class.getDeclaredFields()) {
                String typeName = field.getGenericType().getTypeName();
                assertFalse(typeName.contains("PreparedJob"));
                assertFalse(typeName.contains("PreparedSource"));
                assertFalse(typeName.contains("PreparedSink"));
                assertFalse(typeName.contains("ReadonlyConfig"));
            }
        } finally {
            fixture.close();
        }
    }

    @Test
    public void textExplainMustBeDeterministicAndSecretSafe()
            throws Exception {

        Fixture first = new Fixture();
        Fixture second = new Fixture();
        try {
            JobDefinition firstDefinition =
                    first.definition("TEST_ONLY_SECRET");
            JobDefinition secondDefinition =
                    second.definition("TEST_ONLY_SECRET");

            String firstText =
                    first.explainer.explain(firstDefinition)
                            .getText();
            String secondText =
                    second.explainer.explain(secondDefinition)
                            .getText();

            assertEquals(firstText, secondText);
            assertTrue(firstText.contains("pipeline-demo.orders"));
            assertTrue(firstText.contains("PLAN_SINK_PREPARATION_SKIPPED"));
            assertFalse(firstText.contains("TEST_ONLY_SECRET"));
        } finally {
            first.close();
            second.close();
        }
    }

    private static final class Fixture implements AutoCloseable {

        private final AtomicInteger sourceCreateCalls =
                new AtomicInteger();
        private final AtomicInteger schemaDiscoveryCalls =
                new AtomicInteger();
        private final AtomicInteger splitEnumerationCalls =
                new AtomicInteger();
        private final AtomicInteger sinkPreparationCalls =
                new AtomicInteger();
        private final FactoryRegistry registry;
        private final JobPlanExplainer explainer;

        private Fixture() throws Exception {
            TableSourceFactory<TestSplit> sourceFactory =
                    sourceFactory();
            SinkFactory sinkFactory = sinkFactory();
            this.registry = registry(
                    sourceFactory,
                    sinkFactory);
            this.explainer = new JobPlanExplainer(
                    new ConnectorPreparer(
                            registry,
                            getClass().getClassLoader()),
                    new JobPlanner());
        }

        private JobDefinition definition() {
            return definition(null);
        }

        private JobDefinition definition(String secret) {
            Map<String, Object> sourceOptions =
                    new LinkedHashMap<String, Object>();
            if (secret != null) {
                sourceOptions.put("password", secret);
            }

            return new JobDefinition(
                    "plan-explain-test",
                    new SourceDefinition(
                            "test-source",
                            ReadonlyConfig.fromMap(sourceOptions)),
                    new SinkDefinition(
                            "test-sink",
                            ReadonlyConfig.fromMap(
                                    Collections.<String, Object>emptyMap())),
                    new ExecutionConfig(
                            100,
                            2,
                            2,
                            32));
        }

        private TableSourceFactory<TestSplit> sourceFactory() {
            return new TableSourceFactory<TestSplit>() {
                @Override
                public String factoryIdentifier() {
                    return "test-source";
                }

                @Override
                public OptionRule optionRule() {
                    return OptionRule.builder().build();
                }

                @Override
                public Source<TestSplit> createSource(
                        SourceFactoryContext context) {

                    sourceCreateCalls.incrementAndGet();
                    return new Source<TestSplit>() {
                        @Override
                        public SourceSplitEnumerator<TestSplit> createEnumerator(
                                Map<TablePath, CatalogTable> tables,
                                SourceEnumeratorContext context) {

                            return new SourceSplitEnumerator<TestSplit>() {
                                @Override
                                public List<TestSplit> enumerateSplits() {
                                    splitEnumerationCalls.incrementAndGet();
                                    return Arrays.asList(
                                            new TestSplit("split-1"),
                                            new TestSplit("split-2"));
                                }
                            };
                        }

                        @Override
                        public SourceReader<FluxRow, TestSplit> createReader(
                                Map<TablePath, CatalogTable> tables,
                                int batchSize) {
                            return null;
                        }
                    };
                }

                @Override
                public List<CatalogTable> discoverTableSchemas(
                        SourceFactoryContext context) {

                    schemaDiscoveryCalls.incrementAndGet();
                    return Collections.singletonList(table());
                }
            };
        }

        private SinkFactory sinkFactory() {
            return new SinkFactory() {
                @Override
                public String factoryIdentifier() {
                    return "test-sink";
                }

                @Override
                public OptionRule optionRule() {
                    return OptionRule.builder().build();
                }

                @Override
                public SinkPreparer createPreparer(
                        ReadonlyConfig config) {

                    sinkPreparationCalls.incrementAndGet();
                    throw new AssertionError(
                            "Explain must not create a SinkPreparer");
                }
            };
        }

        @SuppressWarnings("unchecked")
        private FactoryRegistry registry(
                TableSourceFactory<TestSplit> sourceFactory,
                SinkFactory sinkFactory)
                throws Exception {

            FactoryRegistry result = new FactoryRegistry(
                    getClass().getClassLoader());

            Field sources = FactoryRegistry.class.getDeclaredField(
                    "sourceFactories");
            sources.setAccessible(true);
            ((Map<String, TableSourceFactory<?>>) sources.get(result))
                    .put("test-source", sourceFactory);

            Field sinks = FactoryRegistry.class.getDeclaredField(
                    "sinkFactories");
            sinks.setAccessible(true);
            ((Map<String, SinkFactory>) sinks.get(result))
                    .put("test-sink", sinkFactory);

            return result;
        }

        private CatalogTable table() {
            TableSchema schema = TableSchema.builder()
                    .column(
                            Column.builder(
                                            "id",
                                            BasicType.LONG_TYPE)
                                    .nullable(false)
                                    .build())
                    .build();
            return CatalogTable.builder(
                            TablePath.of("demo", "orders"),
                            schema)
                    .build();
        }

        @Override
        public void close() {
            registry.close();
        }
    }

    private static final class TestSplit
            implements SourceSplit {

        private static final long serialVersionUID = 1L;

        private final String splitId;

        private TestSplit(String splitId) {
            this.splitId = splitId;
        }

        @Override
        public String splitId() {
            return splitId;
        }

        @Override
        public String dataSetId() {
            return "demo.orders";
        }
    }
}
