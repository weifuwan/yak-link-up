package com.link.up.framework.planning;

import com.link.up.api.configuration.ReadonlyConfig;
import com.link.up.api.configuration.util.OptionRule;
import com.link.up.api.connector.schema.ConnectorCapability;
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
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class JobPlanCapabilityBoundaryTest {

    @Test
    public void multiTableRejectionMustHappenBeforeSinkPreparation()
            throws Exception {

        final AtomicInteger sourceDiscoveryCalls =
                new AtomicInteger();
        final AtomicInteger splitEnumerationCalls =
                new AtomicInteger();
        final AtomicInteger sinkPreparationCalls =
                new AtomicInteger();
        FactoryRegistry registry = registry(
                sourceDiscoveryCalls,
                splitEnumerationCalls,
                sinkPreparationCalls);

        try {
            JobPlanExplainer explainer =
                    new JobPlanExplainer(
                            new ConnectorPreparer(
                                    registry,
                                    getClass().getClassLoader()),
                            new JobPlanner());

            try {
                explainer.explain(definition());
                fail("Expected multi-table capability rejection");
            } catch (PlanningException expected) {
                assertEquals(
                        PlanningErrorCode.REQUIRED_CAPABILITY_MISSING,
                        expected.getPlanningErrorCode());
                assertEquals(
                        "SINK",
                        expected.getParams().get("role"));
                assertEquals(
                        "MULTI_TABLE",
                        expected.getParams().get("capability"));
            }

            assertEquals(1, sourceDiscoveryCalls.get());
            assertEquals(0, splitEnumerationCalls.get());
            assertEquals(0, sinkPreparationCalls.get());
        } finally {
            registry.close();
        }
    }

    private JobDefinition definition() {
        ReadonlyConfig options = ReadonlyConfig.fromMap(
                Collections.<String, Object>emptyMap());
        return new JobDefinition(
                "multi-table-boundary",
                new SourceDefinition("test-source", options),
                new SinkDefinition("test-sink", options),
                new ExecutionConfig(100, 1, 1, 32));
    }

    @SuppressWarnings("unchecked")
    private FactoryRegistry registry(
            final AtomicInteger sourceDiscoveryCalls,
            final AtomicInteger splitEnumerationCalls,
            final AtomicInteger sinkPreparationCalls)
            throws Exception {

        FactoryRegistry registry = new FactoryRegistry(
                getClass().getClassLoader());

        TableSourceFactory<TestSplit> sourceFactory =
                new TableSourceFactory<TestSplit>() {
                    @Override
                    public String factoryIdentifier() {
                        return "test-source";
                    }

                    @Override
                    public java.util.Set<ConnectorCapability> capabilities() {
                        return EnumSet.of(
                                ConnectorCapability.TABLE_SCHEMA_DISCOVERY,
                                ConnectorCapability.MULTI_TABLE);
                    }

                    @Override
                    public OptionRule optionRule() {
                        return OptionRule.builder().build();
                    }

                    @Override
                    public Source<TestSplit> createSource(
                            SourceFactoryContext context) {
                        return source(splitEnumerationCalls);
                    }

                    @Override
                    public List<CatalogTable> discoverTableSchemas(
                            SourceFactoryContext context) {
                        sourceDiscoveryCalls.incrementAndGet();
                        return Arrays.asList(
                                table("orders"),
                                table("customers"));
                    }
                };

        SinkFactory sinkFactory = new SinkFactory() {
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
                        "Capability rejection must happen before Sink preparation");
            }
        };

        Field sources = FactoryRegistry.class.getDeclaredField(
                "sourceFactories");
        sources.setAccessible(true);
        ((Map<String, TableSourceFactory<?>>) sources.get(registry))
                .put("test-source", sourceFactory);

        Field sinks = FactoryRegistry.class.getDeclaredField(
                "sinkFactories");
        sinks.setAccessible(true);
        ((Map<String, SinkFactory>) sinks.get(registry))
                .put("test-sink", sinkFactory);

        return registry;
    }

    private Source<TestSplit> source(
            final AtomicInteger splitEnumerationCalls) {

        return new Source<TestSplit>() {
            @Override
            public SourceSplitEnumerator<TestSplit> createEnumerator(
                    Map<TablePath, CatalogTable> tables,
                    SourceEnumeratorContext context) {
                splitEnumerationCalls.incrementAndGet();
                throw new AssertionError(
                        "Split discovery must not start after capability rejection");
            }

            @Override
            public SourceReader<FluxRow, TestSplit> createReader(
                    Map<TablePath, CatalogTable> tables,
                    int batchSize) {
                return null;
            }
        };
    }

    private CatalogTable table(String name) {
        TableSchema schema = TableSchema.builder()
                .column(
                        Column.builder(
                                        "id",
                                        BasicType.LONG_TYPE)
                                .nullable(false)
                                .build())
                .build();
        return CatalogTable.builder(
                        TablePath.of("demo", name),
                        schema)
                .build();
    }

    private static final class TestSplit
            implements SourceSplit {

        private static final long serialVersionUID = 1L;

        @Override
        public String splitId() {
            return "unused";
        }

        @Override
        public String dataSetId() {
            return "demo.orders";
        }
    }
}
