package com.link.up.framework.planning;

import com.link.up.api.configuration.ReadonlyConfig;
import com.link.up.api.configuration.util.OptionRule;
import com.link.up.api.connector.schema.ConnectorCapability;
import com.link.up.api.exception.FluxErrorCategory;
import com.link.up.api.exception.FluxErrorPhase;
import com.link.up.api.factory.SinkFactory;
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
import com.link.up.framework.connector.PreparedSource;
import com.link.up.framework.job.ExecutionConfig;
import com.link.up.framework.job.JobCapabilityRequirements;
import com.link.up.framework.job.JobDefinition;
import com.link.up.framework.job.SinkDefinition;
import com.link.up.framework.job.SourceDefinition;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class CapabilityNegotiatorTest {

    @Test
    public void missingPreferredCapabilityShouldDegradeButRemainValid()
            throws Exception {

        FactoryRegistry registry = registry(
                EnumSet.of(
                        ConnectorCapability.TABLE_SCHEMA_DISCOVERY),
                EnumSet.of(
                        ConnectorCapability.TWO_PHASE_COMMIT));

        try {
            JobCapabilityRequirements requirements =
                    new JobCapabilityRequirements(
                            Collections.singleton(
                                    ConnectorCapability.TABLE_SCHEMA_DISCOVERY),
                            Collections.singleton(
                                    ConnectorCapability.PARTITION_SPLIT),
                            Collections.singleton(
                                    ConnectorCapability.TWO_PHASE_COMMIT),
                            Collections.<ConnectorCapability>emptySet());

            CapabilityNegotiation result = negotiator(registry)
                    .negotiate(definition(requirements));

            assertEquals(
                    CapabilityNegotiation.Status.DEGRADED,
                    result.getStatus());
            assertEquals(
                    Collections.singletonList(
                            ConnectorCapability.PARTITION_SPLIT),
                    result.getSource().getMissingPreferred());
            assertTrue(
                    result.getSource().getMissingRequired().isEmpty());
            assertTrue(
                    result.getSink().getMissingRequired().isEmpty());
        } finally {
            registry.close();
        }
    }

    @Test
    public void missingRequiredCapabilityShouldRaiseStructuredError()
            throws Exception {

        FactoryRegistry registry = registry(
                Collections.<ConnectorCapability>emptySet(),
                Collections.<ConnectorCapability>emptySet());

        try {
            JobCapabilityRequirements requirements =
                    new JobCapabilityRequirements(
                            Collections.<ConnectorCapability>emptySet(),
                            Collections.<ConnectorCapability>emptySet(),
                            Collections.singleton(
                                    ConnectorCapability.UPSERT),
                            Collections.<ConnectorCapability>emptySet());
            CapabilityNegotiator negotiator =
                    negotiator(registry);
            CapabilityNegotiation result =
                    negotiator.negotiate(
                            definition(requirements));

            try {
                negotiator.requireSatisfied(result);
                fail("Expected required capability rejection");
            } catch (PlanningException expected) {
                assertEquals(
                        PlanningErrorCode.REQUIRED_CAPABILITY_MISSING,
                        expected.getPlanningErrorCode());
                assertEquals(
                        FluxErrorCategory.CAPABILITY,
                        expected.getErrorCategory());
                assertEquals(
                        FluxErrorPhase.CAPABILITY_NEGOTIATION,
                        expected.getErrorPhase());
                assertEquals(
                        "SINK",
                        expected.getParams().get("role"));
                assertEquals(
                        "UPSERT",
                        expected.getParams().get("capability"));
            }
        } finally {
            registry.close();
        }
    }

    @Test
    public void multiTableTopologyShouldDeriveSourceAndSinkRequirement()
            throws Exception {

        FactoryRegistry registry = registry(
                EnumSet.of(
                        ConnectorCapability.TABLE_SCHEMA_DISCOVERY,
                        ConnectorCapability.MULTI_TABLE),
                Collections.<ConnectorCapability>emptySet());

        try {
            CapabilityNegotiation result = negotiator(registry)
                    .negotiate(
                            definition(
                                    JobCapabilityRequirements.empty()),
                            preparedSource());

            assertEquals(
                    CapabilityNegotiation.Status.REJECTED,
                    result.getStatus());
            assertTrue(
                    result.getSource()
                            .getDerivedRequired()
                            .contains(
                                    ConnectorCapability.MULTI_TABLE));
            assertTrue(
                    result.getSink()
                            .getDerivedRequired()
                            .contains(
                                    ConnectorCapability.MULTI_TABLE));
            assertTrue(
                    result.getSink()
                            .getMissingRequired()
                            .contains(
                                    ConnectorCapability.MULTI_TABLE));
        } finally {
            registry.close();
        }
    }

    private CapabilityNegotiator negotiator(
            FactoryRegistry registry) {

        return new CapabilityNegotiator(
                new ConnectorPreparer(
                        registry,
                        getClass().getClassLoader()));
    }

    private JobDefinition definition(
            JobCapabilityRequirements requirements) {

        ReadonlyConfig options = ReadonlyConfig.fromMap(
                Collections.<String, Object>emptyMap());

        return new JobDefinition(
                "capability-negotiation",
                new SourceDefinition("test-source", options),
                new SinkDefinition("test-sink", options),
                new ExecutionConfig(100, 1, 1, 32),
                null,
                requirements);
    }

    private PreparedSource<TestSplit> preparedSource() {
        Map<TablePath, CatalogTable> tables =
                new LinkedHashMap<TablePath, CatalogTable>();
        tables.put(
                TablePath.of("demo", "orders"),
                table("orders"));
        tables.put(
                TablePath.of("demo", "customers"),
                table("customers"));

        return new PreparedSource<TestSplit>(
                "test-source",
                source(),
                tables);
    }

    private Source<TestSplit> source() {
        return new Source<TestSplit>() {
            @Override
            public SourceSplitEnumerator<TestSplit> createEnumerator(
                    Map<TablePath, CatalogTable> tables,
                    SourceEnumeratorContext context) {
                return null;
            }

            @Override
            public SourceReader<FluxRow, TestSplit> createReader(
                    Map<TablePath, CatalogTable> tables,
                    int batchSize) {
                return null;
            }
        };
    }

    private CatalogTable table(String tableName) {
        TableSchema schema = TableSchema.builder()
                .column(
                        Column.builder(
                                        "id",
                                        BasicType.LONG_TYPE)
                                .nullable(false)
                                .build())
                .build();
        return CatalogTable.builder(
                        TablePath.of("demo", tableName),
                        schema)
                .build();
    }

    @SuppressWarnings("unchecked")
    private FactoryRegistry registry(
            final Set<ConnectorCapability> sourceCapabilities,
            final Set<ConnectorCapability> sinkCapabilities)
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
                    public Set<ConnectorCapability> capabilities() {
                        return sourceCapabilities;
                    }

                    @Override
                    public OptionRule optionRule() {
                        return OptionRule.builder().build();
                    }

                    @Override
                    public Source<TestSplit> createSource(
                            SourceFactoryContext context) {
                        return source();
                    }

                    @Override
                    public List<CatalogTable> discoverTableSchemas(
                            SourceFactoryContext context) {
                        return Collections.singletonList(
                                table("orders"));
                    }
                };

        SinkFactory sinkFactory = new SinkFactory() {
            @Override
            public String factoryIdentifier() {
                return "test-sink";
            }

            @Override
            public Set<ConnectorCapability> capabilities() {
                return sinkCapabilities;
            }

            @Override
            public OptionRule optionRule() {
                return OptionRule.builder().build();
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

    private static final class TestSplit
            implements SourceSplit {

        private static final long serialVersionUID = 1L;

        @Override
        public String splitId() {
            return "test-split";
        }

        @Override
        public String dataSetId() {
            return "demo.orders";
        }
    }
}
