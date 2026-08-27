package com.link.up.framework.connector;

import com.link.up.api.configuration.util.ConfigValidator;
import com.link.up.api.connector.schema.ConnectorCapability;
import com.link.up.api.factory.SinkFactory;
import com.link.up.api.sink.PreparedSinkMetadata;
import com.link.up.api.sink.SinkPrepareContext;
import com.link.up.api.source.Source;
import com.link.up.api.source.SourceFactoryContext;
import com.link.up.api.source.SourceSplit;
import com.link.up.api.table.catalog.CatalogTable;
import com.link.up.api.table.catalog.TablePath;
import com.link.up.api.table.factory.TableSourceFactory;
import com.link.up.framework.classloading.ClassLoaderScope;
import com.link.up.framework.job.ColumnMapping;
import com.link.up.framework.job.JobDefinition;
import com.link.up.framework.job.SinkDefinition;
import com.link.up.framework.job.SourceDefinition;
import com.link.up.framework.mapping.ColumnMappingPlanner;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/** Resolves, validates and prepares connector participation in one Job. */
public final class ConnectorPreparer {

    private final FactoryRegistry registry;
    private final ClassLoader classLoader;

    public ConnectorPreparer(
            FactoryRegistry registry,
            ClassLoader classLoader) {

        this.registry = Objects.requireNonNull(
                registry,
                "registry must not be null");
        this.classLoader = Objects.requireNonNull(
                classLoader,
                "classLoader must not be null");
    }

    /**
     * Validates connector discovery and option rules without creating a Source,
     * discovering metadata or invoking Sink preparation.
     */
    public void validate(JobDefinition definition) {
        JobDefinition job = Objects.requireNonNull(
                definition,
                "definition must not be null");

        validateSourceOptions(job.getSource());
        validateSinkOptions(job.getSink());
    }

    public void validateSourceOptions(
            SourceDefinition definition) {

        SourceDefinition source = Objects.requireNonNull(
                definition,
                "source definition must not be null");
        TableSourceFactory<?> factory =
                registry.getSourceFactory(source.getType());
        ConfigValidator.of(source.getOptions())
                .validate(factory.optionRule());
    }

    public void validateSinkOptions(
            SinkDefinition definition) {

        SinkDefinition sink = Objects.requireNonNull(
                definition,
                "sink definition must not be null");
        SinkFactory factory =
                registry.getSinkFactory(sink.getType());
        ConfigValidator.of(sink.getOptions())
                .validate(factory.optionRule());
    }

    /** Returns a defensive Source capability snapshot without creating it. */
    public Set<ConnectorCapability> sourceCapabilities(
            String connectorId) {

        TableSourceFactory<?> factory =
                registry.getSourceFactory(connectorId);
        return capabilities(
                factory.capabilities(),
                connectorId,
                "source");
    }

    /** Returns a defensive Sink capability snapshot without creating it. */
    public Set<ConnectorCapability> sinkCapabilities(
            String connectorId) {

        SinkFactory factory =
                registry.getSinkFactory(connectorId);
        return capabilities(
                factory.capabilities(),
                connectorId,
                "sink");
    }

    /** Formal runtime preparation. Existing execution semantics remain here. */
    public PreparedJob prepare(JobDefinition definition)
            throws Exception {

        JobDefinition job = Objects.requireNonNull(
                definition,
                "definition must not be null");
        PreparedSource<?> source = prepareSource(job);
        return prepare(job, source);
    }

    /**
     * Creates and discovers the Source before any Sink preparation occurs.
     * This boundary allows capability negotiation to reject unsafe multi-table
     * combinations before target-side DDL or cleanup can run.
     */
    public PreparedSource<?> prepareSource(
            JobDefinition definition)
            throws Exception {

        JobDefinition job = Objects.requireNonNull(
                definition,
                "definition must not be null");
        PreparedSource<?> source = prepareSource(
                job.getSource(),
                job.getExecutionConfig().getSourceParallelism());
        return applyColumnMapping(
                source,
                job.getColumnMapping());
    }

    /** Completes formal Sink preparation for an already prepared Source. */
    public PreparedJob prepare(
            JobDefinition definition,
            PreparedSource<?> preparedSource)
            throws Exception {

        JobDefinition job = Objects.requireNonNull(
                definition,
                "definition must not be null");
        PreparedSource<?> source = Objects.requireNonNull(
                preparedSource,
                "preparedSource must not be null");

        Map<String, List<PreparedSink>> sinks = prepareSinks(
                job.getSink(),
                job.getExecutionConfig().getSinkParallelism(),
                source.getOutputTables());

        return new PreparedJob(
                job.getName(),
                source,
                sinks,
                job.getExecutionConfig());
    }

    /**
     * Planning preparation used by Explain.
     *
     * <p>Source creation, schema discovery and split enumeration remain real so
     * the resulting JobGraph reflects executable topology. Sink option rules are
     * validated, but {@code SinkPreparer} is never invoked because it may create,
     * truncate or otherwise mutate target tables.</p>
     */
    public PreparedJob prepareForExplain(
            JobDefinition definition)
            throws Exception {

        JobDefinition job = Objects.requireNonNull(
                definition,
                "definition must not be null");
        return prepareForExplain(
                job,
                prepareSource(job));
    }

    /** Builds Explain-only Sink stubs for an already prepared Source. */
    public PreparedJob prepareForExplain(
            JobDefinition definition,
            PreparedSource<?> preparedSource) {

        JobDefinition job = Objects.requireNonNull(
                definition,
                "definition must not be null");
        PreparedSource<?> source = Objects.requireNonNull(
                preparedSource,
                "preparedSource must not be null");

        Map<String, List<PreparedSink>> sinks = prepareExplainSinks(
                job.getSink(),
                job.getExecutionConfig().getSinkParallelism(),
                source.getOutputTables());

        return new PreparedJob(
                job.getName(),
                source,
                sinks,
                job.getExecutionConfig());
    }

    private <SplitT extends SourceSplit> PreparedSource<SplitT>
    applyColumnMapping(
            PreparedSource<SplitT> source,
            ColumnMapping mapping) {

        ColumnMappingPlanner.Result result =
                new ColumnMappingPlanner().plan(
                        source.getTables(),
                        mapping);

        return new PreparedSource<SplitT>(
                source.getFactoryIdentifier(),
                source.getSource(),
                source.getTables(),
                result.getOutputTables(),
                result.getPlans(),
                source.getClassLoader());
    }

    private PreparedSource<?> prepareSource(
            SourceDefinition definition,
            int sourceParallelism)
            throws Exception {

        TableSourceFactory<?> factory =
                registry.getSourceFactory(definition.getType());
        ConfigValidator.of(definition.getOptions())
                .validate(factory.optionRule());

        SourceFactoryContext context = new SourceFactoryContext(
                definition.getOptions(),
                classLoader);

        return createPreparedSource(
                definition.getType(),
                factory,
                context,
                sourceParallelism);
    }

    private <SplitT extends SourceSplit> PreparedSource<SplitT>
    createPreparedSource(
            String identifier,
            TableSourceFactory<SplitT> factory,
            SourceFactoryContext context,
            int sourceParallelism)
            throws Exception {

        Source<SplitT> source = createSourceInScope(
                factory,
                context);
        if (source == null) {
            throw new ConnectorException(
                    "Source factory '"
                            + identifier
                            + "' returned a null source");
        }

        source.validateParallelism(sourceParallelism);

        List<CatalogTable> catalogTables;
        try (ClassLoaderScope ignored = ClassLoaderScope.open(
                registry.getClassLoader(factory))) {
            catalogTables = factory.discoverTableSchemas(context);
        }

        Map<TablePath, CatalogTable> tableMap = buildTableMap(
                identifier,
                catalogTables);

        return new PreparedSource<SplitT>(
                identifier,
                source,
                tableMap,
                registry.getClassLoader(factory));
    }

    private Map<String, List<PreparedSink>> prepareSinks(
            SinkDefinition definition,
            int parallelism,
            Map<TablePath, CatalogTable> sourceTables)
            throws Exception {

        SinkFactory factory =
                registry.getSinkFactory(definition.getType());
        ConfigValidator.of(definition.getOptions())
                .validate(factory.optionRule());

        Map<String, List<PreparedSink>> result =
                new LinkedHashMap<String, List<PreparedSink>>();

        for (Map.Entry<TablePath, CatalogTable> table :
                sourceTables.entrySet()) {

            Map<TablePath, CatalogTable> oneTable =
                    oneTable(table);
            PreparedSinkMetadata metadata;

            try (ClassLoaderScope ignored = ClassLoaderScope.open(
                    registry.getClassLoader(factory))) {
                metadata = factory.createPreparer(
                                definition.getOptions())
                        .prepare(
                                new SinkPrepareContext(
                                        definition.getOptions(),
                                        oneTable));
            }

            if (metadata == null) {
                throw new ConnectorException(
                        "Sink factory '"
                                + definition.getType()
                                + "' returned null preparation metadata");
            }

            result.put(
                    table.getKey().toString(),
                    preparedSinks(
                            definition,
                            factory,
                            metadata,
                            parallelism));
        }

        return result;
    }

    private Map<String, List<PreparedSink>> prepareExplainSinks(
            SinkDefinition definition,
            int parallelism,
            Map<TablePath, CatalogTable> sourceTables) {

        SinkFactory factory =
                registry.getSinkFactory(definition.getType());
        ConfigValidator.of(definition.getOptions())
                .validate(factory.optionRule());

        Map<String, List<PreparedSink>> result =
                new LinkedHashMap<String, List<PreparedSink>>();

        for (Map.Entry<TablePath, CatalogTable> table :
                sourceTables.entrySet()) {

            PreparedSinkMetadata metadata =
                    new PreparedSinkMetadata(
                            oneTable(table));

            result.put(
                    table.getKey().toString(),
                    preparedSinks(
                            definition,
                            factory,
                            metadata,
                            parallelism));
        }

        return result;
    }

    private List<PreparedSink> preparedSinks(
            SinkDefinition definition,
            SinkFactory factory,
            PreparedSinkMetadata metadata,
            int parallelism) {

        List<PreparedSink> sinks =
                new ArrayList<PreparedSink>(parallelism);

        for (int index = 0; index < parallelism; index++) {
            sinks.add(
                    new PreparedSink(
                            definition.getType(),
                            factory,
                            definition.getOptions(),
                            metadata,
                            registry.getClassLoader(factory)));
        }

        return sinks;
    }

    private Map<TablePath, CatalogTable> oneTable(
            Map.Entry<TablePath, CatalogTable> table) {

        Map<TablePath, CatalogTable> result =
                new LinkedHashMap<TablePath, CatalogTable>();
        result.put(table.getKey(), table.getValue());
        return result;
    }

    private <SplitT extends SourceSplit> Source<SplitT>
    createSourceInScope(
            TableSourceFactory<SplitT> factory,
            SourceFactoryContext context)
            throws Exception {

        try (ClassLoaderScope ignored = ClassLoaderScope.open(
                registry.getClassLoader(factory))) {
            return factory.createSource(context);
        }
    }

    private Map<TablePath, CatalogTable> buildTableMap(
            String factoryIdentifier,
            List<CatalogTable> catalogTables) {

        if (catalogTables == null) {
            throw new ConnectorException(
                    "Source factory '"
                            + factoryIdentifier
                            + "' returned null catalog tables");
        }
        if (catalogTables.isEmpty()) {
            throw new ConnectorException(
                    "No source tables were discovered by factory '"
                            + factoryIdentifier
                            + "'");
        }

        Map<TablePath, CatalogTable> result =
                new LinkedHashMap<TablePath, CatalogTable>();

        for (CatalogTable catalogTable : catalogTables) {
            if (catalogTable == null) {
                throw new ConnectorException(
                        "Source factory '"
                                + factoryIdentifier
                                + "' returned a null CatalogTable");
            }

            TablePath tablePath = catalogTable.getTablePath();
            if (tablePath == null) {
                throw new ConnectorException(
                        "CatalogTable returned by source factory '"
                                + factoryIdentifier
                                + "' has no TablePath");
            }

            CatalogTable previous = result.put(
                    tablePath,
                    catalogTable);
            if (previous != null) {
                throw new ConnectorException(
                        "Duplicated source table path: "
                                + tablePath);
            }
        }

        return result;
    }

    private Set<ConnectorCapability> capabilities(
            Set<ConnectorCapability> values,
            String connectorId,
            String role) {

        if (values == null) {
            throw new ConnectorException(
                    role
                            + " factory '"
                            + connectorId
                            + "' returned null capabilities");
        }

        Set<ConnectorCapability> copy =
                new LinkedHashSet<ConnectorCapability>();

        for (ConnectorCapability capability : values) {
            copy.add(
                    Objects.requireNonNull(
                            capability,
                            "capabilities must not contain null"));
        }

        return Collections.unmodifiableSet(copy);
    }
}
