package com.link.up.connector.starrocks.source;

import com.google.auto.service.AutoService;
import com.link.up.api.configuration.ReadonlyConfig;
import com.link.up.api.configuration.util.OptionRule;
import com.link.up.api.connector.schema.ConnectorCapability;
import com.link.up.api.source.Source;
import com.link.up.api.source.SourceFactoryContext;
import com.link.up.api.table.catalog.CatalogTable;
import com.link.up.api.table.factory.TableSourceFactory;
import com.link.up.connector.starrocks.config.StarRocksSourceConfig;
import com.link.up.connector.starrocks.config.StarRocksSourceOptions;

import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/** SPI factory for the standalone StarRocks Native Source connector. */
@AutoService(TableSourceFactory.class)
public final class StarRocksSourceFactory
        implements TableSourceFactory<StarRocksSourceSplit> {

    private static final String IDENTIFIER = "starrocks";

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConnectorCapability> capabilities() {
        return Collections.unmodifiableSet(
                EnumSet.of(
                        ConnectorCapability.TABLE_SCHEMA_DISCOVERY,
                        ConnectorCapability.MULTI_TABLE,
                        ConnectorCapability.PARTITION_SPLIT));
    }

    @Override
    public Source<StarRocksSourceSplit> createSource(
            SourceFactoryContext context) {
        return new StarRocksSource(createConfig(context));
    }

    @Override
    public List<CatalogTable> discoverTableSchemas(
            SourceFactoryContext context) {
        return createConfig(context).getCatalogTables();
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(
                        StarRocksSourceOptions.NODE_URLS,
                        StarRocksSourceOptions.USERNAME,
                        StarRocksSourceOptions.DATABASE)
                .optional(
                        StarRocksSourceOptions.PASSWORD,
                        StarRocksSourceOptions.TABLE,
                        StarRocksSourceOptions.TABLE_LIST,
                        StarRocksSourceOptions.SCHEMA_FIELDS,
                        StarRocksSourceOptions.SCAN_FILTER,
                        StarRocksSourceOptions.REQUEST_TABLET_SIZE,
                        StarRocksSourceOptions.SCAN_CONNECT_TIMEOUT_MS,
                        StarRocksSourceOptions.SCAN_QUERY_TIMEOUT_SEC,
                        StarRocksSourceOptions.SCAN_KEEP_ALIVE_MIN,
                        StarRocksSourceOptions.SCAN_BATCH_ROWS,
                        StarRocksSourceOptions.SCAN_MEM_LIMIT,
                        StarRocksSourceOptions.MAX_RETRIES,
                        StarRocksSourceOptions.SCAN_PARAMS)
                .build();
    }

    private StarRocksSourceConfig createConfig(SourceFactoryContext context) {
        Objects.requireNonNull(context, "context must not be null");
        ReadonlyConfig options =
                Objects.requireNonNull(
                        context.getOptions(),
                        "source options must not be null");
        return StarRocksSourceConfig.of(options);
    }
}
