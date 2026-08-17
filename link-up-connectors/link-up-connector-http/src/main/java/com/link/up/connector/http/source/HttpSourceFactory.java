package com.link.up.connector.http.source;

import com.google.auto.service.AutoService;
import com.link.up.api.configuration.ReadonlyConfig;
import com.link.up.api.configuration.util.OptionRule;
import com.link.up.api.connector.schema.ConnectorCapability;
import com.link.up.api.factory.SourceFactory;
import com.link.up.api.source.Source;
import com.link.up.api.source.SourceFactoryContext;
import com.link.up.api.source.SourceSplit;
import com.link.up.api.table.catalog.Catalog;
import com.link.up.api.table.catalog.CatalogTable;
import com.link.up.api.table.catalog.TablePath;
import com.link.up.api.table.factory.TableSourceFactory;
import com.link.up.connector.http.catalog.HttpCatalog;
import com.link.up.connector.http.catalog.HttpCatalogConfig;
import com.link.up.connector.http.config.HttpSourceConfig;
import com.link.up.connector.http.config.HttpSourceOptions;

import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * HTTP Source 工厂。
 *
 * <p>负责：
 * <ol>
 *   <li>解析并校验 Source 配置</li>
 *   <li>解析用户定义的 Schema</li>
 *   <li>创建 HTTP Source</li>
 *   <li>返回 Schema 作为 discoverTableSchemas 结果</li>
 * </ol>
 */
@AutoService(TableSourceFactory.class)
public final class HttpSourceFactory
        implements TableSourceFactory<HttpSourceSplit> {

    private static final String IDENTIFIER = "http";

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConnectorCapability> capabilities() {
        return Collections.unmodifiableSet(
                EnumSet.of(
                        ConnectorCapability.TABLE_SCHEMA_DISCOVERY));
    }

    @Override
    public Source<HttpSourceSplit> createSource(
            SourceFactoryContext context) throws Exception {

        HttpSourceConfig config = createConfig(context);
        return new HttpSource(config);
    }

    @Override
    public List<CatalogTable> discoverTableSchemas(
            SourceFactoryContext context) throws Exception {

        HttpSourceConfig sourceConfig = createConfig(context);

        HttpCatalogConfig catalogConfig =
                HttpCatalogConfig.fromSourceConfig(sourceConfig);

        try (Catalog catalog = new HttpCatalog(catalogConfig)) {
            catalog.open();

            List<TablePath> tablePaths =
                    catalog.listTables(null, null);

            if (tablePaths.isEmpty()) {
                return Collections.emptyList();
            }

            TablePath tablePath = tablePaths.get(0);
            CatalogTable table = catalog.getTable(tablePath);

            return Collections.singletonList(table);
        }
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(HttpSourceOptions.URL)
                .optional(
                        HttpSourceOptions.METHOD,
                        HttpSourceOptions.HEADERS,
                        HttpSourceOptions.PARAMS,
                        HttpSourceOptions.BODY,
                        HttpSourceOptions.FORMAT,
                        HttpSourceOptions.SCHEMA_FIELDS,
                        HttpSourceOptions.CONTENT_FIELD,
                        HttpSourceOptions.JSON_FIELD,
                        HttpSourceOptions.PAGE_FIELD,
                        HttpSourceOptions.TOTAL_PAGE_SIZE,
                        HttpSourceOptions.PAGE_BATCH_SIZE,
                        HttpSourceOptions.START_PAGE_NUMBER,
                        HttpSourceOptions.PAGE_TYPE,
                        HttpSourceOptions.CURSOR_FIELD,
                        HttpSourceOptions.CURSOR_RESPONSE_FIELD,
                        HttpSourceOptions.USE_PLACEHOLDER_REPLACEMENT,
                        HttpSourceOptions.RETRY,
                        HttpSourceOptions.RETRY_BACKOFF_MULTIPLIER_MS,
                        HttpSourceOptions.RETRY_BACKOFF_MAX_MS,
                        HttpSourceOptions.RETRYABLE_STATUS_CODES,
                        HttpSourceOptions.RETRY_JITTER_MS,
                        HttpSourceOptions.CONNECT_TIMEOUT_MS,
                        HttpSourceOptions.SOCKET_TIMEOUT_MS,
                        HttpSourceOptions.POOL_MAX_IDLE_CONNECTIONS,
                        HttpSourceOptions.POOL_KEEP_ALIVE_DURATION_MS,
                        HttpSourceOptions.ENABLE_MULTI_LINES,
                        HttpSourceOptions.JSON_FIELD_MISSED_RETURN_NULL)
                .build();
    }

    private HttpSourceConfig createConfig(SourceFactoryContext context) {
        Objects.requireNonNull(context, "context must not be null");
        ReadonlyConfig options = Objects.requireNonNull(
                context.getOptions(), "source options must not be null");
        return HttpSourceConfig.of(options);
    }
}
