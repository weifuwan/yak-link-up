package com.link.up.connector.jdbc.source;

import com.google.auto.service.AutoService;
import com.link.up.api.configuration.ReadonlyConfig;
import com.link.up.api.configuration.util.OptionRule;
import com.link.up.api.connector.preflight.ConnectorPreflightSupport;
import com.link.up.api.connector.schema.ConnectorCapability;
import com.link.up.api.source.SourceFactoryContext;
import com.link.up.api.table.catalog.CatalogTable;
import com.link.up.api.table.factory.TableSourceFactory;
import com.link.up.connector.jdbc.catalog.JdbcCatalogUtils;
import com.link.up.connector.jdbc.client.JdbcConnectionPreflight;
import com.link.up.connector.jdbc.config.JdbcCommonOptions;
import com.link.up.connector.jdbc.config.JdbcSourceConfig;
import com.link.up.connector.jdbc.config.JdbcSourceOptions;
import com.link.up.connector.jdbc.core.dialect.JdbcDialect;
import com.link.up.connector.jdbc.core.dialect.JdbcDialectLoader;
import com.link.up.connector.jdbc.options.MultiTableCommonOptions;

import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * JDBC Source 工厂。
 *
 * <p>主要负责：
 *
 * <ol>
 *   <li>解析并校验 Source 配置；</li>
 *   <li>校验数据库方言是否可用；</li>
 *   <li>创建 JDBC Source；</li>
 *   <li>发现源表结构。</li>
 * </ol>
 *
 * <p>表读取、分片生成和连接管理不在 Factory 中处理。
 */
@AutoService(TableSourceFactory.class)
public final class JdbcSourceFactory
        implements TableSourceFactory<JdbcSourceSplit>,
        ConnectorPreflightSupport {

    private static final String IDENTIFIER = "jdbc";

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
                        ConnectorCapability.CUSTOM_SQL,
                        ConnectorCapability.PARTITION_SPLIT));
    }

    @Override
    public void preflight(
            ReadonlyConfig options,
            ClassLoader classLoader)
            throws Exception {

        JdbcSourceConfig.of(
                Objects.requireNonNull(
                        options,
                        "source options must not be null"));

        JdbcConnectionPreflight.validate(
                options,
                classLoader);
    }

    @Override
    public JdbcSource createSource(
            SourceFactoryContext context)
            throws Exception {

        JdbcSourceConfig config = createConfig(context);
        loadDialect(config);
        return new JdbcSource(config);
    }

    @Override
    public List<CatalogTable> discoverTableSchemas(
            SourceFactoryContext context)
            throws Exception {

        JdbcSourceConfig config = createConfig(context);
        JdbcDialect dialect = loadDialect(config);

        Map<?, JdbcSourceTable> tables =
                JdbcCatalogUtils.getTables(
                        config,
                        dialect);

        List<CatalogTable> result =
                new ArrayList<CatalogTable>(tables.size());

        for (JdbcSourceTable table : tables.values()) {
            result.add(table.getCatalogTable());
        }

        return Collections.unmodifiableList(result);
    }

    @Override
    public OptionRule optionRule() {
        return JdbcCommonOptions.baseConnectionRule()
                .optional(
                        JdbcSourceOptions.TABLE_LIST,
                        JdbcSourceOptions.TABLE_PATH,
                        JdbcSourceOptions.QUERY,
                        JdbcSourceOptions.WHERE_CONDITION,
                        JdbcSourceOptions.FETCH_SIZE,
                        JdbcSourceOptions.READ_CONSISTENCY,
                        JdbcSourceOptions.SPLIT_PLANNING_MODE,
                        JdbcSourceOptions.STATISTICS_QUERY_TIMEOUT,
                        JdbcSourceOptions.SAMPLE_SIZE,
                        JdbcSourceOptions.ALLOW_STATISTICS_FALLBACK,
                        JdbcSourceOptions.NULL_PARTITION_SINGLE_SPLIT,
                        JdbcSourceOptions.PARTITION_COLUMN,
                        JdbcSourceOptions.PARTITION_LOWER_BOUND,
                        JdbcSourceOptions.PARTITION_UPPER_BOUND,
                        JdbcSourceOptions.PARTITION_NUM,
                        MultiTableCommonOptions
                                .MULTI_TABLE_FAILURE_POLICY)
                .build();
    }

    private JdbcSourceConfig createConfig(
            SourceFactoryContext context) {

        Objects.requireNonNull(
                context,
                "context must not be null");

        ReadonlyConfig options =
                Objects.requireNonNull(
                        context.getOptions(),
                        "source options must not be null");

        return JdbcSourceConfig.of(options);
    }

    private JdbcDialect loadDialect(
            JdbcSourceConfig config) {

        return JdbcDialectLoader.load(
                config.getConnectionConfig());
    }
}
