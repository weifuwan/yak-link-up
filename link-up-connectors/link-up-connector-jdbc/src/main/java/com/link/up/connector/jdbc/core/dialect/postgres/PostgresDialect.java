package com.link.up.connector.jdbc.core.dialect.postgres;

import com.link.up.api.table.catalog.Catalog;
import com.link.up.api.table.catalog.Column;
import com.link.up.api.table.catalog.TablePath;
import com.link.up.connector.jdbc.catalog.JdbcCatalogConfig;
import com.link.up.connector.jdbc.catalog.postgres.PostgresCatalog;
import com.link.up.connector.jdbc.config.JdbcConnectionConfig;
import com.link.up.connector.jdbc.config.ReadConsistency;
import com.link.up.connector.jdbc.core.converter.JdbcRowConverter;
import com.link.up.connector.jdbc.core.dialect.DatabaseIdentifier;
import com.link.up.connector.jdbc.core.dialect.JdbcDialect;
import com.link.up.connector.jdbc.core.dialect.JdbcTypeMapper;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * PostgreSQL 离线 JDBC 方言。
 *
 * <p>只包含全量/离线 Source、批量 Sink、Catalog、类型映射和分片所需能力，
 * 不包含 WAL、CDC、LSN、replication slot、流式 checkpoint 或 XA。
 */
public final class PostgresDialect
        implements JdbcDialect {

    private static final String DEFAULT_SCHEMA = "public";

    private final JdbcConnectionConfig connectionConfig;
    private final PostgresTypeMapper typeMapper;

    public PostgresDialect(
            JdbcConnectionConfig connectionConfig) {

        if (connectionConfig == null) {
            throw new IllegalArgumentException(
                    "connectionConfig must not be null");
        }

        this.connectionConfig =
                connectionConfig;

        this.typeMapper =
                new PostgresTypeMapper();
    }

    @Override
    public String name() {
        return DatabaseIdentifier.POSTGRESQL;
    }

    @Override
    public Catalog createCatalog(
            String catalogName,
            JdbcConnectionConfig connectionConfig) {

        return new PostgresCatalog(
                catalogName,
                new JdbcCatalogConfig(
                        connectionConfig.getUrl(),
                        connectionConfig.getUsername(),
                        connectionConfig.getPassword(),
                        connectionConfig.getDriverName(),
                        connectionConfig.getProperties(),
                        false),
                connectionConfig.getSchema());
    }

    @Override
    public JdbcTypeMapper typeMapper() {
        return typeMapper;
    }

    @Override
    public JdbcRowConverter rowConverter() {
        return new PostgresJdbcRowConverter();
    }

    @Override
    public Set<ReadConsistency> supportedReadConsistencies() {
        return EnumSet.of(
                ReadConsistency.BEST_EFFORT,
                ReadConsistency.SINGLE_CONNECTION_SNAPSHOT);
    }

    /**
     * PostgreSQL 两段路径按 schema.table 解释，而不是 database.table。
     */
    @Override
    public TablePath parseTablePath(
            String tablePath) {

        if (!JdbcDialect.hasText(tablePath)) {
            throw new IllegalArgumentException(
                    "tablePath must not be empty");
        }

        String[] parts =
                tablePath.trim()
                        .split("\\.");

        switch (parts.length) {
            case 1:
                return TablePath.of(
                        parts[0]);

            case 2:
                return TablePath.of(
                        null,
                        parts[0],
                        parts[1]);

            case 3:
                return TablePath.of(
                        parts[0],
                        parts[1],
                        parts[2]);

            default:
                throw new IllegalArgumentException(
                        "非法 PostgreSQL 表路径："
                                + tablePath);
        }
    }

    @Override
    public String quoteIdentifier(
            String identifier) {

        if (!JdbcDialect.hasText(identifier)) {
            throw new IllegalArgumentException(
                    "identifier must not be empty");
        }

        return "\""
                + identifier.trim()
                .replace("\"", "\"\"")
                + "\"";
    }

    /**
     * PostgreSQL SQL 标识只包含 schema.table，database 由 JDBC URL 决定。
     */
    @Override
    public String tableIdentifier(
            TablePath tablePath) {

        if (tablePath == null) {
            throw new IllegalArgumentException(
                    "tablePath must not be null");
        }

        String schema =
                tablePath.getSchemaName();

        if (!JdbcDialect.hasText(schema)) {
            schema =
                    connectionConfig.getSchema();
        }

        if (!JdbcDialect.hasText(schema)) {
            schema = DEFAULT_SCHEMA;
        }

        return quoteIdentifier(schema)
                + "."
                + quoteIdentifier(
                tablePath.getTableName());
    }

    @Override
    public Optional<String> buildUpsertSql(
            TablePath tablePath,
            List<String> fieldNames,
            List<String> primaryKeys) {

        JdbcDialect.validateFields(fieldNames);

        Set<String> primaryKeySet =
                JdbcDialect.normalizeFields(
                        primaryKeys);

        if (primaryKeySet.isEmpty()) {
            throw new IllegalArgumentException(
                    "PostgreSQL UPSERT 必须配置主键字段");
        }

        String conflictFields =
                primaryKeys.stream()
                        .map(this::quoteIdentifier)
                        .collect(
                                Collectors.joining(", "));

        String updateClause =
                fieldNames.stream()
                        .filter(
                                field ->
                                        !primaryKeySet.contains(
                                                field))
                        .map(
                                field ->
                                        quoteIdentifier(field)
                                                + " = EXCLUDED."
                                                + quoteIdentifier(field))
                        .collect(
                                Collectors.joining(", "));

        String conflictAction =
                updateClause.isEmpty()
                        ? "DO NOTHING"
                        : "DO UPDATE SET "
                        + updateClause;

        return Optional.of(
                buildInsertSql(
                        tablePath,
                        fieldNames)
                        + " ON CONFLICT ("
                        + conflictFields
                        + ") "
                        + conflictAction);
    }

    @Override
    public PreparedStatement prepareReadStatement(
            Connection connection,
            String sql,
            int fetchSize)
            throws SQLException {

        /*
         * PostgreSQL JDBC 只有在事务中才使用 cursor fetch，
         * 因此在执行大表离线读取前关闭 autoCommit。
         */
        if (connection.getAutoCommit()) {
            connection.setAutoCommit(false);
        }

        PreparedStatement statement =
                connection.prepareStatement(
                        sql,
                        ResultSet.TYPE_FORWARD_ONLY,
                        ResultSet.CONCUR_READ_ONLY);

        if (fetchSize > 0) {
            statement.setFetchSize(fetchSize);
        }

        return statement;
    }

    @Override
    public Map<String, String>
    defaultConnectionProperties() {

        Map<String, String> result =
                new LinkedHashMap<String, String>();

        result.put(
                "reWriteBatchedInserts",
                "true");

        return Collections.unmodifiableMap(
                result);
    }

    @Override
    public Optional<String> buildHashPartitionPredicate(
            Column column,
            int bucket,
            int bucketCount) {

        if (column == null) {
            throw new IllegalArgumentException(
                    "column must not be null");
        }

        if (bucketCount <= 0) {
            throw new IllegalArgumentException(
                    "bucketCount must be greater than 0");
        }

        if (bucket < 0
                || bucket >= bucketCount) {
            throw new IllegalArgumentException(
                    "bucket must be between 0 and bucketCount - 1");
        }

        String field =
                quoteIdentifier(
                        column.getName());

        return Optional.of(
                "MOD(ABS(HASHTEXT(CAST("
                        + field
                        + " AS TEXT))::BIGINT), "
                        + bucketCount
                        + ") = "
                        + bucket);
    }
}
