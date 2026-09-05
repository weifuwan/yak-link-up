package com.link.up.connector.jdbc.core.dialect.sqlserver;

import com.link.up.api.table.catalog.Catalog;
import com.link.up.api.table.catalog.Column;
import com.link.up.api.table.catalog.TablePath;
import com.link.up.connector.jdbc.catalog.JdbcCatalogConfig;
import com.link.up.connector.jdbc.catalog.sqlserver.SqlServerCatalog;
import com.link.up.connector.jdbc.config.JdbcConnectionConfig;
import com.link.up.connector.jdbc.core.converter.JdbcRowConverter;
import com.link.up.connector.jdbc.core.dialect.DatabaseIdentifier;
import com.link.up.connector.jdbc.core.dialect.JdbcDialect;
import com.link.up.connector.jdbc.core.dialect.JdbcTypeMapper;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/** SQL Server bounded/offline JDBC dialect. */
public final class SqlServerDialect implements JdbcDialect {

    private static final String DEFAULT_SCHEMA = "dbo";

    private final JdbcConnectionConfig connectionConfig;
    private final SqlServerTypeMapper typeMapper;
    private final String databaseName;

    public SqlServerDialect(JdbcConnectionConfig connectionConfig) {
        if (connectionConfig == null) {
            throw new IllegalArgumentException("connectionConfig must not be null");
        }
        this.connectionConfig = connectionConfig;
        this.typeMapper = new SqlServerTypeMapper();
        this.databaseName = SqlServerJdbcUrl.databaseName(connectionConfig.getUrl());
    }

    @Override
    public String name() {
        return DatabaseIdentifier.SQLSERVER;
    }

    @Override
    public Catalog createCatalog(
            String catalogName, JdbcConnectionConfig connectionConfig) {
        return new SqlServerCatalog(
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
        return new SqlServerJdbcRowConverter();
    }

    /** One part is table, two parts are schema.table, three are database.schema.table. */
    @Override
    public TablePath parseTablePath(String tablePath) {
        if (!JdbcDialect.hasText(tablePath)) {
            throw new IllegalArgumentException("tablePath must not be empty");
        }
        String[] parts = tablePath.trim().split("\\.");
        switch (parts.length) {
            case 1:
                return TablePath.of(parts[0]);
            case 2:
                return TablePath.of(null, parts[0], parts[1]);
            case 3:
                return TablePath.of(parts[0], parts[1], parts[2]);
            default:
                throw new IllegalArgumentException("非法 SQL Server 表路径：" + tablePath);
        }
    }

    @Override
    public String quoteIdentifier(String identifier) {
        if (!JdbcDialect.hasText(identifier)) {
            throw new IllegalArgumentException("identifier must not be empty");
        }
        return "[" + identifier.trim().replace("]", "]]" ) + "]";
    }

    @Override
    public String tableIdentifier(TablePath tablePath) {
        if (tablePath == null) {
            throw new IllegalArgumentException("tablePath must not be null");
        }
        String database = resolveDatabase(tablePath);
        String schema = resolveSchema(tablePath, database);
        String table = quoteIdentifier(tablePath.getTableName());
        return JdbcDialect.hasText(database)
                ? quoteIdentifier(database) + "." + quoteIdentifier(schema) + "." + table
                : quoteIdentifier(schema) + "." + table;
    }

    @Override
    public Optional<String> buildUpsertSql(
            TablePath tablePath,
            List<String> fieldNames,
            List<String> primaryKeys) {
        JdbcDialect.validateFields(fieldNames);
        Set<String> primaryKeySet = JdbcDialect.normalizeFields(primaryKeys);
        if (primaryKeySet.isEmpty()) {
            throw new IllegalArgumentException("SQL Server UPSERT 必须配置主键字段");
        }
        Set<String> fields = new HashSet<String>(fieldNames);
        for (String primaryKey : primaryKeys) {
            if (!fields.contains(primaryKey)) {
                throw new IllegalArgumentException(
                        "SQL Server UPSERT 主键字段不存在：" + primaryKey);
            }
        }

        String placeholders = fieldNames.stream()
                .map(field -> "?")
                .collect(Collectors.joining(", "));
        String sourceColumns = fieldNames.stream()
                .map(this::quoteIdentifier)
                .collect(Collectors.joining(", "));
        String onClause = primaryKeys.stream()
                .map(field -> "TARGET." + quoteIdentifier(field)
                        + " = SOURCE." + quoteIdentifier(field))
                .collect(Collectors.joining(" AND "));
        List<String> updateFields = fieldNames.stream()
                .filter(field -> !primaryKeySet.contains(field))
                .collect(Collectors.toList());

        StringBuilder sql = new StringBuilder()
                .append("MERGE INTO ")
                .append(tableIdentifier(tablePath))
                .append(" AS TARGET USING (VALUES (")
                .append(placeholders)
                .append(")) AS SOURCE (")
                .append(sourceColumns)
                .append(") ON ")
                .append(onClause);

        if (!updateFields.isEmpty()) {
            sql.append(" WHEN MATCHED THEN UPDATE SET ")
                    .append(updateFields.stream()
                            .map(field -> "TARGET." + quoteIdentifier(field)
                                    + " = SOURCE." + quoteIdentifier(field))
                            .collect(Collectors.joining(", ")));
        }

        sql.append(" WHEN NOT MATCHED THEN INSERT (")
                .append(sourceColumns)
                .append(") VALUES (")
                .append(fieldNames.stream()
                        .map(field -> "SOURCE." + quoteIdentifier(field))
                        .collect(Collectors.joining(", ")))
                .append(");");
        return Optional.of(sql.toString());
    }

    @Override
    public PreparedStatement prepareReadStatement(
            Connection connection, String sql, int fetchSize) throws SQLException {
        PreparedStatement statement = connection.prepareStatement(
                sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        if (fetchSize > 0) {
            statement.setFetchSize(fetchSize);
        }
        return statement;
    }

    @Override
    public Map<String, String> defaultConnectionProperties() {
        Map<String, String> result = new LinkedHashMap<String, String>();
        result.put("responseBuffering", "adaptive");
        return Collections.unmodifiableMap(result);
    }

    @Override
    public Optional<String> buildHashPartitionPredicate(
            Column column, int bucket, int bucketCount) {
        if (column == null) {
            throw new IllegalArgumentException("column must not be null");
        }
        if (bucketCount <= 0) {
            throw new IllegalArgumentException("bucketCount must be greater than 0");
        }
        if (bucket < 0 || bucket >= bucketCount) {
            throw new IllegalArgumentException(
                    "bucket must be between 0 and bucketCount - 1");
        }
        return Optional.of(
                "(ABS(CAST(CHECKSUM(CAST(" + quoteIdentifier(column.getName())
                        + " AS NVARCHAR(4000))) AS BIGINT)) % "
                        + bucketCount + ") = " + bucket);
    }

    private String resolveDatabase(TablePath tablePath) {
        if (JdbcDialect.hasText(databaseName)) {
            return databaseName.trim();
        }
        /*
         * When databaseName is absent from the URL, rely on the connection's
         * default database instead of leaking a source database into Sink SQL.
         */
        return null;
    }

    private String resolveSchema(TablePath tablePath, String resolvedDatabase) {
        String pathDatabase = tablePath.getDatabaseName();
        String pathSchema = tablePath.getSchemaName();
        if (JdbcDialect.hasText(pathSchema)
                && (!JdbcDialect.hasText(pathDatabase)
                || !JdbcDialect.hasText(databaseName)
                || databaseName.equalsIgnoreCase(pathDatabase))) {
            return pathSchema.trim();
        }
        if (JdbcDialect.hasText(connectionConfig.getSchema())) {
            return connectionConfig.getSchema().trim();
        }
        if (JdbcDialect.hasText(pathSchema)
                && (!JdbcDialect.hasText(resolvedDatabase)
                || resolvedDatabase.equalsIgnoreCase(pathDatabase))) {
            return pathSchema.trim();
        }
        return DEFAULT_SCHEMA;
    }
}
