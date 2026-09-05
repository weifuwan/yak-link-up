package com.link.up.connector.jdbc.core.dialect.yashandb;

import com.link.up.api.table.catalog.Catalog;
import com.link.up.api.table.catalog.Column;
import com.link.up.api.table.catalog.TablePath;
import com.link.up.connector.jdbc.catalog.JdbcCatalogConfig;
import com.link.up.connector.jdbc.catalog.yashandb.YashanDbCatalog;
import com.link.up.connector.jdbc.config.JdbcConnectionConfig;
import com.link.up.connector.jdbc.core.converter.JdbcRowConverter;
import com.link.up.connector.jdbc.core.dialect.DatabaseIdentifier;
import com.link.up.connector.jdbc.core.dialect.JdbcDialect;
import com.link.up.connector.jdbc.core.dialect.JdbcTypeMapper;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/** YashanDB bounded/offline JDBC dialect. */
public final class YashanDbDialect implements JdbcDialect {

    private static final int DEFAULT_FETCH_SIZE = 128;

    private final JdbcConnectionConfig connectionConfig;
    private final YashanDbTypeMapper typeMapper;
    private final String logicalDatabaseName;

    public YashanDbDialect(JdbcConnectionConfig connectionConfig) {
        if (connectionConfig == null) {
            throw new IllegalArgumentException("connectionConfig must not be null");
        }
        this.connectionConfig = connectionConfig;
        this.typeMapper = new YashanDbTypeMapper();
        this.logicalDatabaseName = YashanDbJdbcUrl.databaseName(connectionConfig.getUrl());
        if (!JdbcDialect.hasText(logicalDatabaseName)) {
            throw new IllegalArgumentException(
                    "YashanDB JDBC URL 必须包含 database_name，例如 "
                            + "jdbc:yasdb://127.0.0.1:1688/YASDB");
        }
    }

    @Override
    public String name() {
        return DatabaseIdentifier.YASHANDB;
    }

    @Override
    public Catalog createCatalog(
            String catalogName,
            JdbcConnectionConfig connectionConfig) {

        return new YashanDbCatalog(
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
        return new YashanDbJdbcRowConverter();
    }

    @Override
    public TablePath parseTablePath(String tablePath) {
        if (!JdbcDialect.hasText(tablePath)) {
            throw new IllegalArgumentException("tablePath must not be empty");
        }

        List<String> parts = splitIdentifierPath(tablePath.trim());
        switch (parts.size()) {
            case 1:
                return TablePath.of(normalizePathPart(parts.get(0)));
            case 2:
                return TablePath.of(
                        null,
                        normalizePathPart(parts.get(0)),
                        normalizePathPart(parts.get(1)));
            case 3:
                String database = normalizePathPart(parts.get(0));
                if (!logicalDatabaseName.equalsIgnoreCase(database)) {
                    throw new IllegalArgumentException(
                            "YashanDB 表路径中的逻辑 database 与 JDBC URL 不一致，"
                                    + "urlDatabase=" + logicalDatabaseName
                                    + "，pathDatabase=" + database);
                }
                return TablePath.of(
                        logicalDatabaseName,
                        normalizePathPart(parts.get(1)),
                        normalizePathPart(parts.get(2)));
            default:
                throw new IllegalArgumentException("非法 YashanDB 表路径：" + tablePath);
        }
    }

    @Override
    public String quoteIdentifier(String identifier) {
        if (!JdbcDialect.hasText(identifier)) {
            throw new IllegalArgumentException("identifier must not be empty");
        }
        return "\"" + identifier.trim().replace("\"", "\"\"") + "\"";
    }

    /** YashanDB objects are addressed as schema.table. */
    @Override
    public String tableIdentifier(TablePath tablePath) {
        if (tablePath == null) {
            throw new IllegalArgumentException("tablePath must not be null");
        }
        return quoteIdentifier(resolveSchema(tablePath))
                + "."
                + quoteIdentifier(tablePath.getTableName());
    }

    @Override
    public Optional<String> buildUpsertSql(
            TablePath tablePath,
            List<String> fieldNames,
            List<String> primaryKeys) {

        JdbcDialect.validateFields(fieldNames);
        Set<String> primaryKeySet = JdbcDialect.normalizeFields(primaryKeys);
        if (primaryKeySet.isEmpty()) {
            throw new IllegalArgumentException("YashanDB UPSERT 必须配置主键字段");
        }

        Set<String> fields = new HashSet<String>(fieldNames);
        for (String primaryKey : primaryKeys) {
            if (!fields.contains(primaryKey)) {
                throw new IllegalArgumentException(
                        "YashanDB UPSERT 主键字段不存在：" + primaryKey);
            }
        }

        String sourceProjection = fieldNames.stream()
                .map(field -> "? AS " + quoteIdentifier(field))
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
                .append(" TARGET USING (SELECT ")
                .append(sourceProjection)
                .append(" FROM DUAL) SOURCE ON (")
                .append(onClause)
                .append(")");

        if (!updateFields.isEmpty()) {
            String updateClause = updateFields.stream()
                    .map(field -> "TARGET." + quoteIdentifier(field)
                            + " = SOURCE." + quoteIdentifier(field))
                    .collect(Collectors.joining(", "));
            sql.append(" WHEN MATCHED THEN UPDATE SET ")
                    .append(updateClause);
        }

        String insertFields = fieldNames.stream()
                .map(this::quoteIdentifier)
                .collect(Collectors.joining(", "));
        String insertValues = fieldNames.stream()
                .map(field -> "SOURCE." + quoteIdentifier(field))
                .collect(Collectors.joining(", "));

        sql.append(" WHEN NOT MATCHED THEN INSERT (")
                .append(insertFields)
                .append(") VALUES (")
                .append(insertValues)
                .append(")");

        return Optional.of(sql.toString());
    }

    @Override
    public PreparedStatement prepareReadStatement(
            Connection connection,
            String sql,
            int fetchSize) throws SQLException {

        PreparedStatement statement = connection.prepareStatement(
                sql,
                ResultSet.TYPE_FORWARD_ONLY,
                ResultSet.CONCUR_READ_ONLY);
        statement.setFetchSize(fetchSize > 0 ? fetchSize : DEFAULT_FETCH_SIZE);
        return statement;
    }

    @Override
    public Optional<String> buildHashPartitionPredicate(
            Column column,
            int bucket,
            int bucketCount) {

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
                "MOD(ORA_HASH(" + quoteIdentifier(column.getName())
                        + "), " + bucketCount + ") = " + bucket);
    }

    private String resolveSchema(TablePath tablePath) {
        String pathDatabase = tablePath.getDatabaseName();
        String pathSchema = tablePath.getSchemaName();

        if (JdbcDialect.hasText(pathSchema)
                && (!JdbcDialect.hasText(pathDatabase)
                || logicalDatabaseName.equalsIgnoreCase(pathDatabase))) {
            return pathSchema.trim();
        }
        if (JdbcDialect.hasText(connectionConfig.getSchema())) {
            return connectionConfig.getSchema().trim();
        }
        if (JdbcDialect.hasText(connectionConfig.getUsername())) {
            return connectionConfig.getUsername().trim().toUpperCase(Locale.ROOT);
        }
        if (JdbcDialect.hasText(pathSchema)) {
            return pathSchema.trim();
        }
        throw new IllegalArgumentException(
                "YashanDB 表路径缺少 schema，且 connection schema/username 均未配置："
                        + tablePath);
    }

    private static List<String> splitIdentifierPath(String path) {
        List<String> parts = new ArrayList<String>();
        StringBuilder current = new StringBuilder();
        boolean quoted = false;
        for (int i = 0; i < path.length(); i++) {
            char c = path.charAt(i);
            if (c == '\"') {
                current.append(c);
                if (quoted && i + 1 < path.length() && path.charAt(i + 1) == '\"') {
                    current.append('\"');
                    i++;
                } else {
                    quoted = !quoted;
                }
                continue;
            }
            if (c == '.' && !quoted) {
                if (current.length() == 0) {
                    throw new IllegalArgumentException("非法 YashanDB 表路径：" + path);
                }
                parts.add(current.toString());
                current.setLength(0);
            } else {
                current.append(c);
            }
        }
        if (quoted || current.length() == 0) {
            throw new IllegalArgumentException("非法 YashanDB 表路径：" + path);
        }
        parts.add(current.toString());
        return parts;
    }

    private static String normalizePathPart(String part) {
        String value = part.trim();
        if (value.length() >= 2
                && value.charAt(0) == '\"'
                && value.charAt(value.length() - 1) == '\"') {
            return value.substring(1, value.length() - 1).replace("\"\"", "\"");
        }
        return value.toUpperCase(Locale.ROOT);
    }
}
