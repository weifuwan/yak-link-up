package com.link.up.connector.jdbc.core.dialect.dameng;

import com.link.up.api.table.catalog.Catalog;
import com.link.up.api.table.catalog.Column;
import com.link.up.api.table.catalog.TablePath;
import com.link.up.connector.jdbc.catalog.JdbcCatalogConfig;
import com.link.up.connector.jdbc.catalog.dameng.DamengCatalog;
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
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Dameng DM8 offline JDBC dialect.
 *
 * <p>Only bounded Source, batch Sink, Catalog and offline DDL are implemented.
 * Redo/archive-log CDC, checkpoints and runtime schema events are intentionally
 * outside this dialect.</p>
 */
public final class DamengDialect implements JdbcDialect {

    private final JdbcConnectionConfig connectionConfig;
    private final DamengTypeMapper typeMapper;

    public DamengDialect(JdbcConnectionConfig connectionConfig) {
        if (connectionConfig == null) {
            throw new IllegalArgumentException("connectionConfig must not be null");
        }
        if (!DamengJdbcUrl.accepts(connectionConfig.getUrl())) {
            throw new IllegalArgumentException(
                    "非法 Dameng JDBC URL：" + connectionConfig.getUrl());
        }
        this.connectionConfig = connectionConfig;
        this.typeMapper = new DamengTypeMapper();
    }

    @Override
    public String name() {
        return DatabaseIdentifier.DAMENG;
    }

    @Override
    public Catalog createCatalog(String catalogName, JdbcConnectionConfig connectionConfig) {
        return new DamengCatalog(
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
        return new DamengJdbcRowConverter();
    }

    /** Dameng SQL addresses schema.table; a three-part path keeps a logical DB for Catalog APIs. */
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
                return TablePath.of(
                        normalizePathPart(parts.get(0)),
                        normalizePathPart(parts.get(1)),
                        normalizePathPart(parts.get(2)));
            default:
                throw new IllegalArgumentException("非法 Dameng 表路径：" + tablePath);
        }
    }

    @Override
    public String quoteIdentifier(String identifier) {
        if (!JdbcDialect.hasText(identifier)) {
            throw new IllegalArgumentException("identifier must not be empty");
        }
        return "\"" + identifier.trim().replace("\"", "\"\"") + "\"";
    }

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
            throw new IllegalArgumentException("Dameng UPSERT 必须配置主键字段");
        }

        Set<String> fieldSet = new HashSet<String>(fieldNames);
        for (String primaryKey : primaryKeys) {
            if (!fieldSet.contains(primaryKey)) {
                throw new IllegalArgumentException(
                        "Dameng UPSERT 主键字段不存在：" + primaryKey);
            }
        }

        String sourceProjection = fieldNames.stream()
                .map(field -> "? " + quoteIdentifier(field))
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
                .append(") SOURCE ON (")
                .append(onClause)
                .append(")");

        if (!updateFields.isEmpty()) {
            String updateClause = updateFields.stream()
                    .map(field -> "TARGET." + quoteIdentifier(field)
                            + " = SOURCE." + quoteIdentifier(field))
                    .collect(Collectors.joining(", "));
            sql.append(" WHEN MATCHED THEN UPDATE SET ").append(updateClause);
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
        if (fetchSize > 0) {
            statement.setFetchSize(fetchSize);
        }
        return statement;
    }

    /** ORA_HASH returns a deterministic bucket from 0 through max_bucket inclusive. */
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
            throw new IllegalArgumentException("bucket must be between 0 and bucketCount - 1");
        }
        return Optional.of(
                "ORA_HASH(" + quoteIdentifier(column.getName()) + ", "
                        + (bucketCount - 1) + ") = " + bucket);
    }

    /** Explicit connector schema becomes the DM JDBC session schema unless user properties override it. */
    @Override
    public Map<String, String> defaultConnectionProperties() {
        if (!JdbcDialect.hasText(connectionConfig.getSchema())) {
            return Collections.emptyMap();
        }
        Map<String, String> result = new LinkedHashMap<String, String>();
        result.put("schema", connectionConfig.getSchema().trim());
        return Collections.unmodifiableMap(result);
    }

    private String resolveSchema(TablePath tablePath) {
        if (JdbcDialect.hasText(tablePath.getSchemaName())) {
            return tablePath.getSchemaName().trim();
        }
        if (JdbcDialect.hasText(connectionConfig.getSchema())) {
            return connectionConfig.getSchema().trim();
        }
        String schema = DamengJdbcUrl.schema(
                connectionConfig.getUrl(),
                connectionConfig.getProperties());
        if (JdbcDialect.hasText(schema)) {
            return schema.trim();
        }
        if (JdbcDialect.hasText(connectionConfig.getUsername())) {
            return connectionConfig.getUsername().trim().toUpperCase(Locale.ROOT);
        }
        throw new IllegalArgumentException(
                "Dameng 表路径缺少 schema，且 connection schema/JDBC URL schema/username 均未配置："
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
                    throw new IllegalArgumentException("非法 Dameng 表路径：" + path);
                }
                parts.add(current.toString());
                current.setLength(0);
            } else {
                current.append(c);
            }
        }
        if (quoted || current.length() == 0) {
            throw new IllegalArgumentException("非法 Dameng 表路径：" + path);
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
