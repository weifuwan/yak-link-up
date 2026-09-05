package com.link.up.connector.jdbc.core.dialect.highgo;

import com.link.up.api.table.catalog.Catalog;
import com.link.up.api.table.catalog.Column;
import com.link.up.api.table.catalog.TablePath;
import com.link.up.connector.jdbc.catalog.JdbcCatalogConfig;
import com.link.up.connector.jdbc.catalog.highgo.HighGoCatalog;
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
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/** HighGo offline JDBC dialect. */
public final class HighGoDialect implements JdbcDialect {

    private final JdbcConnectionConfig connectionConfig;
    private final HighGoTypeMapper typeMapper;
    private final String databaseName;
    private final String defaultSchema;

    public HighGoDialect(JdbcConnectionConfig connectionConfig) {
        if (connectionConfig == null) {
            throw new IllegalArgumentException("connectionConfig must not be null");
        }
        if (!HighGoJdbcUrl.accepts(connectionConfig.getUrl())) {
            throw new IllegalArgumentException(
                    "非法 HighGo JDBC URL：" + connectionConfig.getUrl());
        }

        this.connectionConfig = connectionConfig;
        this.typeMapper = new HighGoTypeMapper();
        this.databaseName = HighGoJdbcUrl.databaseName(connectionConfig.getUrl());
        if (!JdbcDialect.hasText(databaseName)) {
            throw new IllegalArgumentException(
                    "HighGo JDBC URL 必须包含数据库名，例如 jdbc:highgo://127.0.0.1:5866/highgo");
        }
        this.defaultSchema = resolveConfiguredSchema(connectionConfig);
    }

    @Override
    public String name() {
        return DatabaseIdentifier.HIGHGO;
    }

    @Override
    public Catalog createCatalog(
            String catalogName,
            JdbcConnectionConfig connectionConfig) {

        return new HighGoCatalog(
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
        return new HighGoJdbcRowConverter();
    }

    @Override
    public Set<ReadConsistency> supportedReadConsistencies() {
        return EnumSet.of(
                ReadConsistency.BEST_EFFORT,
                ReadConsistency.SINGLE_CONNECTION_SNAPSHOT);
    }

    @Override
    public TablePath parseTablePath(String tablePath) {
        if (!JdbcDialect.hasText(tablePath)) {
            throw new IllegalArgumentException("tablePath must not be empty");
        }

        List<String> parts = splitIdentifierPath(tablePath.trim());
        switch (parts.size()) {
            case 1:
                return TablePath.of(normalizeIdentifier(parts.get(0)));
            case 2:
                return TablePath.of(
                        null,
                        normalizeIdentifier(parts.get(0)),
                        normalizeIdentifier(parts.get(1)));
            case 3:
                String database = normalizeIdentifier(parts.get(0));
                if (!databaseName.equalsIgnoreCase(database)) {
                    throw new IllegalArgumentException(
                            "HighGo table_path database 与 JDBC URL 不一致，urlDatabase="
                                    + databaseName + "，pathDatabase=" + database);
                }
                return TablePath.of(
                        databaseName,
                        normalizeIdentifier(parts.get(1)),
                        normalizeIdentifier(parts.get(2)));
            default:
                throw new IllegalArgumentException("非法 HighGo 表路径：" + tablePath);
        }
    }

    @Override
    public String quoteIdentifier(String identifier) {
        if (!JdbcDialect.hasText(identifier)) {
            throw new IllegalArgumentException("identifier must not be empty");
        }
        return "\"" + identifier.trim().replace("\"", "\"\"") + "\"";
    }

    /** HighGo SQL addresses schema.table; database is selected by the JDBC URL. */
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
            throw new IllegalArgumentException("HighGo UPSERT 必须配置主键字段");
        }

        Set<String> fields = new HashSet<String>(fieldNames);
        for (String primaryKey : primaryKeys) {
            if (!fields.contains(primaryKey)) {
                throw new IllegalArgumentException(
                        "HighGo UPSERT 主键字段不存在：" + primaryKey);
            }
        }

        String conflictFields = primaryKeys.stream()
                .map(this::quoteIdentifier)
                .collect(Collectors.joining(", "));
        String updateClause = fieldNames.stream()
                .filter(field -> !primaryKeySet.contains(field))
                .map(field -> quoteIdentifier(field)
                        + " = EXCLUDED." + quoteIdentifier(field))
                .collect(Collectors.joining(", "));
        String action = updateClause.isEmpty()
                ? "DO NOTHING"
                : "DO UPDATE SET " + updateClause;

        return Optional.of(
                buildInsertSql(tablePath, fieldNames)
                        + " ON CONFLICT (" + conflictFields + ") " + action);
    }

    @Override
    public PreparedStatement prepareReadStatement(
            Connection connection,
            String sql,
            int fetchSize) throws SQLException {

        if (connection.getAutoCommit()) {
            connection.setAutoCommit(false);
        }
        PreparedStatement statement = connection.prepareStatement(
                sql,
                ResultSet.TYPE_FORWARD_ONLY,
                ResultSet.CONCUR_READ_ONLY);
        if (fetchSize > 0) {
            statement.setFetchSize(fetchSize);
        }
        return statement;
    }

    @Override
    public Map<String, String> defaultConnectionProperties() {
        Map<String, String> result = new LinkedHashMap<String, String>();
        result.put("reWriteBatchedInserts", "true");

        String urlSchema = HighGoJdbcUrl.currentSchema(
                connectionConfig.getUrl(), null);
        String effectiveSchema = HighGoJdbcUrl.currentSchema(
                connectionConfig.getUrl(), connectionConfig.getProperties());
        if (!JdbcDialect.hasText(urlSchema)
                && !JdbcDialect.hasText(effectiveSchema)
                && JdbcDialect.hasText(connectionConfig.getSchema())) {
            result.put("currentSchema", connectionConfig.getSchema().trim());
        }
        return Collections.unmodifiableMap(result);
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
                "MOD(ABS(HASHTEXT(CAST("
                        + quoteIdentifier(column.getName())
                        + " AS TEXT))::BIGINT), "
                        + bucketCount + ") = " + bucket);
    }

    private String resolveSchema(TablePath tablePath) {
        String pathDatabase = tablePath.getDatabaseName();
        String pathSchema = tablePath.getSchemaName();
        if (JdbcDialect.hasText(pathSchema)
                && (!JdbcDialect.hasText(pathDatabase)
                || databaseName.equalsIgnoreCase(pathDatabase))) {
            return pathSchema.trim();
        }
        return defaultSchema;
    }

    private static String resolveConfiguredSchema(JdbcConnectionConfig config) {
        String urlSchema = HighGoJdbcUrl.currentSchema(config.getUrl(), null);
        if (JdbcDialect.hasText(urlSchema)) {
            return urlSchema.trim();
        }
        String propertySchema = HighGoJdbcUrl.currentSchema(
                config.getUrl(), config.getProperties());
        if (JdbcDialect.hasText(propertySchema)) {
            return propertySchema.trim();
        }
        if (JdbcDialect.hasText(config.getSchema())) {
            return config.getSchema().trim();
        }
        return "public";
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
                    throw new IllegalArgumentException("非法 HighGo 表路径：" + path);
                }
                parts.add(current.toString());
                current.setLength(0);
            } else {
                current.append(c);
            }
        }
        if (quoted || current.length() == 0) {
            throw new IllegalArgumentException("非法 HighGo 表路径：" + path);
        }
        parts.add(current.toString());
        return parts;
    }

    private static String normalizeIdentifier(String part) {
        String value = part.trim();
        if (value.length() >= 2
                && value.charAt(0) == '\"'
                && value.charAt(value.length() - 1) == '\"') {
            return value.substring(1, value.length() - 1)
                    .replace("\"\"", "\"");
        }
        return value.toLowerCase(Locale.ROOT);
    }
}
