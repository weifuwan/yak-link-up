package com.link.up.connector.jdbc.core.dialect.xugu;

import com.link.up.api.table.catalog.Catalog;
import com.link.up.api.table.catalog.TablePath;
import com.link.up.connector.jdbc.catalog.JdbcCatalogConfig;
import com.link.up.connector.jdbc.catalog.xugu.XuguCatalog;
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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/** XuguDB offline JDBC dialect. */
public final class XuguDialect implements JdbcDialect {

    private static final int DEFAULT_FETCH_SIZE = 500;

    private final JdbcConnectionConfig connectionConfig;
    private final XuguTypeMapper typeMapper = new XuguTypeMapper();
    private final String databaseName;
    private final String defaultSchema;
    private final String compatibleMode;

    public XuguDialect(JdbcConnectionConfig connectionConfig) {
        if (connectionConfig == null) {
            throw new IllegalArgumentException("connectionConfig must not be null");
        }
        this.connectionConfig = connectionConfig;
        this.databaseName = XuguJdbcUrl.databaseName(connectionConfig.getUrl());
        if (!JdbcDialect.hasText(databaseName)) {
            throw new IllegalArgumentException(
                    "XuguDB JDBC URL 必须包含 database，例如 jdbc:xugu://127.0.0.1:5138/SYSTEM");
        }
        this.compatibleMode = resolveCompatibleMode(connectionConfig);
        this.defaultSchema = resolveDefaultSchema(connectionConfig, compatibleMode);
    }

    @Override
    public String name() {
        return DatabaseIdentifier.XUGU;
    }

    @Override
    public Catalog createCatalog(
            String catalogName,
            JdbcConnectionConfig connectionConfig) {

        return new XuguCatalog(
                catalogName,
                new JdbcCatalogConfig(
                        connectionConfig.getUrl(),
                        connectionConfig.getUsername(),
                        connectionConfig.getPassword(),
                        connectionConfig.getDriverName(),
                        resolvedCatalogProperties(connectionConfig),
                        false),
                connectionConfig.getSchema());
    }

    @Override
    public JdbcTypeMapper typeMapper() {
        return typeMapper;
    }

    @Override
    public JdbcRowConverter rowConverter() {
        return new XuguJdbcRowConverter();
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
                            "XuguDB 表路径中的 database 与 JDBC URL 不一致，urlDatabase="
                                    + databaseName + "，pathDatabase=" + database);
                }
                return TablePath.of(
                        databaseName,
                        normalizeIdentifier(parts.get(1)),
                        normalizeIdentifier(parts.get(2)));
            default:
                throw new IllegalArgumentException("非法 XuguDB 表路径：" + tablePath);
        }
    }

    @Override
    public String quoteIdentifier(String identifier) {
        if (!JdbcDialect.hasText(identifier)) {
            throw new IllegalArgumentException("identifier must not be empty");
        }
        return "\"" + identifier.trim().replace("\"", "\"\"") + "\"";
    }

    /** Xugu SQL addresses schema.table; database is selected by the JDBC URL. */
    @Override
    public String tableIdentifier(TablePath tablePath) {
        if (tablePath == null) {
            throw new IllegalArgumentException("tablePath must not be null");
        }
        String pathDatabase = tablePath.getDatabaseName();
        if (JdbcDialect.hasText(pathDatabase)
                && !databaseName.equalsIgnoreCase(pathDatabase)) {
            throw new IllegalArgumentException(
                    "XuguDB JDBC 连接只支持 URL database：" + databaseName
                            + "，requested=" + pathDatabase);
        }
        String schema = JdbcDialect.hasText(tablePath.getSchemaName())
                ? tablePath.getSchemaName().trim()
                : defaultSchema;
        if (!JdbcDialect.hasText(schema)) {
            throw new IllegalArgumentException(
                    "无法解析 XuguDB 默认 schema；请配置 schema、username 或 URL user/current_schema");
        }
        return quoteIdentifier(schema)
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
            throw new IllegalArgumentException("XuguDB UPSERT 必须配置主键字段");
        }
        Set<String> fields = new HashSet<String>(fieldNames);
        for (String primaryKey : primaryKeys) {
            if (!fields.contains(primaryKey)) {
                throw new IllegalArgumentException(
                        "XuguDB UPSERT 主键字段不存在：" + primaryKey);
            }
        }

        String valuesBinding = fieldNames.stream()
                .map(field -> "? " + quoteIdentifier(field))
                .collect(Collectors.joining(", "));
        String usingClause = "SELECT " + valuesBinding + " FROM DUAL";
        String onConditions = primaryKeys.stream()
                .map(field -> "TARGET." + quoteIdentifier(field)
                        + "=SOURCE." + quoteIdentifier(field))
                .collect(Collectors.joining(" AND "));
        String updateClause = fieldNames.stream()
                .filter(field -> !primaryKeySet.contains(field))
                .map(field -> "TARGET." + quoteIdentifier(field)
                        + "=SOURCE." + quoteIdentifier(field))
                .collect(Collectors.joining(", "));
        String insertFields = fieldNames.stream()
                .map(this::quoteIdentifier)
                .collect(Collectors.joining(", "));
        String insertValues = fieldNames.stream()
                .map(field -> "SOURCE." + quoteIdentifier(field))
                .collect(Collectors.joining(", "));

        StringBuilder sql = new StringBuilder()
                .append("MERGE INTO ")
                .append(tableIdentifier(tablePath))
                .append(" TARGET USING (")
                .append(usingClause)
                .append(") SOURCE ON (")
                .append(onConditions)
                .append(")");
        if (!updateClause.isEmpty()) {
            sql.append(" WHEN MATCHED THEN UPDATE SET ")
                    .append(updateClause);
        }
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
    public Map<String, String> defaultConnectionProperties() {
        Map<String, String> result = new LinkedHashMap<String, String>();
        if (JdbcDialect.hasText(connectionConfig.getCompatibleMode())
                && !JdbcDialect.hasText(
                XuguJdbcUrl.compatibleMode(
                        connectionConfig.getUrl(),
                        connectionConfig.getProperties()))) {
            result.put("compatiblemode", connectionConfig.getCompatibleMode().trim());
        }

        // Source-generated SQL is schema-qualified, but custom SQL is user-owned
        // and may legitimately use unqualified table names. Keep the actual Xugu
        // session schema aligned with the connector-level schema in that case.
        // Connector schema is an already-resolved physical identifier, so quote
        // it before passing it through current_schema to avoid compatibility-mode
        // case folding changing the intended object name.
        if (JdbcDialect.hasText(connectionConfig.getSchema())
                && !JdbcDialect.hasText(
                XuguJdbcUrl.currentSchema(
                        connectionConfig.getUrl(),
                        connectionConfig.getProperties()))) {
            result.put(
                    "current_schema",
                    quoteSessionIdentifier(connectionConfig.getSchema()));
        }
        return result;
    }

    // String HASH partition intentionally remains unsupported. The current
    // Xugu reference dialect exposes no stable hash predicate for JDBC splits;
    // numeric MIN/MAX partitioning remains available through the shared planner.

    private String normalizeIdentifier(String part) {
        String value = part.trim();
        if (value.length() >= 2
                && value.charAt(0) == '\"'
                && value.charAt(value.length() - 1) == '\"') {
            return value.substring(1, value.length() - 1).replace("\"\"", "\"");
        }
        if ("POSTGRESQL".equals(compatibleMode)) {
            return value.toLowerCase(Locale.ROOT);
        }
        if ("MYSQL".equals(compatibleMode)) {
            return value;
        }
        return value.toUpperCase(Locale.ROOT);
    }

    private static String resolveDefaultSchema(
            JdbcConnectionConfig config,
            String compatibleMode) {
        String currentSchema = XuguJdbcUrl.currentSchema(
                config.getUrl(), config.getProperties());
        if (JdbcDialect.hasText(currentSchema)) {
            return XuguJdbcUrl.normalizeSessionIdentifier(
                    currentSchema, compatibleMode);
        }
        if (JdbcDialect.hasText(config.getSchema())) {
            return config.getSchema().trim();
        }
        String user = config.getUsername();
        if (!JdbcDialect.hasText(user)) {
            user = XuguJdbcUrl.user(config.getUrl(), config.getProperties());
        }
        return XuguJdbcUrl.normalizeSessionIdentifier(user, compatibleMode);
    }

    private static String resolveCompatibleMode(JdbcConnectionConfig config) {
        String jdbcMode = XuguJdbcUrl.compatibleMode(
                config.getUrl(), config.getProperties());
        String mode = JdbcDialect.hasText(jdbcMode)
                ? jdbcMode
                : config.getCompatibleMode();
        return JdbcDialect.hasText(mode)
                ? mode.trim().toUpperCase(Locale.ROOT)
                : "NONE";
    }

    private static Map<String, String> resolvedCatalogProperties(
            JdbcConnectionConfig config) {
        Map<String, String> result = new LinkedHashMap<String, String>(config.getProperties());
        if (!JdbcDialect.hasText(
                XuguJdbcUrl.compatibleMode(config.getUrl(), result))
                && JdbcDialect.hasText(config.getCompatibleMode())) {
            result.put("compatiblemode", config.getCompatibleMode().trim());
        }
        return result;
    }

    private static String quoteSessionIdentifier(String identifier) {
        String value = identifier.trim();
        return "\"" + value.replace("\"", "\"\"") + "\"";
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
                    throw new IllegalArgumentException("非法 XuguDB 表路径：" + path);
                }
                parts.add(current.toString());
                current.setLength(0);
            } else {
                current.append(c);
            }
        }
        if (quoted || current.length() == 0) {
            throw new IllegalArgumentException("非法 XuguDB 表路径：" + path);
        }
        parts.add(current.toString());
        return parts;
    }
}
