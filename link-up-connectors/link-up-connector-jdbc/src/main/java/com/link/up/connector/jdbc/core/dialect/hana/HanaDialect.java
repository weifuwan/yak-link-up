package com.link.up.connector.jdbc.core.dialect.hana;

import com.link.up.api.table.catalog.Catalog;
import com.link.up.api.table.catalog.TablePath;
import com.link.up.connector.jdbc.catalog.JdbcCatalogConfig;
import com.link.up.connector.jdbc.catalog.hana.HanaCatalog;
import com.link.up.connector.jdbc.config.JdbcConnectionConfig;
import com.link.up.connector.jdbc.core.converter.JdbcRowConverter;
import com.link.up.connector.jdbc.core.dialect.DatabaseIdentifier;
import com.link.up.connector.jdbc.core.dialect.JdbcDialect;
import com.link.up.connector.jdbc.core.dialect.JdbcTypeMapper;

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

/** SAP HANA bounded JDBC dialect for offline Source and Sink jobs. */
public final class HanaDialect implements JdbcDialect {

    private final JdbcConnectionConfig connectionConfig;
    private final HanaTypeMapper typeMapper;
    private final String databaseName;
    private final String defaultSchema;

    public HanaDialect(JdbcConnectionConfig connectionConfig) {
        if (connectionConfig == null) {
            throw new IllegalArgumentException("connectionConfig must not be null");
        }
        if (!HanaJdbcUrl.accepts(connectionConfig.getUrl())) {
            throw new IllegalArgumentException(
                    "非法 SAP HANA JDBC URL：" + connectionConfig.getUrl());
        }
        this.connectionConfig = connectionConfig;
        this.typeMapper = new HanaTypeMapper();
        this.databaseName = HanaJdbcUrl.databaseName(
                connectionConfig.getUrl(),
                connectionConfig.getProperties());
        this.defaultSchema = HanaJdbcUrl.currentSchema(
                connectionConfig.getUrl(),
                connectionConfig.getProperties(),
                connectionConfig.getSchema());
    }

    @Override
    public String name() {
        return DatabaseIdentifier.HANA;
    }

    @Override
    public Catalog createCatalog(
            String catalogName,
            JdbcConnectionConfig connectionConfig) {
        String resolvedDatabase = HanaJdbcUrl.databaseName(
                connectionConfig.getUrl(),
                connectionConfig.getProperties());
        String resolvedSchema = HanaJdbcUrl.currentSchema(
                connectionConfig.getUrl(),
                connectionConfig.getProperties(),
                connectionConfig.getSchema());
        return new HanaCatalog(
                catalogName,
                new JdbcCatalogConfig(
                        connectionConfig.getUrl(),
                        connectionConfig.getUsername(),
                        connectionConfig.getPassword(),
                        connectionConfig.getDriverName(),
                        connectionConfig.getProperties(),
                        false),
                resolvedSchema,
                resolvedDatabase);
    }

    @Override
    public JdbcTypeMapper typeMapper() {
        return typeMapper;
    }

    @Override
    public JdbcRowConverter rowConverter() {
        return new HanaJdbcRowConverter();
    }

    /** HANA SQL uses schema.table; the database/tenant is selected by the JDBC connection. */
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
                if (!JdbcDialect.hasText(databaseName)) {
                    throw new IllegalArgumentException(
                            "三段式 SAP HANA 表路径需要 JDBC URL/properties 显式配置 databaseName："
                                    + tablePath);
                }
                if (!databaseName.equalsIgnoreCase(database)) {
                    throw new IllegalArgumentException(
                            "SAP HANA 表路径中的 database 与 JDBC 连接不一致，urlDatabase="
                                    + databaseName + "，pathDatabase=" + database);
                }
                return TablePath.of(
                        databaseName,
                        normalizePathPart(parts.get(1)),
                        normalizePathPart(parts.get(2)));
            default:
                throw new IllegalArgumentException(
                        "非法 SAP HANA 表路径：" + tablePath);
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
        if (JdbcDialect.hasText(tablePath.getDatabaseName())
                && JdbcDialect.hasText(databaseName)
                && !databaseName.equalsIgnoreCase(tablePath.getDatabaseName())) {
            throw new IllegalArgumentException(
                    "SAP HANA 表路径中的 database 与 JDBC 连接不一致，urlDatabase="
                            + databaseName + "，pathDatabase=" + tablePath.getDatabaseName());
        }

        String schema = JdbcDialect.hasText(tablePath.getSchemaName())
                ? tablePath.getSchemaName().trim()
                : defaultSchema;
        if (JdbcDialect.hasText(schema)) {
            return quoteIdentifier(schema)
                    + "."
                    + quoteIdentifier(tablePath.getTableName());
        }
        return quoteIdentifier(tablePath.getTableName());
    }

    /** HANA offline UPSERT is expressed with MERGE and one positional marker per field. */
    @Override
    public Optional<String> buildUpsertSql(
            TablePath tablePath,
            List<String> fieldNames,
            List<String> primaryKeys) {

        JdbcDialect.validateFields(fieldNames);
        Set<String> primaryKeySet = JdbcDialect.normalizeFields(primaryKeys);
        if (primaryKeySet.isEmpty()) {
            throw new IllegalArgumentException("SAP HANA UPSERT 必须配置主键字段");
        }

        Set<String> fields = new HashSet<String>(fieldNames);
        for (String primaryKey : primaryKeys) {
            if (!fields.contains(primaryKey)) {
                throw new IllegalArgumentException(
                        "SAP HANA UPSERT 主键字段不存在：" + primaryKey);
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
                .append(" AS TARGET USING (SELECT ")
                .append(sourceProjection)
                .append(" FROM DUMMY) AS SOURCE ON ")
                .append(onClause);

        if (!updateFields.isEmpty()) {
            String updates = updateFields.stream()
                    .map(field -> "TARGET." + quoteIdentifier(field)
                            + " = SOURCE." + quoteIdentifier(field))
                    .collect(Collectors.joining(", "));
            sql.append(" WHEN MATCHED THEN UPDATE SET ")
                    .append(updates);
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

    /** Keep unqualified queries aligned with the connector-level schema option. */
    @Override
    public Map<String, String> defaultConnectionProperties() {
        if (!JdbcDialect.hasText(connectionConfig.getSchema())
                || HanaJdbcUrl.hasCurrentSchemaProperty(
                connectionConfig.getUrl(),
                connectionConfig.getProperties())) {
            return Collections.emptyMap();
        }
        Map<String, String> result = new LinkedHashMap<String, String>();
        result.put("currentSchema", connectionConfig.getSchema().trim());
        return Collections.unmodifiableMap(result);
    }

    private static List<String> splitIdentifierPath(String path) {
        List<String> parts = new ArrayList<String>();
        StringBuilder current = new StringBuilder();
        boolean quoted = false;
        for (int i = 0; i < path.length(); i++) {
            char c = path.charAt(i);
            if (c == '"') {
                current.append(c);
                if (quoted && i + 1 < path.length() && path.charAt(i + 1) == '"') {
                    current.append('"');
                    i++;
                } else {
                    quoted = !quoted;
                }
                continue;
            }
            if (c == '.' && !quoted) {
                if (current.length() == 0) {
                    throw new IllegalArgumentException(
                            "非法 SAP HANA 表路径：" + path);
                }
                parts.add(current.toString());
                current.setLength(0);
            } else {
                current.append(c);
            }
        }
        if (quoted || current.length() == 0) {
            throw new IllegalArgumentException(
                    "非法 SAP HANA 表路径：" + path);
        }
        parts.add(current.toString());
        return parts;
    }

    private static String normalizePathPart(String part) {
        String value = part.trim();
        if (value.length() >= 2
                && value.charAt(0) == '"'
                && value.charAt(value.length() - 1) == '"') {
            return value.substring(1, value.length() - 1)
                    .replace("\"\"", "\"");
        }
        return value.toUpperCase(Locale.ROOT);
    }
}
