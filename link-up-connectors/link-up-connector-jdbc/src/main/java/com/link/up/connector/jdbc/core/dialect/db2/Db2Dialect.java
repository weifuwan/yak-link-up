package com.link.up.connector.jdbc.core.dialect.db2;

import com.link.up.api.table.catalog.Catalog;
import com.link.up.api.table.catalog.TablePath;
import com.link.up.connector.jdbc.catalog.JdbcCatalogConfig;
import com.link.up.connector.jdbc.catalog.db2.Db2Catalog;
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

/**
 * DB2 LUW offline JDBC dialect.
 *
 * <p>Only bounded Source, batch Sink, Catalog and offline DDL are implemented.
 * DB2 CDC/LSN state and runtime schema events stay outside this dialect.</p>
 */
public final class Db2Dialect implements JdbcDialect {

    private final JdbcConnectionConfig connectionConfig;
    private final Db2TypeMapper typeMapper;
    private final String databaseName;

    public Db2Dialect(JdbcConnectionConfig connectionConfig) {
        if (connectionConfig == null) {
            throw new IllegalArgumentException("connectionConfig must not be null");
        }
        this.connectionConfig = connectionConfig;
        this.typeMapper = new Db2TypeMapper();
        this.databaseName = Db2JdbcUrl.databaseName(connectionConfig.getUrl());
        if (!JdbcDialect.hasText(databaseName)) {
            throw new IllegalArgumentException(
                    "DB2 JDBC URL 必须包含数据库名，例如 jdbc:db2://127.0.0.1:50000/SAMPLE");
        }
    }

    @Override
    public String name() {
        return DatabaseIdentifier.DB2;
    }

    @Override
    public Catalog createCatalog(String catalogName, JdbcConnectionConfig connectionConfig) {
        return new Db2Catalog(
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
        return new Db2JdbcRowConverter();
    }

    /** DB2 two-part paths are schema.table. */
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
                if (!databaseName.equalsIgnoreCase(database)) {
                    throw new IllegalArgumentException(
                            "DB2 表路径中的 database 与 JDBC URL 不一致，urlDatabase="
                                    + databaseName + "，pathDatabase=" + database);
                }
                return TablePath.of(
                        databaseName,
                        normalizePathPart(parts.get(1)),
                        normalizePathPart(parts.get(2)));
            default:
                throw new IllegalArgumentException("非法 DB2 表路径：" + tablePath);
        }
    }

    @Override
    public String quoteIdentifier(String identifier) {
        if (!JdbcDialect.hasText(identifier)) {
            throw new IllegalArgumentException("identifier must not be empty");
        }
        return "\"" + identifier.trim().replace("\"", "\"\"") + "\"";
    }

    /** DB2 SQL is schema.table; the database is selected by the JDBC URL. */
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
            throw new IllegalArgumentException("DB2 UPSERT 必须配置主键字段");
        }

        Set<String> fields = new HashSet<String>(fieldNames);
        for (String primaryKey : primaryKeys) {
            if (!fields.contains(primaryKey)) {
                throw new IllegalArgumentException("DB2 UPSERT 主键字段不存在：" + primaryKey);
            }
        }

        String sourceFields = fieldNames.stream()
                .map(this::quoteIdentifier)
                .collect(Collectors.joining(", "));
        String placeholders = fieldNames.stream()
                .map(field -> "?")
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
                .append(sourceFields)
                .append(") ON ")
                .append(onClause);

        if (!updateFields.isEmpty()) {
            String updateClause = updateFields.stream()
                    .map(field -> "TARGET." + quoteIdentifier(field)
                            + " = SOURCE." + quoteIdentifier(field))
                    .collect(Collectors.joining(", "));
            sql.append(" WHEN MATCHED THEN UPDATE SET ")
                    .append(updateClause);
        }

        String insertValues = fieldNames.stream()
                .map(field -> "SOURCE." + quoteIdentifier(field))
                .collect(Collectors.joining(", "));
        sql.append(" WHEN NOT MATCHED THEN INSERT (")
                .append(sourceFields)
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

    private String resolveSchema(TablePath tablePath) {
        String pathDatabase = tablePath.getDatabaseName();
        String pathSchema = tablePath.getSchemaName();

        if (JdbcDialect.hasText(pathSchema)
                && (!JdbcDialect.hasText(pathDatabase)
                || databaseName.equalsIgnoreCase(pathDatabase))) {
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
                "DB2 表路径缺少 schema，且 connection schema/username 均未配置：" + tablePath);
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
                    throw new IllegalArgumentException("非法 DB2 表路径：" + path);
                }
                parts.add(current.toString());
                current.setLength(0);
            } else {
                current.append(c);
            }
        }
        if (quoted || current.length() == 0) {
            throw new IllegalArgumentException("非法 DB2 表路径：" + path);
        }
        parts.add(current.toString());
        return parts;
    }

    private static String normalizePathPart(String part) {
        String value = part.trim();
        if (value.length() >= 2 && value.charAt(0) == '\"'
                && value.charAt(value.length() - 1) == '\"') {
            return value.substring(1, value.length() - 1).replace("\"\"", "\"");
        }
        return value.toUpperCase(Locale.ROOT);
    }
}
