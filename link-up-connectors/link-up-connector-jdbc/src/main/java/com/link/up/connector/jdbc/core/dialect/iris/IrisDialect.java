package com.link.up.connector.jdbc.core.dialect.iris;

import com.link.up.api.table.catalog.Catalog;
import com.link.up.api.table.catalog.TablePath;
import com.link.up.connector.jdbc.catalog.JdbcCatalogConfig;
import com.link.up.connector.jdbc.catalog.iris.IrisCatalog;
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
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/** InterSystems IRIS offline JDBC dialect. */
public final class IrisDialect implements JdbcDialect {

    private static final int DEFAULT_FETCH_SIZE = 500;
    private static final String DEFAULT_SCHEMA = "SQLUser";

    private final JdbcConnectionConfig connectionConfig;
    private final IrisTypeMapper typeMapper = new IrisTypeMapper();
    private final String namespaceName;
    private final String defaultSchema;

    public IrisDialect(JdbcConnectionConfig connectionConfig) {
        if (connectionConfig == null) {
            throw new IllegalArgumentException("connectionConfig must not be null");
        }
        this.connectionConfig = connectionConfig;
        this.namespaceName = IrisJdbcUrl.namespaceName(connectionConfig.getUrl());
        if (!JdbcDialect.hasText(namespaceName)) {
            throw new IllegalArgumentException(
                    "IRIS JDBC URL 必须包含 namespace，例如 jdbc:IRIS://127.0.0.1:1972/USER");
        }
        this.defaultSchema = JdbcDialect.hasText(connectionConfig.getSchema())
                ? connectionConfig.getSchema().trim()
                : DEFAULT_SCHEMA;
    }

    @Override
    public String name() {
        return DatabaseIdentifier.IRIS;
    }

    @Override
    public Catalog createCatalog(
            String catalogName,
            JdbcConnectionConfig connectionConfig) {

        return new IrisCatalog(
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
        return new IrisJdbcRowConverter();
    }

    /**
     * IRIS SQL addresses schema.table; namespace is selected by the JDBC URL.
     */
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
                String namespace = normalizeIdentifier(parts.get(0));
                if (!namespaceName.equalsIgnoreCase(namespace)) {
                    throw new IllegalArgumentException(
                            "IRIS 表路径中的 namespace 与 JDBC URL 不一致，urlNamespace="
                                    + namespaceName + "，pathNamespace=" + namespace);
                }
                return TablePath.of(
                        namespaceName,
                        normalizeIdentifier(parts.get(1)),
                        normalizeIdentifier(parts.get(2)));
            default:
                throw new IllegalArgumentException("非法 IRIS 表路径：" + tablePath);
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
        String database = tablePath.getDatabaseName();
        if (JdbcDialect.hasText(database)
                && !namespaceName.equalsIgnoreCase(database)) {
            // Source metadata from another database is safe to ignore only at
            // sink target normalization. Direct Source table paths must fail.
            throw new IllegalArgumentException(
                    "IRIS JDBC 连接只支持 URL namespace：" + namespaceName
                            + "，requested=" + database);
        }
        String schema = JdbcDialect.hasText(tablePath.getSchemaName())
                ? tablePath.getSchemaName().trim()
                : defaultSchema;
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
            throw new IllegalArgumentException("IRIS UPSERT 必须配置主键字段");
        }
        Set<String> fields = new HashSet<String>(fieldNames);
        for (String primaryKey : primaryKeys) {
            if (!fields.contains(primaryKey)) {
                throw new IllegalArgumentException(
                        "IRIS UPSERT 主键字段不存在：" + primaryKey);
            }
        }

        String columns = fieldNames.stream()
                .map(this::quoteIdentifier)
                .collect(Collectors.joining(", "));
        String placeholders = fieldNames.stream()
                .map(field -> "?")
                .collect(Collectors.joining(", "));

        // IRIS resolves INSERT OR UPDATE through the table's IDKey / primary
        // key / unique constraints. Link Up requires configured PK metadata so
        // auto-created targets have a deterministic conflict identity.
        return Optional.of(
                "INSERT OR UPDATE "
                        + tableIdentifier(tablePath)
                        + " (" + columns + ") VALUES ("
                        + placeholders + ")");
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

    // String HASH partition intentionally remains unsupported. IRIS does not
    // expose a stable hash/md5 scalar suitable for bucket predicates, and its
    // default string collation is commonly SQLUPPER, so generic string RANGE
    // partitioning is also left disabled by JdbcDialect's safe default.

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
                    throw new IllegalArgumentException("非法 IRIS 表路径：" + path);
                }
                parts.add(current.toString());
                current.setLength(0);
            } else {
                current.append(c);
            }
        }
        if (quoted || current.length() == 0) {
            throw new IllegalArgumentException("非法 IRIS 表路径：" + path);
        }
        parts.add(current.toString());
        return parts;
    }

    private static String normalizeIdentifier(String part) {
        String value = part.trim();
        if (value.length() >= 2
                && value.charAt(0) == '\"'
                && value.charAt(value.length() - 1) == '\"') {
            return value.substring(1, value.length() - 1).replace("\"\"", "\"");
        }
        return value;
    }
}
