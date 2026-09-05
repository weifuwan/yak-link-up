package com.link.up.connector.jdbc.core.dialect.duckdb;

import com.link.up.api.table.catalog.Catalog;
import com.link.up.api.table.catalog.Column;
import com.link.up.api.table.catalog.TablePath;
import com.link.up.connector.jdbc.catalog.JdbcCatalogConfig;
import com.link.up.connector.jdbc.catalog.duckdb.DuckDbCatalog;
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
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/** DuckDB offline JDBC dialect. */
public final class DuckDbDialect implements JdbcDialect {

    private static final int DEFAULT_FETCH_SIZE = 2048;

    private final JdbcConnectionConfig connectionConfig;
    private final DuckDbTypeMapper typeMapper;
    private final String databaseName;
    private final String defaultSchema;

    public DuckDbDialect(JdbcConnectionConfig connectionConfig) {
        if (connectionConfig == null) {
            throw new IllegalArgumentException("connectionConfig must not be null");
        }
        if (DuckDbJdbcUrl.isConnectionPrivateMemory(connectionConfig.getUrl())) {
            throw new IllegalArgumentException(
                    "DuckDB 匿名内存 URL 是 connection-private，无法跨 Catalog/Reader/Writer 连接共享数据；"
                            + "请使用带 jdbc_pin_db=true 的命名内存库，或文件库路径");
        }
        if (DuckDbJdbcUrl.isDuckLake(connectionConfig.getUrl())) {
            throw new IllegalArgumentException(
                    "DuckLake catalog 暂不属于 DuckDB Offline JDBC 首阶段范围，请使用本地/命名内存 DuckDB");
        }
        if (DuckDbJdbcUrl.isInstanceCacheDisabled(
                connectionConfig.getUrl(), connectionConfig.getProperties())) {
            throw new IllegalArgumentException(
                    "DuckDB JDBC connector 依赖 jdbc_instance_cache=true 让 Catalog/Reader/Writer 共享同一实例");
        }
        if (DuckDbJdbcUrl.isUnpinnedNamedMemory(
                connectionConfig.getUrl(), connectionConfig.getProperties())) {
            throw new IllegalArgumentException(
                    "DuckDB 命名内存库必须配置 jdbc_pin_db=true；否则 Catalog 阶段关闭最后一个连接后数据库会被释放");
        }

        this.connectionConfig = connectionConfig;
        this.typeMapper = new DuckDbTypeMapper();
        this.databaseName = DuckDbJdbcUrl.databaseName(connectionConfig.getUrl());
        if (!JdbcDialect.hasText(databaseName)) {
            throw new IllegalArgumentException("无法从 DuckDB JDBC URL 解析 database/catalog 名称");
        }

        String configured = connectionConfig.getSchema();
        if (!JdbcDialect.hasText(configured)) {
            configured = DuckDbJdbcUrl.configuredSchema(
                    connectionConfig.getUrl(), connectionConfig.getProperties());
        }
        this.defaultSchema = JdbcDialect.hasText(configured)
                ? configured.trim()
                : "main";
    }

    @Override
    public String name() {
        return DatabaseIdentifier.DUCKDB;
    }

    @Override
    public Catalog createCatalog(
            String catalogName,
            JdbcConnectionConfig connectionConfig) {

        return new DuckDbCatalog(
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
        return new DuckDbJdbcRowConverter();
    }

    @Override
    public Set<ReadConsistency> supportedReadConsistencies() {
        return EnumSet.of(
                ReadConsistency.BEST_EFFORT,
                ReadConsistency.SINGLE_CONNECTION_SNAPSHOT);
    }

    @Override
    public void configureSnapshotConnection(
            Connection connection,
            ReadConsistency consistency) throws SQLException {

        if (connection == null) {
            throw new IllegalArgumentException("connection must not be null");
        }
        if (consistency != ReadConsistency.SINGLE_CONNECTION_SNAPSHOT) {
            throw new UnsupportedOperationException(
                    "Read consistency is not supported by dialect " + name());
        }
        // DuckDB transactions provide snapshot isolation. Avoid JDBC isolation
        // setters that are not uniformly implemented by the embedded driver.
        connection.setAutoCommit(false);
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
                if (!databaseName.equalsIgnoreCase(database)) {
                    throw new IllegalArgumentException(
                            "DuckDB 表路径中的 database 与 JDBC URL 不一致，urlDatabase="
                                    + databaseName + "，pathDatabase=" + database);
                }
                return TablePath.of(
                        databaseName,
                        normalizePathPart(parts.get(1)),
                        normalizePathPart(parts.get(2)));
            default:
                throw new IllegalArgumentException("非法 DuckDB 表路径：" + tablePath);
        }
    }

    @Override
    public String quoteIdentifier(String identifier) {
        if (!JdbcDialect.hasText(identifier)) {
            throw new IllegalArgumentException("identifier must not be empty");
        }
        return "\"" + identifier.trim().replace("\"", "\"\"") + "\"";
    }

    /** Connection URL selects the active catalog; ordinary SQL uses schema.table. */
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
            throw new IllegalArgumentException("DuckDB UPSERT 必须配置主键字段");
        }

        Set<String> fields = new HashSet<String>(fieldNames);
        for (String primaryKey : primaryKeys) {
            if (!fields.contains(primaryKey)) {
                throw new IllegalArgumentException(
                        "DuckDB UPSERT 主键字段不存在：" + primaryKey);
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
                        + " ON CONFLICT (" + conflictFields + ") "
                        + action);
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
        if (!JdbcDialect.hasText(connectionConfig.getSchema())) {
            return Collections.emptyMap();
        }
        Map<String, String> result = new LinkedHashMap<String, String>();
        result.put("schema", connectionConfig.getSchema().trim());
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
                "MOD(HASH(CAST(" + quoteIdentifier(column.getName())
                        + " AS VARCHAR)), " + bucketCount + ") = " + bucket);
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
                    throw new IllegalArgumentException("非法 DuckDB 表路径：" + path);
                }
                parts.add(current.toString());
                current.setLength(0);
            } else {
                current.append(c);
            }
        }
        if (quoted || current.length() == 0) {
            throw new IllegalArgumentException("非法 DuckDB 表路径：" + path);
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
        return value;
    }
}
