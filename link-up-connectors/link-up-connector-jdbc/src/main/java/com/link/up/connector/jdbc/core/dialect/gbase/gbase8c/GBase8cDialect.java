package com.link.up.connector.jdbc.core.dialect.gbase.gbase8c;

import com.link.up.api.table.catalog.Catalog;
import com.link.up.api.table.catalog.TablePath;
import com.link.up.connector.jdbc.catalog.JdbcCatalogConfig;
import com.link.up.connector.jdbc.catalog.gbase.gbase8c.GBase8cCatalog;
import com.link.up.connector.jdbc.config.JdbcConnectionConfig;
import com.link.up.connector.jdbc.core.converter.JdbcRowConverter;
import com.link.up.connector.jdbc.core.dialect.DatabaseIdentifier;
import com.link.up.connector.jdbc.core.dialect.JdbcDialect;
import com.link.up.connector.jdbc.core.dialect.JdbcTypeMapper;

/**
 * GBase 8c bounded/offline JDBC dialect.
 *
 * <p>The adapter targets the dedicated GBase 8c JDBC protocol and PG-compatible metadata/read
 * semantics. The existing-table Sink stage reuses the portable INSERT/batch path only; UPSERT,
 * structure-changing DDL, CDC and compatibility-mode expansion remain separate stages.</p>
 */
public final class GBase8cDialect implements JdbcDialect {

    private static final String DEFAULT_SCHEMA = "public";

    private final JdbcConnectionConfig connectionConfig;
    private final String databaseName;
    private final GBase8cTypeMapper typeMapper;

    public GBase8cDialect(JdbcConnectionConfig connectionConfig) {
        if (connectionConfig == null) {
            throw new IllegalArgumentException("connectionConfig must not be null");
        }
        if (!GBase8cJdbcUrl.accepts(connectionConfig.getUrl())) {
            throw new IllegalArgumentException(
                    "非法 GBase 8c JDBC URL：" + connectionConfig.getUrl());
        }
        String database = GBase8cJdbcUrl.databaseName(connectionConfig.getUrl());
        if (!JdbcDialect.hasText(database)) {
            throw new IllegalArgumentException("GBase 8c JDBC URL 必须指定数据库");
        }
        this.connectionConfig = connectionConfig;
        this.databaseName = database;
        this.typeMapper = new GBase8cTypeMapper();
    }

    @Override
    public String name() {
        return DatabaseIdentifier.GBASE8C;
    }

    @Override
    public Catalog createCatalog(
            String catalogName,
            JdbcConnectionConfig connectionConfig) {
        return new GBase8cCatalog(
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
        return new GBase8cJdbcRowConverter();
    }

    /** GBase 8c follows PG-compatible schema.table path semantics. */
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
                requireCurrentDatabase(parts[0]);
                return TablePath.of(databaseName, parts[1], parts[2]);
            default:
                throw new IllegalArgumentException(
                        "非法 GBase 8c 表路径：" + tablePath);
        }
    }

    @Override
    public String quoteIdentifier(String identifier) {
        if (!JdbcDialect.hasText(identifier)) {
            throw new IllegalArgumentException("identifier must not be empty");
        }
        return "\"" + identifier.trim().replace("\"", "\"\"") + "\"";
    }

    /** Database is selected by the JDBC URL; SQL identifiers use schema.table. */
    @Override
    public String tableIdentifier(TablePath tablePath) {
        if (tablePath == null) {
            throw new IllegalArgumentException("tablePath must not be null");
        }
        if (JdbcDialect.hasText(tablePath.getDatabaseName())) {
            requireCurrentDatabase(tablePath.getDatabaseName());
        }
        if (!JdbcDialect.hasText(tablePath.getTableName())) {
            throw new IllegalArgumentException("table name must not be empty");
        }
        String schema = tablePath.getSchemaName();
        if (!JdbcDialect.hasText(schema)) {
            schema = connectionConfig.getSchema();
        }
        if (!JdbcDialect.hasText(schema)) {
            schema = DEFAULT_SCHEMA;
        }
        return quoteIdentifier(schema) + "." + quoteIdentifier(tablePath.getTableName());
    }

    private void requireCurrentDatabase(String requestedDatabase) {
        if (!JdbcDialect.hasText(requestedDatabase)
                || !databaseName.equals(requestedDatabase.trim())) {
            throw new IllegalArgumentException(
                    "GBase 8c bounded JDBC adapter does not support cross-database table_path; "
                            + "current database="
                            + databaseName
                            + ", requested database="
                            + requestedDatabase);
        }
    }
}
