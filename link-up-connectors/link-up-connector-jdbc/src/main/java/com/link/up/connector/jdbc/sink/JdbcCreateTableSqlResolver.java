package com.link.up.connector.jdbc.sink;

import com.link.up.api.table.catalog.CatalogTable;
import com.link.up.api.table.catalog.TablePath;
import com.link.up.connector.jdbc.catalog.mysql.MySqlCreateTableSqlBuilder;
import com.link.up.connector.jdbc.catalog.mysql.MySqlTypeMapper;
import com.link.up.connector.jdbc.catalog.oracle.OracleCreateTableSqlBuilder;
import com.link.up.connector.jdbc.catalog.postgres.PostgresCreateTableSqlBuilder;
import com.link.up.connector.jdbc.catalog.sqlserver.SqlServerCreateTableSqlBuilder;
import com.link.up.connector.jdbc.config.JdbcConnectionConfig;
import com.link.up.connector.jdbc.core.dialect.DatabaseIdentifier;
import com.link.up.connector.jdbc.core.dialect.JdbcDialect;
import com.link.up.connector.jdbc.core.dialect.oceanbase.OceanBaseCompatibleMode;
import com.link.up.connector.jdbc.core.dialect.oceanbase.OceanBaseJdbcUrl;
import com.link.up.connector.jdbc.core.dialect.oracle.OracleJdbcUrl;
import com.link.up.connector.jdbc.core.dialect.oracle.OracleTypeMapper;
import com.link.up.connector.jdbc.core.dialect.postgres.PostgresTypeMapper;
import com.link.up.connector.jdbc.core.dialect.sqlserver.SqlServerJdbcUrl;
import com.link.up.connector.jdbc.core.dialect.sqlserver.SqlServerTypeMapper;

import java.util.Locale;

/**
 * Generates the CREATE TABLE statement used for offline sink preparation.
 */
final class JdbcCreateTableSqlResolver {

    private JdbcCreateTableSqlResolver() {
    }

    static String resolve(
            JdbcDialect dialect,
            JdbcConnectionConfig connectionConfig,
            CatalogTable table) {

        if (dialect == null
                || connectionConfig == null
                || table == null) {

            return null;
        }

        if (DatabaseIdentifier.MYSQL
                .equalsIgnoreCase(
                        dialect.name())) {

            TablePath targetPath =
                    resolveTargetPath(
                            connectionConfig,
                            table.getTablePath());

            if (targetPath == null) {
                return null;
            }

            CatalogTable ddlTable =
                    table.getTablePath()
                            .equals(targetPath)
                            ? table
                            : table.withPath(
                                    targetPath);

            return new MySqlCreateTableSqlBuilder(
                    targetPath,
                    ddlTable,
                    new MySqlTypeMapper(false))
                    .build();
        }

        if (DatabaseIdentifier.OCEANBASE
                .equalsIgnoreCase(
                        dialect.name())) {

            TablePath targetPath =
                    resolveTargetPath(
                            connectionConfig,
                            table.getTablePath());

            if (targetPath == null) {
                return null;
            }

            CatalogTable ddlTable =
                    table.getTablePath()
                            .equals(targetPath)
                            ? table
                            : table.withPath(
                                    targetPath);

            OceanBaseCompatibleMode mode =
                    OceanBaseCompatibleMode.from(
                            connectionConfig
                                    .getCompatibleMode());

            if (mode.isMySql()) {
                return new MySqlCreateTableSqlBuilder(
                        targetPath,
                        ddlTable,
                        new MySqlTypeMapper(false))
                        .build();
            }

            return new OracleCreateTableSqlBuilder(
                    targetPath,
                    ddlTable,
                    new OracleTypeMapper())
                    .build();
        }

        if (DatabaseIdentifier.POSTGRESQL
                .equalsIgnoreCase(
                        dialect.name())) {

            TablePath targetPath =
                    resolveTargetPath(
                            connectionConfig,
                            table.getTablePath());

            if (targetPath == null) {
                return null;
            }

            CatalogTable ddlTable =
                    table.getTablePath()
                            .equals(targetPath)
                            ? table
                            : table.withPath(
                                    targetPath);

            return new PostgresCreateTableSqlBuilder(
                    targetPath,
                    ddlTable,
                    new PostgresTypeMapper())
                    .build();
        }

        if (DatabaseIdentifier.ORACLE
                .equalsIgnoreCase(
                        dialect.name())) {

            TablePath targetPath =
                    resolveTargetPath(
                            connectionConfig,
                            table.getTablePath());

            if (targetPath == null) {
                return null;
            }

            CatalogTable ddlTable =
                    table.getTablePath()
                            .equals(targetPath)
                            ? table
                            : table.withPath(
                                    targetPath);

            return new OracleCreateTableSqlBuilder(
                    targetPath,
                    ddlTable,
                    new OracleTypeMapper())
                    .build();
        }

        if (DatabaseIdentifier.SQLSERVER
                .equalsIgnoreCase(
                        dialect.name())) {

            TablePath targetPath =
                    resolveTargetPath(
                            connectionConfig,
                            table.getTablePath());

            if (targetPath == null) {
                return null;
            }

            CatalogTable ddlTable =
                    table.getTablePath()
                            .equals(targetPath)
                            ? table
                            : table.withPath(
                                    targetPath);

            return new SqlServerCreateTableSqlBuilder(
                    targetPath,
                    ddlTable,
                    new SqlServerTypeMapper())
                    .build();
        }

        /*
         * Future JDBC dialects can add their CREATE TABLE builder here
         * without changing the connector-neutral protocol.
         */
        return null;
    }

    static TablePath resolveTargetPath(
            JdbcConnectionConfig connectionConfig,
            TablePath tablePath) {

        if (connectionConfig == null
                || tablePath == null) {

            return tablePath;
        }

        if (isOceanBase(
                connectionConfig)) {

            String database =
                    OceanBaseJdbcUrl.databaseName(
                            connectionConfig.getUrl());

            OceanBaseCompatibleMode mode =
                    OceanBaseCompatibleMode.from(
                            connectionConfig
                                    .getCompatibleMode());

            if (mode.isOracle()) {
                if (!hasText(database)) {
                    return null;
                }

                String schema =
                        resolveOracleSchema(
                                connectionConfig,
                                tablePath,
                                database);

                if (!hasText(schema)) {
                    return null;
                }

                return TablePath.of(
                        database,
                        schema,
                        tablePath.getTableName());
            }

            if (hasText(database)) {
                return TablePath.of(
                        database,
                        tablePath.getTableName());
            }

            if (hasText(
                    tablePath.getDatabaseName())) {

                return TablePath.of(
                        tablePath.getDatabaseName(),
                        tablePath.getTableName());
            }

            if (hasText(
                    connectionConfig.getSchema())) {

                return TablePath.of(
                        connectionConfig
                                .getSchema(),
                        tablePath.getTableName());
            }

            return null;
        }

        if (isSqlServer(
                connectionConfig)) {

            String database =
                    SqlServerJdbcUrl.databaseName(
                            connectionConfig.getUrl());

            String pathDatabase =
                    tablePath.getDatabaseName();

            String pathSchema =
                    tablePath.getSchemaName();

            String schema;

            if (!hasText(pathDatabase)) {
                schema = hasText(pathSchema)
                        ? pathSchema.trim()
                        : resolveSqlServerSchema(
                                connectionConfig);
            } else if (hasText(database)
                    && database.equalsIgnoreCase(
                            pathDatabase)) {
                schema = hasText(pathSchema)
                        ? pathSchema.trim()
                        : resolveSqlServerSchema(
                                connectionConfig);
            } else {
                /*
                 * Do not leak a MySQL/PostgreSQL/Oracle source database/schema
                 * into an unqualified SQL Server target.
                 */
                schema = resolveSqlServerSchema(
                        connectionConfig);
            }

            return hasText(database)
                    ? TablePath.of(
                            database,
                            schema,
                            tablePath.getTableName())
                    : TablePath.of(
                            null,
                            schema,
                            tablePath.getTableName());
        }

        if (isOracle(
                connectionConfig)) {

            String database =
                    OracleJdbcUrl.databaseName(
                            connectionConfig.getUrl());

            if (!hasText(database)) {
                return null;
            }

            String schema =
                    resolveOracleSchema(
                            connectionConfig,
                            tablePath,
                            database);

            if (!hasText(schema)) {
                return null;
            }

            return TablePath.of(
                    database,
                    schema,
                    tablePath.getTableName());
        }

        if (isPostgres(
                connectionConfig)) {

            String database =
                    databaseFromUrl(
                            connectionConfig.getUrl());

            if (!hasText(database)) {
                return null;
            }

            String schema =
                    tablePath.getSchemaName();

            if (!hasText(schema)) {
                schema =
                        connectionConfig.getSchema();
            }

            if (!hasText(schema)) {
                schema = "public";
            }

            return TablePath.of(
                    database,
                    schema,
                    tablePath.getTableName());
        }

        if (hasText(
                tablePath.getDatabaseName())) {

            return tablePath;
        }

        String database =
                connectionConfig.getSchema();

        if (!hasText(database)) {
            database =
                    databaseFromUrl(
                            connectionConfig.getUrl());
        }

        if (!hasText(database)) {
            return null;
        }

        return TablePath.of(
                database,
                tablePath.getTableName());
    }

    private static String resolveSqlServerSchema(
            JdbcConnectionConfig config) {

        return hasText(
                config.getSchema())
                ? config.getSchema().trim()
                : "dbo";
    }

    private static String resolveOracleSchema(
            JdbcConnectionConfig config,
            TablePath tablePath,
            String currentDatabase) {

        String pathSchema =
                tablePath.getSchemaName();

        String pathDatabase =
                tablePath.getDatabaseName();

        /*
         * A schema with no database is a target schema.table parsed by the
         * Oracle dialect. Preserve it. Also preserve a three-part target path
         * when its logical database matches this Oracle service.
         */
        if (hasText(pathSchema)
                && (!hasText(pathDatabase)
                || currentDatabase
                .equalsIgnoreCase(
                        pathDatabase))) {

            return pathSchema.trim();
        }

        if (hasText(
                config.getSchema())) {

            return config.getSchema()
                    .trim();
        }

        if (hasText(
                config.getUsername())) {

            return config.getUsername()
                    .trim()
                    .toUpperCase(
                            Locale.ROOT);
        }

        return hasText(pathSchema)
                ? pathSchema.trim()
                : null;
    }

    private static boolean isPostgres(
            JdbcConnectionConfig config) {

        if (DatabaseIdentifier.POSTGRESQL
                .equalsIgnoreCase(
                        config.getDialect())) {

            return true;
        }

        String url =
                config.getUrl();

        return url != null
                && url.trim()
                .toLowerCase()
                .startsWith(
                        "jdbc:postgresql:");
    }

    private static boolean isOceanBase(
            JdbcConnectionConfig config) {

        if (DatabaseIdentifier.OCEANBASE
                .equalsIgnoreCase(
                        config.getDialect())) {

            return true;
        }

        return OceanBaseJdbcUrl.accepts(
                config.getUrl());
    }

    private static boolean isOracle(
            JdbcConnectionConfig config) {

        if (DatabaseIdentifier.ORACLE
                .equalsIgnoreCase(
                        config.getDialect())) {

            return true;
        }

        return OracleJdbcUrl.accepts(
                config.getUrl());
    }

    private static boolean isSqlServer(
            JdbcConnectionConfig config) {

        if (DatabaseIdentifier.SQLSERVER
                .equalsIgnoreCase(
                        config.getDialect())) {

            return true;
        }

        return SqlServerJdbcUrl.accepts(
                config.getUrl());
    }

    private static String databaseFromUrl(
            String url) {

        if (!hasText(url)) {
            return null;
        }

        String normalized =
                url.trim();

        int protocolSeparator =
                normalized.indexOf(
                        "://");

        if (protocolSeparator < 0) {
            return null;
        }

        int databaseStart =
                normalized.indexOf(
                        '/',
                        protocolSeparator + 3);

        if (databaseStart < 0
                || databaseStart
                == normalized.length() - 1) {

            return null;
        }

        int databaseEnd =
                normalized.length();

        int queryStart =
                normalized.indexOf(
                        '?',
                        databaseStart + 1);

        if (queryStart >= 0) {
            databaseEnd =
                    queryStart;
        }

        int fragmentStart =
                normalized.indexOf(
                        '#',
                        databaseStart + 1);

        if (fragmentStart >= 0
                && fragmentStart
                < databaseEnd) {

            databaseEnd =
                    fragmentStart;
        }

        String database =
                normalized.substring(
                        databaseStart + 1,
                        databaseEnd)
                        .trim();

        return hasText(database)
                ? database
                : null;
    }

    private static boolean hasText(
            String value) {

        return value != null
                && !value.trim()
                .isEmpty();
    }
}
