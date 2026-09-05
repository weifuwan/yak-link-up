package com.link.up.connector.jdbc.sink;

import com.link.up.api.table.catalog.CatalogTable;
import com.link.up.api.table.catalog.TablePath;
import com.link.up.connector.jdbc.catalog.mysql.MySqlCreateTableSqlBuilder;
import com.link.up.connector.jdbc.catalog.mysql.MySqlTypeMapper;
import com.link.up.connector.jdbc.catalog.postgres.PostgresCreateTableSqlBuilder;
import com.link.up.connector.jdbc.config.JdbcConnectionConfig;
import com.link.up.connector.jdbc.core.dialect.DatabaseIdentifier;
import com.link.up.connector.jdbc.core.dialect.JdbcDialect;
import com.link.up.connector.jdbc.core.dialect.postgres.PostgresTypeMapper;

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
