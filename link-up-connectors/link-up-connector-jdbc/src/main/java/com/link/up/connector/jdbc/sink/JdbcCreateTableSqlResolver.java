package com.link.up.connector.jdbc.sink;

import com.link.up.api.table.catalog.CatalogTable;
import com.link.up.api.table.catalog.TablePath;
import com.link.up.connector.jdbc.catalog.dameng.DamengCreateTableSqlBuilder;
import com.link.up.connector.jdbc.catalog.db2.Db2CreateTableSqlBuilder;
import com.link.up.connector.jdbc.catalog.mysql.MySqlCreateTableSqlBuilder;
import com.link.up.connector.jdbc.catalog.mysql.MySqlTypeMapper;
import com.link.up.connector.jdbc.catalog.opengauss.OpenGaussCreateTableSqlBuilder;
import com.link.up.connector.jdbc.catalog.oracle.OracleCreateTableSqlBuilder;
import com.link.up.connector.jdbc.catalog.postgres.PostgresCreateTableSqlBuilder;
import com.link.up.connector.jdbc.catalog.sqlserver.SqlServerCreateTableSqlBuilder;
import com.link.up.connector.jdbc.config.JdbcConnectionConfig;
import com.link.up.connector.jdbc.core.dialect.DatabaseIdentifier;
import com.link.up.connector.jdbc.core.dialect.JdbcDialect;
import com.link.up.connector.jdbc.core.dialect.dameng.DamengJdbcUrl;
import com.link.up.connector.jdbc.core.dialect.dameng.DamengTypeMapper;
import com.link.up.connector.jdbc.core.dialect.db2.Db2JdbcUrl;
import com.link.up.connector.jdbc.core.dialect.db2.Db2TypeMapper;
import com.link.up.connector.jdbc.core.dialect.oceanbase.OceanBaseCompatibleMode;
import com.link.up.connector.jdbc.core.dialect.oceanbase.OceanBaseJdbcUrl;
import com.link.up.connector.jdbc.core.dialect.opengauss.OpenGaussJdbcUrl;
import com.link.up.connector.jdbc.core.dialect.opengauss.OpenGaussTypeMapper;
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

        if (DatabaseIdentifier.OPENGAUSS
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

            return new OpenGaussCreateTableSqlBuilder(
                    targetPath,
                    ddlTable,
                    new OpenGaussTypeMapper())
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

        if (DatabaseIdentifier.DB2
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

            return new Db2CreateTableSqlBuilder(
                    targetPath,
                    ddlTable,
                    new Db2TypeMapper())
                    .build();
        }

        if (DatabaseIdentifier.DAMENG
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

            return new DamengCreateTableSqlBuilder(
                    targetPath,
                    ddlTable,
                    new DamengTypeMapper())
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

        if (isOpenGauss(
                connectionConfig)) {

            String database =
                    OpenGaussJdbcUrl.databaseName(
                            connectionConfig.getUrl());

            if (!hasText(database)) {
                return null;
            }

            String schema =
                    resolveOpenGaussSchema(
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

        if (isDameng(
                connectionConfig)) {

            String schema =
                    resolveDamengSchema(
                            connectionConfig,
                            tablePath);

            if (!hasText(schema)) {
                return null;
            }

            return TablePath.of(
                    null,
                    schema,
                    tablePath.getTableName());
        }

        if (isDb2(
                connectionConfig)) {

            String database =
                    Db2JdbcUrl.databaseName(
                            connectionConfig.getUrl());

            if (!hasText(database)) {
                return null;
            }

            String schema =
                    resolveDb2Schema(
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

    private static String resolveOpenGaussSchema(
            JdbcConnectionConfig config,
            TablePath tablePath,
            String currentDatabase) {

        String pathSchema =
                tablePath.getSchemaName();

        String pathDatabase =
                tablePath.getDatabaseName();

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

        String currentSchema =
                OpenGaussJdbcUrl.currentSchema(
                        config.getUrl(),
                        config.getProperties());

        if (hasText(currentSchema)) {
            return currentSchema.trim();
        }

        if (hasText(
                config.getUsername())) {

            return config.getUsername()
                    .trim()
                    .toLowerCase(
                            Locale.ROOT);
        }

        return "public";
    }

    private static String resolveDamengSchema(
            JdbcConnectionConfig config,
            TablePath tablePath) {

        String pathSchema =
                tablePath.getSchemaName();

        String pathDatabase =
                tablePath.getDatabaseName();

        /*
         * schema.table parsed from an explicit target mapping is safe to
         * preserve. A three-part source path cannot be verified without a DM
         * connection, so prefer target connection settings to avoid leaking
         * source database/schema metadata into the target.
         */
        if (hasText(pathSchema)
                && !hasText(pathDatabase)) {

            return pathSchema.trim();
        }

        if (hasText(
                config.getSchema())) {

            return config.getSchema()
                    .trim();
        }

        String currentSchema =
                DamengJdbcUrl.schema(
                        config.getUrl(),
                        config.getProperties());

        if (hasText(currentSchema)) {
            return currentSchema.trim();
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

    private static String resolveDb2Schema(
            JdbcConnectionConfig config,
            TablePath tablePath,
            String currentDatabase) {

        String pathSchema =
                tablePath.getSchemaName();

        String pathDatabase =
                tablePath.getDatabaseName();

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

        String currentSchema =
                Db2JdbcUrl.currentSchema(
                        config.getUrl(),
                        config.getProperties());

        if (hasText(currentSchema)) {
            return currentSchema.trim();
        }

        if (hasText(
                config.getUsername())) {

            return config.getUsername()
                    .trim()
                    .toUpperCase(
                            Locale.ROOT);
        }

        return null;
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

    private static boolean isOpenGauss(
            JdbcConnectionConfig config) {

        if (DatabaseIdentifier.OPENGAUSS
                .equalsIgnoreCase(
                        config.getDialect())) {

            return true;
        }

        return OpenGaussJdbcUrl.accepts(
                config.getUrl());
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

    private static boolean isDb2(
            JdbcConnectionConfig config) {

        if (DatabaseIdentifier.DB2
                .equalsIgnoreCase(
                        config.getDialect())) {

            return true;
        }

        return Db2JdbcUrl.accepts(
                config.getUrl());
    }

    private static boolean isDameng(
            JdbcConnectionConfig config) {

        if (DatabaseIdentifier.DAMENG
                .equalsIgnoreCase(
                        config.getDialect())) {

            return true;
        }

        return DamengJdbcUrl.accepts(
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
