package com.link.up.connector.jdbc.catalog.postgres;

import com.link.up.api.table.catalog.CatalogTable;
import com.link.up.api.table.catalog.Column;
import com.link.up.api.table.catalog.PrimaryKey;
import com.link.up.api.table.catalog.TablePath;
import com.link.up.api.table.catalog.TableSchema;
import com.link.up.api.table.catalog.WritableCatalog;
import com.link.up.api.table.catalog.exception.CatalogException;
import com.link.up.api.table.catalog.exception.DatabaseAlreadyExistsException;
import com.link.up.api.table.catalog.exception.DatabaseNotFoundException;
import com.link.up.api.table.catalog.exception.TableAlreadyExistsException;
import com.link.up.api.table.catalog.exception.TableNotFoundException;
import com.link.up.connector.jdbc.catalog.AbstractJdbcCatalog;
import com.link.up.connector.jdbc.catalog.JdbcCatalogConfig;
import com.link.up.connector.jdbc.core.dialect.postgres.PostgresTypeMapper;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * PostgreSQL 离线 JDBC Catalog。
 *
 * <p>负责数据库、Schema、表和字段元数据，以及离线 Sink 准备阶段需要的
 * CREATE/DROP/TRUNCATE/ADD COLUMN。它不处理 CDC/WAL 或运行时 Schema Event。
 */
public final class PostgresCatalog
        extends AbstractJdbcCatalog
        implements WritableCatalog {

    public static final String DIALECT =
            "postgresql";

    public static final String TABLE_OPTION_DIALECT =
            "dialect";

    private static final String DEFAULT_SCHEMA =
            "public";

    private static final String LIST_DATABASES_SQL =
            "SELECT datname "
                    + "FROM pg_catalog.pg_database "
                    + "WHERE datistemplate = false "
                    + "ORDER BY datname";

    private static final String DATABASE_EXISTS_SQL =
            "SELECT 1 "
                    + "FROM pg_catalog.pg_database "
                    + "WHERE datname = ?";

    private static final String LIST_SCHEMAS_SQL =
            "SELECT schema_name "
                    + "FROM information_schema.schemata "
                    + "ORDER BY schema_name";

    private static final String LIST_TABLES_SQL =
            "SELECT table_name "
                    + "FROM information_schema.tables "
                    + "WHERE table_schema = ? "
                    + "AND table_type = 'BASE TABLE' "
                    + "ORDER BY table_name";

    private static final String TABLE_EXISTS_SQL =
            "SELECT 1 "
                    + "FROM information_schema.tables "
                    + "WHERE table_catalog = ? "
                    + "AND table_schema = ? "
                    + "AND table_name = ? "
                    + "AND table_type = 'BASE TABLE'";

    private static final String SELECT_COLUMNS_SQL =
            "SELECT "
                    + "c.column_name AS COLUMN_NAME, "
                    + "c.data_type AS DATA_TYPE, "
                    + "c.udt_name AS UDT_NAME, "
                    + "c.character_maximum_length AS CHARACTER_MAXIMUM_LENGTH, "
                    + "c.numeric_precision AS NUMERIC_PRECISION, "
                    + "c.numeric_scale AS NUMERIC_SCALE, "
                    + "c.datetime_precision AS DATETIME_PRECISION, "
                    + "c.is_nullable AS IS_NULLABLE, "
                    + "c.column_default AS COLUMN_DEFAULT, "
                    + "c.is_identity AS IS_IDENTITY, "
                    + "pg_catalog.col_description(cls.oid, c.ordinal_position) "
                    + "AS COLUMN_COMMENT "
                    + "FROM information_schema.columns c "
                    + "JOIN pg_catalog.pg_namespace ns "
                    + "ON ns.nspname = c.table_schema "
                    + "JOIN pg_catalog.pg_class cls "
                    + "ON cls.relnamespace = ns.oid "
                    + "AND cls.relname = c.table_name "
                    + "AND cls.relkind IN ('r', 'p') "
                    + "WHERE c.table_catalog = ? "
                    + "AND c.table_schema = ? "
                    + "AND c.table_name = ? "
                    + "ORDER BY c.ordinal_position";

    private static final String SELECT_PRIMARY_KEY_SQL =
            "SELECT "
                    + "tc.constraint_name AS CONSTRAINT_NAME, "
                    + "kcu.column_name AS COLUMN_NAME "
                    + "FROM information_schema.table_constraints tc "
                    + "JOIN information_schema.key_column_usage kcu "
                    + "ON tc.constraint_catalog = kcu.constraint_catalog "
                    + "AND tc.constraint_schema = kcu.constraint_schema "
                    + "AND tc.constraint_name = kcu.constraint_name "
                    + "WHERE tc.table_catalog = ? "
                    + "AND tc.table_schema = ? "
                    + "AND tc.table_name = ? "
                    + "AND tc.constraint_type = 'PRIMARY KEY' "
                    + "ORDER BY kcu.ordinal_position";

    private static final String SELECT_TABLE_META_SQL =
            "SELECT "
                    + "pg_catalog.obj_description(cls.oid, 'pg_class') "
                    + "AS TABLE_COMMENT "
                    + "FROM pg_catalog.pg_class cls "
                    + "JOIN pg_catalog.pg_namespace ns "
                    + "ON ns.oid = cls.relnamespace "
                    + "WHERE ns.nspname = ? "
                    + "AND cls.relname = ? "
                    + "AND cls.relkind IN ('r', 'p')";

    private final String defaultSchema;
    private final PostgresTypeMapper typeMapper;

    public PostgresCatalog(
            String catalogName,
            JdbcCatalogConfig config,
            String defaultSchema) {

        super(
                catalogName,
                config,
                JdbcUrlInfo.parsePostgres(
                        config.getUrl()));

        this.defaultSchema =
                hasText(defaultSchema)
                        ? defaultSchema.trim()
                        : DEFAULT_SCHEMA;

        this.typeMapper =
                new PostgresTypeMapper();
    }

    @Override
    public List<String> listDatabases()
            throws CatalogException {

        checkOpened();

        try (Connection connection =
                     openDefaultConnection();
             PreparedStatement statement =
                     connection.prepareStatement(
                             LIST_DATABASES_SQL);
             ResultSet resultSet =
                     statement.executeQuery()) {

            List<String> databases =
                    new ArrayList<String>();

            while (resultSet.next()) {
                databases.add(
                        resultSet.getString(1));
            }

            return databases;

        } catch (SQLException e) {
            throw new CatalogException(
                    "获取 PostgreSQL 数据库列表失败",
                    e);
        }
    }

    @Override
    public List<String> listSchemas(
            String databaseName)
            throws CatalogException {

        checkOpened();

        String database =
                resolveDatabaseName(
                        databaseName);

        try (Connection connection =
                     openDatabaseConnection(
                             database);
             PreparedStatement statement =
                     connection.prepareStatement(
                             LIST_SCHEMAS_SQL);
             ResultSet resultSet =
                     statement.executeQuery()) {

            List<String> schemas =
                    new ArrayList<String>();

            while (resultSet.next()) {
                schemas.add(
                        resultSet.getString(1));
            }

            return schemas;

        } catch (SQLException e) {
            throw new CatalogException(
                    "获取 PostgreSQL Schema 列表失败，database="
                            + database,
                    e);
        }
    }

    @Override
    public List<TablePath> listTables(
            String databaseName,
            String schemaName)
            throws CatalogException {

        checkOpened();

        String database =
                resolveDatabaseName(
                        databaseName);

        String schema =
                resolveSchemaName(
                        schemaName);

        if (!databaseExists(database)) {
            throw new DatabaseNotFoundException(
                    catalogName,
                    database);
        }

        try (Connection connection =
                     openDatabaseConnection(
                             database);
             PreparedStatement statement =
                     connection.prepareStatement(
                             LIST_TABLES_SQL)) {

            statement.setString(
                    1,
                    schema);

            try (ResultSet resultSet =
                         statement.executeQuery()) {

                List<TablePath> tables =
                        new ArrayList<TablePath>();

                while (resultSet.next()) {
                    tables.add(
                            TablePath.of(
                                    database,
                                    schema,
                                    resultSet.getString(
                                            "table_name")));
                }

                return tables;
            }

        } catch (SQLException e) {
            throw new CatalogException(
                    "获取 PostgreSQL 表列表失败，database="
                            + database
                            + ", schema="
                            + schema,
                    e);
        }
    }

    public boolean databaseExists(
            String databaseName)
            throws CatalogException {

        if (!hasText(databaseName)) {
            return false;
        }

        checkOpened();

        try (Connection connection =
                     openDefaultConnection();
             PreparedStatement statement =
                     connection.prepareStatement(
                             DATABASE_EXISTS_SQL)) {

            statement.setString(
                    1,
                    databaseName);

            try (ResultSet resultSet =
                         statement.executeQuery()) {

                return resultSet.next();
            }

        } catch (SQLException e) {
            throw new CatalogException(
                    "检查 PostgreSQL 数据库是否存在失败，database="
                            + databaseName,
                    e);
        }
    }

    @Override
    public boolean tableExists(
            TablePath tablePath)
            throws CatalogException {

        checkOpened();

        TablePath normalized =
                normalizePostgresTablePath(
                        tablePath);

        try (Connection connection =
                     openDatabaseConnection(
                             normalized.getDatabaseName());
             PreparedStatement statement =
                     connection.prepareStatement(
                             TABLE_EXISTS_SQL)) {

            statement.setString(
                    1,
                    normalized.getDatabaseName());

            statement.setString(
                    2,
                    normalized.getSchemaName());

            statement.setString(
                    3,
                    normalized.getTableName());

            try (ResultSet resultSet =
                         statement.executeQuery()) {

                return resultSet.next();
            }

        } catch (SQLException e) {
            throw new CatalogException(
                    "检查 PostgreSQL 表是否存在失败，table="
                            + normalized,
                    e);
        }
    }

    @Override
    public CatalogTable getTable(
            TablePath tablePath)
            throws CatalogException,
            TableNotFoundException {

        checkOpened();

        TablePath normalized =
                normalizePostgresTablePath(
                        tablePath);

        try (Connection connection =
                     openDatabaseConnection(
                             normalized.getDatabaseName())) {

            List<Column> columns =
                    readColumns(
                            connection,
                            normalized);

            if (columns.isEmpty()) {
                throw new TableNotFoundException(
                        catalogName,
                        normalized);
            }

            PrimaryKey primaryKey =
                    readPrimaryKey(
                            connection,
                            normalized);

            String comment =
                    readTableComment(
                            connection,
                            normalized);

            TableSchema tableSchema =
                    TableSchema.builder()
                            .columns(columns)
                            .primaryKey(primaryKey)
                            .build();

            CatalogTable.Builder builder =
                    CatalogTable.builder(
                            normalized,
                            tableSchema)
                            .option(
                                    TABLE_OPTION_DIALECT,
                                    DIALECT);

            if (hasText(comment)) {
                builder.comment(comment);
            }

            return builder.build();

        } catch (TableNotFoundException e) {
            throw e;
        } catch (SQLException e) {
            throw new CatalogException(
                    "获取 PostgreSQL 表结构失败，table="
                            + normalized,
                    e);
        }
    }

    @Override
    public void createDatabase(
            String databaseName,
            boolean ignoreIfExists)
            throws CatalogException,
            DatabaseAlreadyExistsException {

        checkOpened();

        if (databaseExists(databaseName)) {
            if (ignoreIfExists) {
                return;
            }

            throw new DatabaseAlreadyExistsException(
                    catalogName,
                    databaseName);
        }

        String sql =
                "CREATE DATABASE "
                        + quoteIdentifier(
                        databaseName);

        try (Connection connection =
                     openAdministrationConnection()) {

            execute(
                    connection,
                    sql);

        } catch (SQLException e) {
            throw new CatalogException(
                    "创建 PostgreSQL 数据库失败，database="
                            + databaseName,
                    e);
        }
    }

    @Override
    public void dropDatabase(
            String databaseName,
            boolean ignoreIfNotExists)
            throws CatalogException,
            DatabaseNotFoundException {

        checkOpened();

        if (!databaseExists(databaseName)) {
            if (ignoreIfNotExists) {
                return;
            }

            throw new DatabaseNotFoundException(
                    catalogName,
                    databaseName);
        }

        String sql =
                "DROP DATABASE "
                        + quoteIdentifier(
                        databaseName);

        try (Connection connection =
                     openAdministrationConnection()) {

            execute(
                    connection,
                    sql);

        } catch (SQLException e) {
            throw new CatalogException(
                    "删除 PostgreSQL 数据库失败，database="
                            + databaseName,
                    e);
        }
    }

    @Override
    public void createTable(
            CatalogTable table,
            boolean ignoreIfExists)
            throws CatalogException,
            DatabaseNotFoundException,
            TableAlreadyExistsException {

        checkOpened();

        TablePath tablePath =
                normalizePostgresTablePath(
                        table.getTablePath());

        String databaseName =
                tablePath.getDatabaseName();

        if (!databaseExists(databaseName)) {
            throw new DatabaseNotFoundException(
                    catalogName,
                    databaseName);
        }

        if (tableExists(tablePath)) {
            if (ignoreIfExists) {
                return;
            }

            throw new TableAlreadyExistsException(
                    catalogName,
                    tablePath);
        }

        CatalogTable ddlTable =
                table.getTablePath()
                        .equals(tablePath)
                        ? table
                        : table.withPath(
                                tablePath);

        PostgresCreateTableSqlBuilder builder =
                new PostgresCreateTableSqlBuilder(
                        tablePath,
                        ddlTable,
                        typeMapper);

        try (Connection connection =
                     openDatabaseConnection(
                             databaseName)) {

            executeStatements(
                    connection,
                    builder.buildStatements());

        } catch (SQLException e) {
            throw new CatalogException(
                    "创建 PostgreSQL 表失败，table="
                            + tablePath,
                    e);
        }
    }

    @Override
    public void addColumn(
            TablePath tablePath,
            Column column)
            throws CatalogException,
            TableNotFoundException {

        checkOpened();

        TablePath normalized =
                normalizePostgresTablePath(
                        tablePath);

        if (!tableExists(normalized)) {
            throw new TableNotFoundException(
                    catalogName,
                    normalized);
        }

        CatalogTable targetTable =
                getTable(normalized);

        String definition =
                new PostgresCreateTableSqlBuilder(
                        normalized,
                        targetTable,
                        typeMapper)
                        .buildColumnDefinition(
                                column,
                                false);

        String sql =
                "ALTER TABLE "
                        + quoteTable(normalized)
                        + " ADD COLUMN "
                        + definition;

        try (Connection connection =
                     openDatabaseConnection(
                             normalized.getDatabaseName())) {

            execute(
                    connection,
                    sql);

            if (hasText(
                    column.getComment())) {

                execute(
                        connection,
                        "COMMENT ON COLUMN "
                                + quoteTable(
                                normalized)
                                + "."
                                + quoteIdentifier(
                                column.getName())
                                + " IS '"
                                + escapeLiteral(
                                column.getComment())
                                + "'");
            }

        } catch (SQLException e) {
            throw new CatalogException(
                    "增加 PostgreSQL 字段失败，table="
                            + normalized
                            + ", column="
                            + column.getName(),
                    e);
        }
    }

    @Override
    public void dropTable(
            TablePath tablePath,
            boolean ignoreIfNotExists)
            throws CatalogException,
            TableNotFoundException {

        checkOpened();

        TablePath normalized =
                normalizePostgresTablePath(
                        tablePath);

        if (!tableExists(normalized)) {
            if (ignoreIfNotExists) {
                return;
            }

            throw new TableNotFoundException(
                    catalogName,
                    normalized);
        }

        try (Connection connection =
                     openDatabaseConnection(
                             normalized.getDatabaseName())) {

            execute(
                    connection,
                    "DROP TABLE "
                            + quoteTable(
                            normalized));

        } catch (SQLException e) {
            throw new CatalogException(
                    "删除 PostgreSQL 表失败，table="
                            + normalized,
                    e);
        }
    }

    @Override
    public void truncateTable(
            TablePath tablePath,
            boolean ignoreIfNotExists)
            throws CatalogException,
            TableNotFoundException {

        checkOpened();

        TablePath normalized =
                normalizePostgresTablePath(
                        tablePath);

        if (!tableExists(normalized)) {
            if (ignoreIfNotExists) {
                return;
            }

            throw new TableNotFoundException(
                    catalogName,
                    normalized);
        }

        try (Connection connection =
                     openDatabaseConnection(
                             normalized.getDatabaseName())) {

            execute(
                    connection,
                    "TRUNCATE TABLE "
                            + quoteTable(
                            normalized));

        } catch (SQLException e) {
            throw new CatalogException(
                    "清空 PostgreSQL 表失败，table="
                            + normalized,
                    e);
        }
    }

    private List<Column> readColumns(
            Connection connection,
            TablePath tablePath)
            throws SQLException {

        try (PreparedStatement statement =
                     connection.prepareStatement(
                             SELECT_COLUMNS_SQL)) {

            statement.setString(
                    1,
                    tablePath.getDatabaseName());

            statement.setString(
                    2,
                    tablePath.getSchemaName());

            statement.setString(
                    3,
                    tablePath.getTableName());

            try (ResultSet resultSet =
                         statement.executeQuery()) {

                List<Column> columns =
                        new ArrayList<Column>();

                while (resultSet.next()) {
                    columns.add(
                            typeMapper.toColumn(
                                    resultSet));
                }

                return columns;
            }
        }
    }

    private PrimaryKey readPrimaryKey(
            Connection connection,
            TablePath tablePath)
            throws SQLException {

        try (PreparedStatement statement =
                     connection.prepareStatement(
                             SELECT_PRIMARY_KEY_SQL)) {

            statement.setString(
                    1,
                    tablePath.getDatabaseName());

            statement.setString(
                    2,
                    tablePath.getSchemaName());

            statement.setString(
                    3,
                    tablePath.getTableName());

            try (ResultSet resultSet =
                         statement.executeQuery()) {

                String primaryKeyName =
                        null;

                List<String> columns =
                        new ArrayList<String>();

                while (resultSet.next()) {
                    if (primaryKeyName == null) {
                        primaryKeyName =
                                resultSet.getString(
                                        "CONSTRAINT_NAME");
                    }

                    columns.add(
                            resultSet.getString(
                                    "COLUMN_NAME"));
                }

                if (columns.isEmpty()) {
                    return null;
                }

                return PrimaryKey.of(
                        primaryKeyName,
                        columns);
            }
        }
    }

    private String readTableComment(
            Connection connection,
            TablePath tablePath)
            throws SQLException {

        try (PreparedStatement statement =
                     connection.prepareStatement(
                             SELECT_TABLE_META_SQL)) {

            statement.setString(
                    1,
                    tablePath.getSchemaName());

            statement.setString(
                    2,
                    tablePath.getTableName());

            try (ResultSet resultSet =
                         statement.executeQuery()) {

                if (!resultSet.next()) {
                    return null;
                }

                return normalize(
                        resultSet.getString(
                                "TABLE_COMMENT"));
            }
        }
    }

    private void executeStatements(
            Connection connection,
            List<String> statements)
            throws SQLException {

        for (String sql : statements) {
            execute(
                    connection,
                    sql);
        }
    }

    private Connection openAdministrationConnection()
            throws SQLException {

        String defaultDatabase =
                getDefaultDatabase()
                        .orElse(null);

        if (!"postgres".equalsIgnoreCase(
                defaultDatabase)) {

            try {
                return openDatabaseConnection(
                        "postgres");
            } catch (SQLException ignored) {
                /*
                 * 某些托管 PostgreSQL 可能没有暴露 postgres 库，
                 * 回退到当前数据库，让服务端返回更明确的权限/当前库错误。
                 */
            }
        }

        return openDefaultConnection();
    }

    private TablePath normalizePostgresTablePath(
            TablePath tablePath) {

        if (tablePath == null) {
            throw new IllegalArgumentException(
                    "tablePath must not be null");
        }

        /*
         * PostgreSQL 不能在同一 JDBC Connection 中跨 database 引用表。
         * database 始终由 JDBC URL 决定；TablePath 中只使用 schema/table。
         */
        String database =
                resolveDatabaseName(null);

        String schema =
                resolveSchemaName(
                        tablePath.getSchemaName());

        return TablePath.of(
                database,
                schema,
                tablePath.getTableName());
    }

    private String resolveDatabaseName(
            String databaseName) {

        if (hasText(databaseName)) {
            return databaseName.trim();
        }

        return getDefaultDatabase()
                .orElseThrow(
                        () ->
                                new IllegalArgumentException(
                                        "PostgreSQL JDBC URL 必须指定数据库"));
    }

    private String resolveSchemaName(
            String schemaName) {

        return hasText(schemaName)
                ? schemaName.trim()
                : defaultSchema;
    }

    protected static String quoteTable(
            TablePath tablePath) {

        return quoteIdentifier(
                tablePath.getSchemaName())
                + "."
                + quoteIdentifier(
                tablePath.getTableName());
    }

    protected static String quoteIdentifier(
            String value) {

        if (!hasText(value)) {
            throw new IllegalArgumentException(
                    "identifier must not be empty");
        }

        return "\""
                + value.trim()
                .replace("\"", "\"\"")
                + "\"";
    }

    private static String escapeLiteral(
            String value) {

        return value.replace(
                "'",
                "''");
    }

    private static boolean hasText(
            String value) {

        return normalize(value) != null;
    }

    private static String normalize(
            String value) {

        if (value == null) {
            return null;
        }

        String normalized =
                value.trim();

        return normalized.isEmpty()
                ? null
                : normalized;
    }
}
