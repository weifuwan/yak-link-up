package com.link.up.connector.jdbc.catalog.gbase.gbase8c;

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
import com.link.up.connector.jdbc.catalog.JdbcCatalogConfig;
import com.link.up.connector.jdbc.core.dialect.DatabaseIdentifier;
import com.link.up.connector.jdbc.core.dialect.gbase.gbase8c.GBase8cJdbcUrl;
import com.link.up.connector.jdbc.core.dialect.gbase.gbase8c.GBase8cTypeMapper;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * GBase 8c Catalog for bounded Source and existing-table Sink jobs.
 *
 * <p>Metadata follows the PG-compatible information_schema contract, while the JDBC connection
 * remains bound to one GBase 8c database. Sink preparation may validate and truncate an existing
 * target table, but all structure-changing DDL stays blocked until GBase 8c distribution and
 * compatibility-mode semantics are modeled explicitly.</p>
 */
public final class GBase8cCatalog implements WritableCatalog {

    public static final String TABLE_OPTION_DIALECT = "dialect";

    private static final String DEFAULT_SCHEMA = "public";

    private static final String LIST_SCHEMAS_SQL =
            "SELECT schema_name FROM information_schema.schemata ORDER BY schema_name";

    private static final String LIST_TABLES_SQL =
            "SELECT table_name FROM information_schema.tables "
                    + "WHERE table_schema = ? AND table_type = 'BASE TABLE' ORDER BY table_name";

    private static final String TABLE_EXISTS_SQL =
            "SELECT 1 FROM information_schema.tables "
                    + "WHERE table_schema = ? AND table_name = ? AND table_type = 'BASE TABLE'";

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
                    + "NULL AS IS_IDENTITY, "
                    + "NULL AS COLUMN_COMMENT "
                    + "FROM information_schema.columns c "
                    + "WHERE c.table_schema = ? AND c.table_name = ? "
                    + "ORDER BY c.ordinal_position";

    private static final String SELECT_PRIMARY_KEY_SQL =
            "SELECT tc.constraint_name AS CONSTRAINT_NAME, "
                    + "kcu.column_name AS COLUMN_NAME "
                    + "FROM information_schema.table_constraints tc "
                    + "JOIN information_schema.key_column_usage kcu "
                    + "ON tc.constraint_schema = kcu.constraint_schema "
                    + "AND tc.constraint_name = kcu.constraint_name "
                    + "WHERE tc.table_schema = ? AND tc.table_name = ? "
                    + "AND tc.constraint_type = 'PRIMARY KEY' "
                    + "ORDER BY kcu.ordinal_position";

    private final String catalogName;
    private final JdbcCatalogConfig config;
    private final String defaultDatabase;
    private final String defaultSchema;
    private final GBase8cTypeMapper typeMapper = new GBase8cTypeMapper();
    private volatile boolean opened;

    public GBase8cCatalog(
            String catalogName,
            JdbcCatalogConfig config,
            String defaultSchema) {
        if (catalogName == null || catalogName.trim().isEmpty()) {
            throw new IllegalArgumentException("catalogName must not be empty");
        }
        if (config == null) {
            throw new IllegalArgumentException("config must not be null");
        }
        if (!GBase8cJdbcUrl.accepts(config.getUrl())) {
            throw new IllegalArgumentException("非法 GBase 8c JDBC URL：" + config.getUrl());
        }
        String database = GBase8cJdbcUrl.databaseName(config.getUrl());
        if (!hasText(database)) {
            throw new IllegalArgumentException("GBase 8c JDBC URL 必须指定数据库");
        }
        this.catalogName = catalogName.trim();
        this.config = config;
        this.defaultDatabase = database;
        this.defaultSchema = hasText(defaultSchema) ? defaultSchema.trim() : DEFAULT_SCHEMA;
    }

    @Override
    public String name() {
        return catalogName;
    }

    @Override
    public synchronized void open() throws CatalogException {
        if (opened) {
            return;
        }
        loadDriver();
        try (Connection connection = newConnection()) {
            if (!connection.isValid(5)) {
                throw new CatalogException("GBase 8c Catalog 连接校验失败：" + config.getUrl());
            }
            opened = true;
        } catch (SQLException e) {
            throw new CatalogException("GBase 8c Catalog 连接失败：" + config.getUrl(), e);
        }
    }

    @Override
    public synchronized void close() {
        opened = false;
    }

    @Override
    public Optional<String> getDefaultDatabase() {
        return Optional.of(defaultDatabase);
    }

    @Override
    public List<String> listDatabases() throws CatalogException {
        checkOpened();
        return Collections.singletonList(defaultDatabase);
    }

    @Override
    public List<String> listSchemas(String databaseName) throws CatalogException {
        checkOpened();
        requireCurrentDatabase(databaseName);
        try (Connection connection = newConnection();
             PreparedStatement statement = connection.prepareStatement(LIST_SCHEMAS_SQL);
             ResultSet resultSet = statement.executeQuery()) {
            List<String> schemas = new ArrayList<String>();
            while (resultSet.next()) {
                schemas.add(resultSet.getString(1));
            }
            return schemas;
        } catch (SQLException e) {
            throw new CatalogException(
                    "获取 GBase 8c Schema 列表失败，database=" + defaultDatabase,
                    e);
        }
    }

    @Override
    public List<TablePath> listTables(
            String databaseName,
            String schemaName) throws CatalogException {
        checkOpened();
        requireCurrentDatabase(databaseName);
        String schema = resolveSchemaName(schemaName);
        try (Connection connection = newConnection();
             PreparedStatement statement = connection.prepareStatement(LIST_TABLES_SQL)) {
            statement.setString(1, schema);
            try (ResultSet resultSet = statement.executeQuery()) {
                List<TablePath> tables = new ArrayList<TablePath>();
                while (resultSet.next()) {
                    tables.add(TablePath.of(
                            defaultDatabase,
                            schema,
                            resultSet.getString("table_name")));
                }
                return tables;
            }
        } catch (SQLException e) {
            throw new CatalogException(
                    "获取 GBase 8c 表列表失败，database="
                            + defaultDatabase
                            + ", schema="
                            + schema,
                    e);
        }
    }

    @Override
    public boolean tableExists(TablePath tablePath) throws CatalogException {
        checkOpened();
        TablePath normalized = normalizeTablePath(tablePath);
        try (Connection connection = newConnection();
             PreparedStatement statement = connection.prepareStatement(TABLE_EXISTS_SQL)) {
            statement.setString(1, normalized.getSchemaName());
            statement.setString(2, normalized.getTableName());
            try (ResultSet resultSet = statement.executeQuery()) {
                return resultSet.next();
            }
        } catch (SQLException e) {
            throw new CatalogException("检查 GBase 8c 表是否存在失败，table=" + normalized, e);
        }
    }

    @Override
    public CatalogTable getTable(TablePath tablePath)
            throws CatalogException, TableNotFoundException {
        checkOpened();
        TablePath normalized = normalizeTablePath(tablePath);
        try (Connection connection = newConnection()) {
            List<Column> columns = readColumns(connection, normalized);
            if (columns.isEmpty()) {
                throw new TableNotFoundException(catalogName, normalized);
            }
            PrimaryKey primaryKey = readPrimaryKey(connection, normalized);
            TableSchema schema = TableSchema.builder()
                    .columns(columns)
                    .primaryKey(primaryKey)
                    .build();
            return CatalogTable.builder(normalized, schema)
                    .option(TABLE_OPTION_DIALECT, DatabaseIdentifier.GBASE8C)
                    .build();
        } catch (TableNotFoundException e) {
            throw e;
        } catch (SQLException e) {
            throw new CatalogException("获取 GBase 8c 表结构失败，table=" + normalized, e);
        }
    }

    @Override
    public void createDatabase(
            String databaseName,
            boolean ignoreIfExists)
            throws CatalogException, DatabaseAlreadyExistsException {
        throw unsupportedSchemaDdl("create database");
    }

    @Override
    public void dropDatabase(
            String databaseName,
            boolean ignoreIfNotExists)
            throws CatalogException, DatabaseNotFoundException {
        throw unsupportedSchemaDdl("drop database");
    }

    @Override
    public void createTable(
            CatalogTable table,
            boolean ignoreIfExists)
            throws CatalogException, DatabaseNotFoundException, TableAlreadyExistsException {
        throw unsupportedSchemaDdl("create table");
    }

    @Override
    public void addColumn(
            TablePath tablePath,
            Column column)
            throws CatalogException, TableNotFoundException {
        throw unsupportedSchemaDdl("add column");
    }

    @Override
    public void dropTable(
            TablePath tablePath,
            boolean ignoreIfNotExists)
            throws CatalogException, TableNotFoundException {
        throw unsupportedSchemaDdl("drop table");
    }

    @Override
    public void truncateTable(
            TablePath tablePath,
            boolean ignoreIfNotExists)
            throws CatalogException, TableNotFoundException {
        checkOpened();
        TablePath normalized = normalizeTablePath(tablePath);
        if (!tableExists(normalized)) {
            if (ignoreIfNotExists) {
                return;
            }
            throw new TableNotFoundException(catalogName, normalized);
        }

        String sql = "TRUNCATE TABLE "
                + quoteIdentifier(normalized.getSchemaName())
                + "."
                + quoteIdentifier(normalized.getTableName());
        try (Connection connection = newConnection();
             Statement statement = connection.createStatement()) {
            statement.execute(sql);
        } catch (SQLException e) {
            throw new CatalogException("清空 GBase 8c 表失败，table=" + normalized, e);
        }
    }

    private List<Column> readColumns(Connection connection, TablePath tablePath)
            throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(SELECT_COLUMNS_SQL)) {
            statement.setString(1, tablePath.getSchemaName());
            statement.setString(2, tablePath.getTableName());
            try (ResultSet resultSet = statement.executeQuery()) {
                List<Column> columns = new ArrayList<Column>();
                while (resultSet.next()) {
                    columns.add(typeMapper.toColumn(resultSet));
                }
                return columns;
            }
        }
    }

    private PrimaryKey readPrimaryKey(Connection connection, TablePath tablePath)
            throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(SELECT_PRIMARY_KEY_SQL)) {
            statement.setString(1, tablePath.getSchemaName());
            statement.setString(2, tablePath.getTableName());
            try (ResultSet resultSet = statement.executeQuery()) {
                String primaryKeyName = null;
                List<String> columns = new ArrayList<String>();
                while (resultSet.next()) {
                    if (primaryKeyName == null) {
                        primaryKeyName = resultSet.getString("CONSTRAINT_NAME");
                    }
                    columns.add(resultSet.getString("COLUMN_NAME"));
                }
                return columns.isEmpty() ? null : PrimaryKey.of(primaryKeyName, columns);
            }
        }
    }

    private TablePath normalizeTablePath(TablePath tablePath) {
        if (tablePath == null) {
            throw new IllegalArgumentException("tablePath must not be null");
        }
        requireCurrentDatabase(tablePath.getDatabaseName());
        if (!hasText(tablePath.getTableName())) {
            throw new IllegalArgumentException("table name must not be empty");
        }
        return TablePath.of(
                defaultDatabase,
                resolveSchemaName(tablePath.getSchemaName()),
                tablePath.getTableName().trim());
    }

    private void requireCurrentDatabase(String databaseName) {
        if (hasText(databaseName) && !defaultDatabase.equals(databaseName.trim())) {
            throw new IllegalArgumentException(
                    "GBase 8c bounded JDBC adapter is fixed to database="
                            + defaultDatabase
                            + " and does not support database="
                            + databaseName);
        }
    }

    private String resolveSchemaName(String schemaName) {
        return hasText(schemaName) ? schemaName.trim() : defaultSchema;
    }

    private Connection newConnection() throws SQLException {
        return DriverManager.getConnection(config.getUrl(), config.toConnectionProperties());
    }

    private void loadDriver() {
        if (!hasText(config.getDriverClass())) {
            return;
        }
        try {
            Class.forName(config.getDriverClass());
        } catch (ClassNotFoundException e) {
            throw new CatalogException("找不到 GBase 8c JDBC Driver：" + config.getDriverClass(), e);
        }
    }

    private void checkOpened() {
        if (!opened) {
            throw new IllegalStateException("Catalog 尚未打开，请先调用 open()");
        }
    }

    private static String quoteIdentifier(String identifier) {
        if (!hasText(identifier)) {
            throw new IllegalArgumentException("identifier must not be empty");
        }
        return "\"" + identifier.trim().replace("\"", "\"\"") + "\"";
    }

    private static CatalogException unsupportedSchemaDdl(String operation) {
        return new CatalogException(
                "GBase 8c existing-table Sink supports target-table DML only; "
                        + operation
                        + " is disabled until distribution and compatibility-mode DDL is modeled");
    }

    private static boolean hasText(String value) {
        return value != null && !value.trim().isEmpty();
    }
}
