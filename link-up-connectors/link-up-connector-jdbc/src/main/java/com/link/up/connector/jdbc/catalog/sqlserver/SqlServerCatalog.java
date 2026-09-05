package com.link.up.connector.jdbc.catalog.sqlserver;

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
import com.link.up.connector.jdbc.core.dialect.sqlserver.SqlServerJdbcUrl;
import com.link.up.connector.jdbc.core.dialect.sqlserver.SqlServerTypeMapper;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/** SQL Server offline JDBC Catalog. */
public final class SqlServerCatalog implements WritableCatalog {

    public static final String DIALECT = "sqlserver";
    public static final String TABLE_OPTION_DIALECT = "dialect";
    private static final String DEFAULT_SCHEMA = "dbo";

    private static final String LIST_DATABASES_SQL =
            "SELECT name FROM sys.databases WHERE state = 0 ORDER BY name";
    private static final String DATABASE_EXISTS_SQL =
            "SELECT 1 FROM sys.databases WHERE name = ?";
    private static final String LIST_SCHEMAS_SQL =
            "SELECT name FROM sys.schemas ORDER BY name";
    private static final String SELECT_COLUMNS_SQL =
            "SELECT c.name AS COLUMN_NAME, t.name AS DATA_TYPE, "
                    + "c.max_length AS MAX_LENGTH, c.precision AS NUMERIC_PRECISION, "
                    + "c.scale AS NUMERIC_SCALE, c.is_nullable AS IS_NULLABLE, "
                    + "dc.definition AS COLUMN_DEFAULT, c.is_identity AS IS_IDENTITY, "
                    + "CAST(ep.value AS nvarchar(4000)) AS COLUMN_COMMENT "
                    + "FROM sys.columns c "
                    + "JOIN sys.tables tb ON c.object_id = tb.object_id "
                    + "JOIN sys.schemas s ON tb.schema_id = s.schema_id "
                    + "JOIN sys.types t ON c.user_type_id = t.user_type_id "
                    + "LEFT JOIN sys.default_constraints dc ON c.default_object_id = dc.object_id "
                    + "LEFT JOIN sys.extended_properties ep ON ep.major_id = c.object_id "
                    + "AND ep.minor_id = c.column_id AND ep.name = 'MS_Description' "
                    + "WHERE s.name = ? AND tb.name = ? ORDER BY c.column_id";
    private static final String SELECT_TABLE_COMMENT_SQL =
            "SELECT CAST(ep.value AS nvarchar(4000)) AS TABLE_COMMENT "
                    + "FROM sys.tables tb JOIN sys.schemas s ON tb.schema_id=s.schema_id "
                    + "LEFT JOIN sys.extended_properties ep ON ep.major_id=tb.object_id "
                    + "AND ep.minor_id=0 AND ep.name='MS_Description' "
                    + "WHERE s.name=? AND tb.name=?";

    private final String catalogName;
    private final JdbcCatalogConfig config;
    private final String configuredSchema;
    private final SqlServerTypeMapper typeMapper = new SqlServerTypeMapper();
    private volatile String defaultDatabase;
    private volatile boolean opened;

    public SqlServerCatalog(
            String catalogName, JdbcCatalogConfig config, String defaultSchema) {
        if (!hasText(catalogName)) {
            throw new IllegalArgumentException("catalogName must not be empty");
        }
        this.catalogName = catalogName.trim();
        this.config = Objects.requireNonNull(config, "config must not be null");
        this.defaultDatabase = SqlServerJdbcUrl.databaseName(config.getUrl());
        this.configuredSchema = hasText(defaultSchema) ? defaultSchema.trim() : DEFAULT_SCHEMA;
    }

    @Override
    public String name() {
        return catalogName;
    }

    @Override
    public Optional<String> getDefaultDatabase() {
        return Optional.ofNullable(defaultDatabase);
    }

    @Override
    public synchronized void open() throws CatalogException {
        if (opened) {
            return;
        }
        loadDriver();
        try (Connection connection = connection()) {
            if (!connection.isValid(5)) {
                throw new CatalogException("SQL Server Catalog 连接校验失败：" + config.getUrl());
            }
            if (!hasText(defaultDatabase)) {
                defaultDatabase = normalize(connection.getCatalog());
            }
            opened = true;
        } catch (SQLException e) {
            throw new CatalogException("SQL Server Catalog 连接失败：" + config.getUrl(), e);
        }
    }

    @Override
    public synchronized void close() {
        opened = false;
    }

    @Override
    public List<String> listDatabases() throws CatalogException {
        checkOpened();
        try (Connection connection = connection();
             PreparedStatement statement = connection.prepareStatement(LIST_DATABASES_SQL);
             ResultSet rs = statement.executeQuery()) {
            List<String> databases = new ArrayList<String>();
            while (rs.next()) {
                databases.add(rs.getString(1));
            }
            return databases;
        } catch (SQLException e) {
            throw new CatalogException("获取 SQL Server 数据库列表失败", e);
        }
    }

    public boolean databaseExists(String database) throws CatalogException {
        if (!hasText(database)) {
            return false;
        }
        checkOpened();
        try (Connection connection = connection();
             PreparedStatement statement = connection.prepareStatement(DATABASE_EXISTS_SQL)) {
            statement.setString(1, database.trim());
            try (ResultSet rs = statement.executeQuery()) {
                return rs.next();
            }
        } catch (SQLException e) {
            throw new CatalogException("检查 SQL Server 数据库失败，database=" + database, e);
        }
    }

    @Override
    public List<String> listSchemas(String database) throws CatalogException {
        checkOpened();
        String targetDatabase = resolveDatabase(database);
        try (Connection connection = connection(targetDatabase);
             PreparedStatement statement = connection.prepareStatement(LIST_SCHEMAS_SQL);
             ResultSet rs = statement.executeQuery()) {
            List<String> schemas = new ArrayList<String>();
            while (rs.next()) {
                schemas.add(rs.getString(1));
            }
            return schemas;
        } catch (SQLException e) {
            throw new CatalogException(
                    "获取 SQL Server Schema 列表失败，database=" + targetDatabase, e);
        }
    }

    @Override
    public List<TablePath> listTables(String database, String schemaName)
            throws CatalogException {
        checkOpened();
        String targetDatabase = resolveDatabase(database);
        String schema = resolveSchema(schemaName);
        try (Connection connection = connection(targetDatabase);
             ResultSet rs = connection.getMetaData().getTables(
                     targetDatabase, schema, "%", new String[]{"TABLE"})) {
            List<TablePath> tables = new ArrayList<TablePath>();
            while (rs.next()) {
                String table = normalize(rs.getString("TABLE_NAME"));
                if (table != null) {
                    tables.add(TablePath.of(targetDatabase, schema, table));
                }
            }
            tables.sort(Comparator.comparing(TablePath::getTableName));
            return tables;
        } catch (SQLException e) {
            throw new CatalogException(
                    "获取 SQL Server 表列表失败，database=" + targetDatabase
                            + ", schema=" + schema, e);
        }
    }

    @Override
    public boolean tableExists(TablePath tablePath) throws CatalogException {
        checkOpened();
        TablePath path = normalizePath(tablePath);
        try (Connection connection = connection(path.getDatabaseName());
             ResultSet rs = connection.getMetaData().getTables(
                     path.getDatabaseName(), path.getSchemaName(), path.getTableName(),
                     new String[]{"TABLE"})) {
            return rs.next();
        } catch (SQLException e) {
            throw new CatalogException("检查 SQL Server 表失败，table=" + path, e);
        }
    }

    @Override
    public CatalogTable getTable(TablePath tablePath)
            throws CatalogException, TableNotFoundException {
        checkOpened();
        TablePath path = normalizePath(tablePath);
        try (Connection connection = connection(path.getDatabaseName())) {
            List<Column> columns = columns(connection, path);
            if (columns.isEmpty()) {
                throw new TableNotFoundException(catalogName, path);
            }
            DatabaseMetaData meta = connection.getMetaData();
            TableSchema schema = TableSchema.builder()
                    .columns(columns)
                    .primaryKey(primaryKey(meta, path))
                    .build();
            CatalogTable.Builder table = CatalogTable.builder(path, schema)
                    .option(TABLE_OPTION_DIALECT, DIALECT);
            String comment = tableComment(connection, path);
            if (hasText(comment)) {
                table.comment(comment);
            }
            return table.build();
        } catch (TableNotFoundException e) {
            throw e;
        } catch (SQLException e) {
            throw new CatalogException("获取 SQL Server 表结构失败，table=" + path, e);
        }
    }

    @Override
    public void createDatabase(String database, boolean ignoreIfExists)
            throws CatalogException, DatabaseAlreadyExistsException {
        checkOpened();
        if (databaseExists(database)) {
            if (ignoreIfExists) {
                return;
            }
            throw new DatabaseAlreadyExistsException(catalogName, database);
        }
        try (Connection connection = masterConnection()) {
            execute(connection, "CREATE DATABASE " + quote(database));
        } catch (SQLException e) {
            throw new CatalogException("创建 SQL Server 数据库失败，database=" + database, e);
        }
    }

    @Override
    public void dropDatabase(String database, boolean ignoreIfNotExists)
            throws CatalogException, DatabaseNotFoundException {
        checkOpened();
        if (!databaseExists(database)) {
            if (ignoreIfNotExists) {
                return;
            }
            throw new DatabaseNotFoundException(catalogName, database);
        }
        try (Connection connection = masterConnection()) {
            execute(connection, "DROP DATABASE " + quote(database));
        } catch (SQLException e) {
            throw new CatalogException("删除 SQL Server 数据库失败，database=" + database, e);
        }
    }

    @Override
    public void createTable(CatalogTable table, boolean ignoreIfExists)
            throws CatalogException, DatabaseNotFoundException, TableAlreadyExistsException {
        checkOpened();
        TablePath path = normalizePath(table.getTablePath());
        if (!databaseExists(path.getDatabaseName())) {
            throw new DatabaseNotFoundException(catalogName, path.getDatabaseName());
        }
        if (tableExists(path)) {
            if (ignoreIfExists) {
                return;
            }
            throw new TableAlreadyExistsException(catalogName, path);
        }
        CatalogTable ddlTable = table.getTablePath().equals(path)
                ? table : table.withPath(path);
        SqlServerCreateTableSqlBuilder builder =
                new SqlServerCreateTableSqlBuilder(path, ddlTable, typeMapper);
        try (Connection connection = connection(path.getDatabaseName())) {
            for (String sql : builder.buildStatements()) {
                execute(connection, sql);
            }
        } catch (SQLException e) {
            throw new CatalogException("创建 SQL Server 表失败，table=" + path, e);
        }
    }

    @Override
    public void addColumn(TablePath tablePath, Column column)
            throws CatalogException, TableNotFoundException {
        checkOpened();
        TablePath path = normalizePath(tablePath);
        if (!tableExists(path)) {
            throw new TableNotFoundException(catalogName, path);
        }
        CatalogTable table = getTable(path);
        SqlServerCreateTableSqlBuilder builder =
                new SqlServerCreateTableSqlBuilder(path, table, typeMapper);
        String definition = builder.buildColumnDefinition(column, false);
        try (Connection connection = connection(path.getDatabaseName())) {
            execute(connection, "ALTER TABLE " + quoteTable(path) + " ADD " + definition);
            if (hasText(column.getComment())) {
                execute(connection, builder.buildColumnCommentStatement(column));
            }
        } catch (SQLException e) {
            throw new CatalogException(
                    "增加 SQL Server 字段失败，table=" + path
                            + ", column=" + column.getName(), e);
        }
    }

    @Override
    public void dropTable(TablePath tablePath, boolean ignoreIfNotExists)
            throws CatalogException, TableNotFoundException {
        tableDdl(tablePath, ignoreIfNotExists, "DROP TABLE ", "删除");
    }

    @Override
    public void truncateTable(TablePath tablePath, boolean ignoreIfNotExists)
            throws CatalogException, TableNotFoundException {
        tableDdl(tablePath, ignoreIfNotExists, "TRUNCATE TABLE ", "清空");
    }

    private void tableDdl(
            TablePath tablePath, boolean ignoreIfNotExists, String prefix, String operation)
            throws CatalogException, TableNotFoundException {
        checkOpened();
        TablePath path = normalizePath(tablePath);
        if (!tableExists(path)) {
            if (ignoreIfNotExists) {
                return;
            }
            throw new TableNotFoundException(catalogName, path);
        }
        try (Connection connection = connection(path.getDatabaseName())) {
            execute(connection, prefix + quoteTable(path));
        } catch (SQLException e) {
            throw new CatalogException(operation + " SQL Server 表失败，table=" + path, e);
        }
    }

    private List<Column> columns(Connection connection, TablePath path) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(SELECT_COLUMNS_SQL)) {
            statement.setString(1, path.getSchemaName());
            statement.setString(2, path.getTableName());
            try (ResultSet rs = statement.executeQuery()) {
                List<Column> result = new ArrayList<Column>();
                while (rs.next()) {
                    result.add(typeMapper.toColumn(rs));
                }
                return result;
            }
        }
    }

    private PrimaryKey primaryKey(DatabaseMetaData meta, TablePath path) throws SQLException {
        try (ResultSet rs = meta.getPrimaryKeys(
                path.getDatabaseName(), path.getSchemaName(), path.getTableName())) {
            String name = null;
            List<KeyColumn> keys = new ArrayList<KeyColumn>();
            while (rs.next()) {
                if (name == null) {
                    name = rs.getString("PK_NAME");
                }
                keys.add(new KeyColumn(rs.getShort("KEY_SEQ"), rs.getString("COLUMN_NAME")));
            }
            if (keys.isEmpty()) {
                return null;
            }
            keys.sort(Comparator.comparingInt(KeyColumn::position));
            List<String> columns = new ArrayList<String>(keys.size());
            for (KeyColumn key : keys) {
                columns.add(key.name);
            }
            return PrimaryKey.of(name, columns);
        }
    }

    private String tableComment(Connection connection, TablePath path) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(SELECT_TABLE_COMMENT_SQL)) {
            statement.setString(1, path.getSchemaName());
            statement.setString(2, path.getTableName());
            try (ResultSet rs = statement.executeQuery()) {
                return rs.next() ? normalize(rs.getString("TABLE_COMMENT")) : null;
            }
        }
    }

    private TablePath normalizePath(TablePath tablePath) {
        Objects.requireNonNull(tablePath, "tablePath must not be null");
        String currentDatabase = resolveDatabase(null);
        String sourceDatabase = tablePath.getDatabaseName();
        String schema = tablePath.getSchemaName();
        if (!hasText(sourceDatabase)) {
            sourceDatabase = currentDatabase;
            if (!hasText(schema)) {
                schema = configuredSchema;
            }
        } else if (!currentDatabase.equalsIgnoreCase(sourceDatabase)) {
            sourceDatabase = currentDatabase;
            schema = configuredSchema;
        } else if (!hasText(schema)) {
            schema = configuredSchema;
        }
        return TablePath.of(sourceDatabase, schema, tablePath.getTableName());
    }

    private String resolveDatabase(String requested) {
        if (hasText(requested)) {
            return requested.trim();
        }
        if (hasText(defaultDatabase)) {
            return defaultDatabase.trim();
        }
        try (Connection connection = connection()) {
            String current = normalize(connection.getCatalog());
            if (current != null) {
                defaultDatabase = current;
                return current;
            }
        } catch (SQLException e) {
            throw new CatalogException("无法确定 SQL Server 当前数据库", e);
        }
        throw new IllegalArgumentException("SQL Server JDBC URL/Connection 未提供默认数据库");
    }

    private String resolveSchema(String schemaName) {
        return hasText(schemaName) ? schemaName.trim() : configuredSchema;
    }

    private Connection connection() throws SQLException {
        return DriverManager.getConnection(config.getUrl(), config.toConnectionProperties());
    }

    private Connection connection(String database) throws SQLException {
        return DriverManager.getConnection(
                SqlServerJdbcUrl.withDatabase(config.getUrl(), database),
                config.toConnectionProperties());
    }

    private Connection masterConnection() throws SQLException {
        return connection("master");
    }

    private void loadDriver() {
        if (!hasText(config.getDriverClass())) {
            return;
        }
        try {
            Class.forName(config.getDriverClass());
        } catch (ClassNotFoundException e) {
            throw new CatalogException(
                    "找不到 SQL Server JDBC Driver：" + config.getDriverClass(), e);
        }
    }

    private static void execute(Connection connection, String sql) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.execute();
        }
    }

    private void checkOpened() {
        if (!opened) {
            throw new IllegalStateException("Catalog 尚未打开，请先调用 open()");
        }
    }

    private static String quoteTable(TablePath path) {
        return quote(path.getDatabaseName()) + "." + quote(path.getSchemaName())
                + "." + quote(path.getTableName());
    }

    private static String quote(String value) {
        if (!hasText(value)) {
            throw new IllegalArgumentException("identifier must not be empty");
        }
        return "[" + value.trim().replace("]", "]]" ) + "]";
    }

    private static boolean hasText(String value) {
        return normalize(value) != null;
    }

    private static String normalize(String value) {
        if (value == null) {
            return null;
        }
        String normalized = value.trim();
        return normalized.isEmpty() ? null : normalized;
    }

    private static final class KeyColumn {
        private final int position;
        private final String name;

        private KeyColumn(int position, String name) {
            this.position = position;
            this.name = name;
        }

        private int position() {
            return position;
        }
    }
}
