package com.link.up.connector.jdbc.catalog.db2;

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
import com.link.up.connector.jdbc.core.dialect.db2.Db2JdbcUrl;
import com.link.up.connector.jdbc.core.dialect.db2.Db2TypeMapper;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Optional;

/** DB2 LUW offline JDBC Catalog. */
public final class Db2Catalog implements WritableCatalog {

    public static final String DIALECT = "db2";
    public static final String TABLE_OPTION_DIALECT = "dialect";

    private static final String LIST_SCHEMAS_SQL =
            "SELECT SCHEMANAME FROM SYSCAT.SCHEMATA "
                    + "WHERE SCHEMANAME NOT LIKE 'SYS%' ORDER BY SCHEMANAME";
    private static final String LIST_TABLES_SQL =
            "SELECT TABNAME FROM SYSCAT.TABLES "
                    + "WHERE TYPE = 'T' AND TABSCHEMA = ? ORDER BY TABNAME";
    private static final String TABLE_EXISTS_SQL =
            "SELECT 1 FROM SYSCAT.TABLES "
                    + "WHERE TYPE = 'T' AND TABSCHEMA = ? AND TABNAME = ?";
    private static final String TABLE_COMMENT_SQL =
            "SELECT REMARKS FROM SYSCAT.TABLES "
                    + "WHERE TYPE = 'T' AND TABSCHEMA = ? AND TABNAME = ?";

    private final String catalogName;
    private final JdbcCatalogConfig config;
    private final String databaseName;
    private final String defaultSchema;
    private final Db2TypeMapper typeMapper = new Db2TypeMapper();
    private volatile boolean opened;

    public Db2Catalog(
            String catalogName,
            JdbcCatalogConfig config,
            String configuredSchema) {
        if (!hasText(catalogName)) {
            throw new IllegalArgumentException("catalogName must not be empty");
        }
        this.catalogName = catalogName.trim();
        this.config = Objects.requireNonNull(config, "config must not be null");
        this.databaseName = Db2JdbcUrl.databaseName(config.getUrl());
        if (!hasText(databaseName)) {
            throw new IllegalArgumentException(
                    "DB2 JDBC URL 必须包含数据库名，例如 jdbc:db2://127.0.0.1:50000/SAMPLE");
        }
        this.defaultSchema = defaultSchema(configuredSchema, config.getUsername());
    }

    @Override
    public String name() {
        return catalogName;
    }

    @Override
    public Optional<String> getDefaultDatabase() {
        return Optional.of(databaseName);
    }

    @Override
    public synchronized void open() throws CatalogException {
        if (opened) {
            return;
        }
        loadDriver();
        try (Connection connection = connection()) {
            if (!connection.isValid(5)) {
                throw new CatalogException("DB2 Catalog 连接校验失败：" + config.getUrl());
            }
            opened = true;
        } catch (SQLException e) {
            throw new CatalogException("DB2 Catalog 连接失败：" + config.getUrl(), e);
        }
    }

    @Override
    public synchronized void close() {
        opened = false;
    }

    @Override
    public List<String> listDatabases() {
        checkOpened();
        return Collections.singletonList(databaseName);
    }

    @Override
    public List<String> listSchemas(String database) throws CatalogException {
        checkDatabase(database);
        checkOpened();
        try (Connection connection = connection();
             PreparedStatement statement = connection.prepareStatement(LIST_SCHEMAS_SQL);
             ResultSet rs = statement.executeQuery()) {
            List<String> schemas = new ArrayList<String>();
            while (rs.next()) {
                String schema = normalize(rs.getString(1));
                if (schema != null) {
                    schemas.add(schema);
                }
            }
            return schemas;
        } catch (SQLException e) {
            throw new CatalogException("获取 DB2 Schema 列表失败", e);
        }
    }

    @Override
    public List<TablePath> listTables(String database, String schemaName)
            throws CatalogException {
        checkDatabase(database);
        checkOpened();
        String schema = schema(schemaName);
        try (Connection connection = connection();
             PreparedStatement statement = connection.prepareStatement(LIST_TABLES_SQL)) {
            statement.setString(1, schema);
            try (ResultSet rs = statement.executeQuery()) {
                List<TablePath> tables = new ArrayList<TablePath>();
                while (rs.next()) {
                    String table = normalize(rs.getString(1));
                    if (table != null) {
                        tables.add(TablePath.of(databaseName, schema, table));
                    }
                }
                return tables;
            }
        } catch (SQLException e) {
            throw new CatalogException("获取 DB2 表列表失败，schema=" + schema, e);
        }
    }

    @Override
    public boolean tableExists(TablePath tablePath) throws CatalogException {
        checkOpened();
        TablePath path = normalizePath(tablePath);
        try (Connection connection = connection();
             PreparedStatement statement = connection.prepareStatement(TABLE_EXISTS_SQL)) {
            statement.setString(1, path.getSchemaName());
            statement.setString(2, path.getTableName());
            try (ResultSet rs = statement.executeQuery()) {
                return rs.next();
            }
        } catch (SQLException e) {
            throw new CatalogException("检查 DB2 表是否存在失败，table=" + path, e);
        }
    }

    @Override
    public CatalogTable getTable(TablePath tablePath)
            throws CatalogException, TableNotFoundException {
        checkOpened();
        TablePath path = normalizePath(tablePath);
        try (Connection connection = connection()) {
            DatabaseMetaData meta = connection.getMetaData();
            List<Column> columns = columns(meta, path);
            if (columns.isEmpty()) {
                throw new TableNotFoundException(catalogName, path);
            }
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
            throw new CatalogException("获取 DB2 表结构失败，table=" + path, e);
        }
    }

    @Override
    public void createDatabase(String database, boolean ignoreIfExists)
            throws CatalogException, DatabaseAlreadyExistsException {
        throw new UnsupportedOperationException(
                "DB2 JDBC Offline Catalog 不负责 CREATE DATABASE");
    }

    @Override
    public void dropDatabase(String database, boolean ignoreIfNotExists)
            throws CatalogException, DatabaseNotFoundException {
        throw new UnsupportedOperationException(
                "DB2 JDBC Offline Catalog 不负责 DROP DATABASE");
    }

    @Override
    public void createTable(CatalogTable table, boolean ignoreIfExists)
            throws CatalogException, DatabaseNotFoundException, TableAlreadyExistsException {
        checkOpened();
        TablePath path = normalizePath(table.getTablePath());
        if (tableExists(path)) {
            if (ignoreIfExists) {
                return;
            }
            throw new TableAlreadyExistsException(catalogName, path);
        }

        CatalogTable ddlTable = table.getTablePath().equals(path)
                ? table
                : table.withPath(path);
        Db2CreateTableSqlBuilder builder =
                new Db2CreateTableSqlBuilder(path, ddlTable, typeMapper);
        try (Connection connection = connection()) {
            for (String sql : builder.buildStatements()) {
                execute(connection, sql);
            }
        } catch (SQLException e) {
            throw new CatalogException("创建 DB2 表失败，table=" + path, e);
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
        CatalogTable current = getTable(path);
        String definition = new Db2CreateTableSqlBuilder(path, current, typeMapper)
                .buildColumnDefinition(column, false);
        try (Connection connection = connection()) {
            execute(connection,
                    "ALTER TABLE " + quoteTable(path) + " ADD COLUMN " + definition);
            if (hasText(column.getComment())) {
                execute(connection,
                        "COMMENT ON COLUMN " + quoteTable(path) + "."
                                + quote(column.getName()) + " IS '"
                                + literal(column.getComment()) + "'");
            }
        } catch (SQLException e) {
            throw new CatalogException(
                    "增加 DB2 字段失败，table=" + path + ", column=" + column.getName(), e);
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
            TablePath tablePath,
            boolean ignoreIfNotExists,
            String prefix,
            String operation) throws CatalogException, TableNotFoundException {
        checkOpened();
        TablePath path = normalizePath(tablePath);
        if (!tableExists(path)) {
            if (ignoreIfNotExists) {
                return;
            }
            throw new TableNotFoundException(catalogName, path);
        }
        String suffix = prefix.startsWith("TRUNCATE") ? " IMMEDIATE" : "";
        try (Connection connection = connection()) {
            execute(connection, prefix + quoteTable(path) + suffix);
        } catch (SQLException e) {
            throw new CatalogException(operation + " DB2 表失败，table=" + path, e);
        }
    }

    private List<Column> columns(DatabaseMetaData meta, TablePath path) throws SQLException {
        try (ResultSet rs = meta.getColumns(
                null,
                path.getSchemaName(),
                path.getTableName(),
                "%")) {
            List<Column> columns = new ArrayList<Column>();
            while (rs.next()) {
                columns.add(typeMapper.toColumn(rs));
            }
            return columns;
        }
    }

    private PrimaryKey primaryKey(DatabaseMetaData meta, TablePath path) throws SQLException {
        try (ResultSet rs = meta.getPrimaryKeys(
                null,
                path.getSchemaName(),
                path.getTableName())) {
            String name = null;
            List<KeyColumn> keys = new ArrayList<KeyColumn>();
            while (rs.next()) {
                if (name == null) {
                    name = rs.getString("PK_NAME");
                }
                keys.add(new KeyColumn(
                        rs.getShort("KEY_SEQ"),
                        rs.getString("COLUMN_NAME")));
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
        try (PreparedStatement statement = connection.prepareStatement(TABLE_COMMENT_SQL)) {
            statement.setString(1, path.getSchemaName());
            statement.setString(2, path.getTableName());
            try (ResultSet rs = statement.executeQuery()) {
                return rs.next() ? normalize(rs.getString(1)) : null;
            }
        }
    }

    private TablePath normalizePath(TablePath tablePath) {
        Objects.requireNonNull(tablePath, "tablePath must not be null");
        String sourceDatabase = tablePath.getDatabaseName();
        String targetSchema = tablePath.getSchemaName();
        if (!hasText(targetSchema)
                || (hasText(sourceDatabase)
                && !databaseName.equalsIgnoreCase(sourceDatabase))) {
            targetSchema = defaultSchema;
        }
        return TablePath.of(databaseName, targetSchema, tablePath.getTableName());
    }

    private String schema(String schemaName) {
        return hasText(schemaName) ? schemaName.trim() : defaultSchema;
    }

    private void checkDatabase(String requested) {
        if (hasText(requested) && !databaseName.equalsIgnoreCase(requested.trim())) {
            throw new IllegalArgumentException(
                    "DB2 JDBC 连接只支持当前 database：" + databaseName
                            + "，requested=" + requested);
        }
    }

    private Connection connection() throws SQLException {
        return DriverManager.getConnection(
                config.getUrl(),
                config.toConnectionProperties());
    }

    private void loadDriver() {
        if (!hasText(config.getDriverClass())) {
            return;
        }
        try {
            Class.forName(config.getDriverClass());
        } catch (ClassNotFoundException e) {
            throw new CatalogException(
                    "找不到 DB2 JDBC Driver：" + config.getDriverClass(), e);
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

    private static String defaultSchema(String configured, String username) {
        if (hasText(configured)) {
            return configured.trim();
        }
        if (hasText(username)) {
            return username.trim().toUpperCase(Locale.ROOT);
        }
        throw new IllegalArgumentException("DB2 需要配置 schema 或 username");
    }

    private static String quoteTable(TablePath path) {
        return quote(path.getSchemaName()) + "." + quote(path.getTableName());
    }

    private static String quote(String value) {
        if (!hasText(value)) {
            throw new IllegalArgumentException("identifier must not be empty");
        }
        return "\"" + value.trim().replace("\"", "\"\"") + "\"";
    }

    private static String literal(String value) {
        return value.replace("'", "''");
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
