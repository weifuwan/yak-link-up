package com.link.up.connector.jdbc.catalog.duckdb;

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
import com.link.up.connector.jdbc.core.dialect.duckdb.DuckDbJdbcUrl;
import com.link.up.connector.jdbc.core.dialect.duckdb.DuckDbTypeMapper;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;

/** DuckDB offline JDBC Catalog. */
public final class DuckDbCatalog implements WritableCatalog {

    public static final String DIALECT = "duckdb";
    public static final String TABLE_OPTION_DIALECT = "dialect";

    private static final String CURRENT_DATABASE_SQL = "SELECT current_database()";
    private static final String CURRENT_SCHEMA_SQL = "SELECT current_schema()";
    private static final Set<String> EXCLUDED_SCHEMAS =
            Collections.unmodifiableSet(new HashSet<String>(Arrays.asList(
                    "information_schema", "pg_catalog", "temp")));

    private final String catalogName;
    private final JdbcCatalogConfig config;
    private final String configuredSchema;
    private final DuckDbTypeMapper typeMapper = new DuckDbTypeMapper();

    private volatile String databaseName;
    private volatile String defaultSchema;
    private volatile boolean opened;

    public DuckDbCatalog(
            String catalogName,
            JdbcCatalogConfig config,
            String configuredSchema) {

        if (!hasText(catalogName)) {
            throw new IllegalArgumentException("catalogName must not be empty");
        }
        this.catalogName = catalogName.trim();
        this.config = Objects.requireNonNull(config, "config must not be null");

        if (DuckDbJdbcUrl.isConnectionPrivateMemory(config.getUrl())) {
            throw new IllegalArgumentException(
                    "DuckDB 匿名内存库无法跨 JDBC 连接共享，请使用命名内存库或文件库");
        }
        if (DuckDbJdbcUrl.isDuckLake(config.getUrl())) {
            throw new IllegalArgumentException("DuckLake 暂不属于 DuckDB Offline JDBC 首阶段范围");
        }
        if (DuckDbJdbcUrl.isInstanceCacheDisabled(
                config.getUrl(), config.getProperties())) {
            throw new IllegalArgumentException(
                    "DuckDB Offline JDBC 需要 jdbc_instance_cache=true");
        }

        this.databaseName = DuckDbJdbcUrl.databaseName(config.getUrl());
        if (!hasText(databaseName)) {
            throw new IllegalArgumentException("无法从 DuckDB JDBC URL 解析 database/catalog 名称");
        }

        String schema = normalize(configuredSchema);
        if (schema == null) {
            schema = normalize(DuckDbJdbcUrl.configuredSchema(
                    config.getUrl(), config.getProperties()));
        }
        this.configuredSchema = schema;
        this.defaultSchema = schema == null ? "main" : schema;
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
                throw new CatalogException(
                        "DuckDB Catalog 连接校验失败：" + config.getUrl());
            }
            String actualDatabase = scalar(connection, CURRENT_DATABASE_SQL);
            if (hasText(actualDatabase)) {
                databaseName = actualDatabase;
            }
            if (!hasText(configuredSchema)) {
                String actualSchema = scalar(connection, CURRENT_SCHEMA_SQL);
                if (hasText(actualSchema)) {
                    defaultSchema = actualSchema;
                }
            }
            opened = true;
        } catch (SQLException e) {
            throw new CatalogException(
                    "DuckDB Catalog 连接失败：" + config.getUrl(), e);
        }
    }

    @Override
    public synchronized void close() {
        opened = false;
    }

    @Override
    public List<String> listDatabases() {
        checkOpened();
        // ATTACH state is connection-local/session initialization territory;
        // this connector is scoped to the catalog opened by its JDBC URL.
        return Collections.singletonList(databaseName);
    }

    @Override
    public List<String> listSchemas(String database) throws CatalogException {
        checkDatabase(database);
        checkOpened();
        try (Connection connection = connection();
             ResultSet rs = connection.getMetaData().getSchemas()) {
            List<String> schemas = new ArrayList<String>();
            while (rs.next()) {
                String schema = normalize(rs.getString("TABLE_SCHEM"));
                if (schema != null && !isSystemSchema(schema)) {
                    schemas.add(schema);
                }
            }
            Collections.sort(schemas);
            return schemas;
        } catch (SQLException e) {
            throw new CatalogException("获取 DuckDB Schema 列表失败", e);
        }
    }

    @Override
    public List<TablePath> listTables(
            String database,
            String schemaName) throws CatalogException {

        checkDatabase(database);
        checkOpened();
        String schema = schema(schemaName);
        try (Connection connection = connection();
             ResultSet rs = connection.getMetaData().getTables(
                     null, schema, "%", new String[]{"TABLE"})) {
            List<TablePath> tables = new ArrayList<TablePath>();
            while (rs.next()) {
                String table = normalize(rs.getString("TABLE_NAME"));
                if (table != null) {
                    tables.add(TablePath.of(databaseName, schema, table));
                }
            }
            tables.sort(Comparator.comparing(TablePath::getTableName));
            return tables;
        } catch (SQLException e) {
            throw new CatalogException(
                    "获取 DuckDB 表列表失败，schema=" + schema, e);
        }
    }

    @Override
    public boolean tableExists(TablePath tablePath) throws CatalogException {
        checkOpened();
        TablePath path = normalizePath(tablePath);
        try (Connection connection = connection();
             ResultSet rs = connection.getMetaData().getTables(
                     null,
                     path.getSchemaName(),
                     path.getTableName(),
                     new String[]{"TABLE"})) {
            return rs.next();
        } catch (SQLException e) {
            throw new CatalogException(
                    "检查 DuckDB 表是否存在失败，table=" + path, e);
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

            String comment = tableComment(meta, path);
            if (hasText(comment)) {
                table.comment(comment);
            }
            return table.build();
        } catch (TableNotFoundException e) {
            throw e;
        } catch (SQLException e) {
            throw new CatalogException(
                    "获取 DuckDB 表结构失败，table=" + path, e);
        }
    }

    @Override
    public void createDatabase(String database, boolean ignoreIfExists)
            throws CatalogException, DatabaseAlreadyExistsException {
        throw new UnsupportedOperationException(
                "DuckDB Offline JDBC 不通过 Catalog CREATE/ATTACH database");
    }

    @Override
    public void dropDatabase(String database, boolean ignoreIfNotExists)
            throws CatalogException, DatabaseNotFoundException {
        throw new UnsupportedOperationException(
                "DuckDB Offline JDBC 不通过 Catalog DROP/DETACH database");
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
                ? table : table.withPath(path);
        DuckDbCreateTableSqlBuilder builder =
                new DuckDbCreateTableSqlBuilder(path, ddlTable, typeMapper);

        try (Connection connection = connection()) {
            for (String sql : builder.buildStatements()) {
                execute(connection, sql);
            }
        } catch (SQLException e) {
            throw new CatalogException(
                    "创建 DuckDB 表失败，table=" + path, e);
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
        DuckDbCreateTableSqlBuilder builder =
                new DuckDbCreateTableSqlBuilder(path, current, typeMapper);
        String definition = builder.buildColumnDefinition(column, false);

        try (Connection connection = connection()) {
            if (column.isAutoIncrement()) {
                execute(connection, builder.buildCreateSequence(column));
            }
            execute(connection,
                    "ALTER TABLE " + quoteTable(path)
                            + " ADD COLUMN " + definition);
            if (hasText(column.getComment())) {
                execute(connection,
                        "COMMENT ON COLUMN " + quoteTable(path) + "."
                                + quote(column.getName()) + " IS '"
                                + literal(column.getComment()) + "'");
            }
        } catch (SQLException e) {
            throw new CatalogException(
                    "增加 DuckDB 字段失败，table=" + path
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
        try (Connection connection = connection()) {
            execute(connection, prefix + quoteTable(path));
        } catch (SQLException e) {
            throw new CatalogException(
                    operation + " DuckDB 表失败，table=" + path, e);
        }
    }

    private List<Column> columns(DatabaseMetaData meta, TablePath path)
            throws SQLException {
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

    private PrimaryKey primaryKey(DatabaseMetaData meta, TablePath path)
            throws SQLException {
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

    private String tableComment(DatabaseMetaData meta, TablePath path)
            throws SQLException {
        try (ResultSet rs = meta.getTables(
                null,
                path.getSchemaName(),
                path.getTableName(),
                new String[]{"TABLE"})) {
            return rs.next() ? normalize(rs.getString("REMARKS")) : null;
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
        if (hasText(requested)
                && !databaseName.equalsIgnoreCase(requested.trim())) {
            throw new IllegalArgumentException(
                    "DuckDB JDBC 连接只支持当前 catalog/database：" + databaseName
                            + "，requested=" + requested);
        }
    }

    private Connection connection() throws SQLException {
        Properties properties = config.toConnectionProperties();
        if (hasText(configuredSchema)
                && !containsPropertyIgnoreCase(properties, "schema")) {
            properties.setProperty("schema", configuredSchema);
        }
        return DriverManager.getConnection(config.getUrl(), properties);
    }

    private void loadDriver() {
        if (!hasText(config.getDriverClass())) {
            return;
        }
        try {
            Class.forName(config.getDriverClass());
        } catch (ClassNotFoundException e) {
            throw new CatalogException(
                    "找不到 DuckDB JDBC Driver：" + config.getDriverClass(), e);
        }
    }

    private static String scalar(Connection connection, String sql) {
        try (PreparedStatement statement = connection.prepareStatement(sql);
             ResultSet rs = statement.executeQuery()) {
            return rs.next() ? normalize(rs.getString(1)) : null;
        } catch (SQLException ignored) {
            return null;
        }
    }

    private static void execute(Connection connection, String sql)
            throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.execute();
        }
    }

    private void checkOpened() {
        if (!opened) {
            throw new IllegalStateException("Catalog 尚未打开，请先调用 open()");
        }
    }

    private static boolean isSystemSchema(String schema) {
        return EXCLUDED_SCHEMAS.contains(schema.toLowerCase(Locale.ROOT));
    }

    private static boolean containsPropertyIgnoreCase(
            Properties properties,
            String key) {
        for (Object propertyKey : properties.keySet()) {
            if (propertyKey != null
                    && key.equalsIgnoreCase(String.valueOf(propertyKey).trim())) {
                return true;
            }
        }
        return false;
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
