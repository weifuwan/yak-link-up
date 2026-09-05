package com.link.up.connector.jdbc.catalog.yashandb;

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
import com.link.up.connector.jdbc.core.dialect.yashandb.YashanDbJdbcUrl;
import com.link.up.connector.jdbc.core.dialect.yashandb.YashanDbTypeMapper;

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

/** YashanDB offline JDBC Catalog. */
public final class YashanDbCatalog implements WritableCatalog {

    public static final String DIALECT = "yashandb";
    public static final String TABLE_OPTION_DIALECT = "dialect";

    private final String catalogName;
    private final JdbcCatalogConfig config;
    private final String logicalDatabaseName;
    private final String defaultSchema;
    private final YashanDbTypeMapper typeMapper = new YashanDbTypeMapper();

    private volatile boolean opened;

    public YashanDbCatalog(
            String catalogName,
            JdbcCatalogConfig config,
            String defaultSchema) {

        if (!hasText(catalogName)) {
            throw new IllegalArgumentException("catalogName must not be empty");
        }
        this.catalogName = catalogName.trim();
        this.config = Objects.requireNonNull(config, "config must not be null");
        this.logicalDatabaseName = YashanDbJdbcUrl.databaseName(config.getUrl());
        if (!hasText(logicalDatabaseName)) {
            throw new IllegalArgumentException(
                    "YashanDB JDBC URL 必须包含 database_name，例如 "
                            + "jdbc:yasdb://127.0.0.1:1688/YASDB");
        }
        this.defaultSchema = resolveDefaultSchema(defaultSchema, config.getUsername());
    }

    @Override
    public String name() {
        return catalogName;
    }

    @Override
    public Optional<String> getDefaultDatabase() {
        return Optional.of(logicalDatabaseName);
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
                        "YashanDB Catalog 连接校验失败：" + config.getUrl());
            }
            opened = true;
        } catch (SQLException e) {
            throw new CatalogException(
                    "YashanDB Catalog 连接失败：" + config.getUrl(), e);
        }
    }

    @Override
    public synchronized void close() {
        opened = false;
    }

    @Override
    public List<String> listDatabases() {
        checkOpened();
        // The URL database_name is a required compatibility token rather than
        // a server-side database selector. Keep one stable logical database.
        return Collections.singletonList(logicalDatabaseName);
    }

    @Override
    public List<String> listSchemas(String databaseName) throws CatalogException {
        checkDatabase(databaseName);
        checkOpened();
        try (Connection connection = connection();
             ResultSet rs = connection.getMetaData().getSchemas()) {

            List<String> schemas = new ArrayList<String>();
            while (rs.next()) {
                String schema = normalize(rs.getString("TABLE_SCHEM"));
                if (schema != null) {
                    schemas.add(schema);
                }
            }
            Collections.sort(schemas);
            return schemas;
        } catch (SQLException e) {
            throw new CatalogException("获取 YashanDB Schema 列表失败", e);
        }
    }

    @Override
    public List<TablePath> listTables(
            String databaseName,
            String schemaName) throws CatalogException {

        checkDatabase(databaseName);
        checkOpened();
        String schema = schema(schemaName);

        try (Connection connection = connection();
             ResultSet rs = connection.getMetaData().getTables(
                     null,
                     schema,
                     "%",
                     new String[]{"TABLE"})) {

            List<TablePath> tables = new ArrayList<TablePath>();
            while (rs.next()) {
                String tableName = normalize(rs.getString("TABLE_NAME"));
                if (tableName != null && !isInternalTable(tableName)) {
                    tables.add(TablePath.of(
                            logicalDatabaseName,
                            schema,
                            tableName));
                }
            }
            tables.sort(Comparator.comparing(TablePath::getTableName));
            return tables;
        } catch (SQLException e) {
            throw new CatalogException(
                    "获取 YashanDB 表列表失败，schema=" + schema, e);
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
                    "检查 YashanDB 表是否存在失败，table=" + path, e);
        }
    }

    @Override
    public CatalogTable getTable(TablePath tablePath)
            throws CatalogException, TableNotFoundException {

        checkOpened();
        TablePath path = normalizePath(tablePath);
        try (Connection connection = connection()) {
            DatabaseMetaData metadata = connection.getMetaData();
            List<Column> columns = columns(metadata, path);
            if (columns.isEmpty()) {
                throw new TableNotFoundException(catalogName, path);
            }

            TableSchema schema = TableSchema.builder()
                    .columns(columns)
                    .primaryKey(primaryKey(metadata, path))
                    .build();

            CatalogTable.Builder table = CatalogTable.builder(path, schema)
                    .option(TABLE_OPTION_DIALECT, DIALECT);
            String comment = tableComment(metadata, path);
            if (hasText(comment)) {
                table.comment(comment);
            }
            return table.build();
        } catch (TableNotFoundException e) {
            throw e;
        } catch (SQLException e) {
            throw new CatalogException(
                    "获取 YashanDB 表结构失败，table=" + path, e);
        }
    }

    @Override
    public void createDatabase(String databaseName, boolean ignoreIfExists)
            throws CatalogException, DatabaseAlreadyExistsException {
        throw new UnsupportedOperationException(
                "YashanDB JDBC Offline Catalog 不负责 CREATE DATABASE");
    }

    @Override
    public void dropDatabase(String databaseName, boolean ignoreIfNotExists)
            throws CatalogException, DatabaseNotFoundException {
        throw new UnsupportedOperationException(
                "YashanDB JDBC Offline Catalog 不负责 DROP DATABASE");
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
        YashanDbCreateTableSqlBuilder builder =
                new YashanDbCreateTableSqlBuilder(path, ddlTable, typeMapper);

        try (Connection connection = connection()) {
            for (String sql : builder.buildStatements()) {
                execute(connection, sql);
            }
        } catch (SQLException e) {
            throw new CatalogException(
                    "创建 YashanDB 表失败，table=" + path, e);
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
        String definition = new YashanDbCreateTableSqlBuilder(
                path,
                current,
                typeMapper)
                .buildColumnDefinition(column, false);

        try (Connection connection = connection()) {
            execute(connection,
                    "ALTER TABLE " + quoteTable(path)
                            + " ADD (" + definition + ")");
            if (hasText(column.getComment())) {
                execute(connection,
                        "COMMENT ON COLUMN " + quoteTable(path) + "."
                                + quote(column.getName()) + " IS '"
                                + literal(column.getComment()) + "'");
            }
        } catch (SQLException e) {
            throw new CatalogException(
                    "增加 YashanDB 字段失败，table=" + path
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
                    operation + " YashanDB 表失败，table=" + path, e);
        }
    }

    private List<Column> columns(DatabaseMetaData metadata, TablePath path)
            throws SQLException {
        try (ResultSet rs = metadata.getColumns(
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

    private PrimaryKey primaryKey(DatabaseMetaData metadata, TablePath path)
            throws SQLException {
        try (ResultSet rs = metadata.getPrimaryKeys(
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

    private String tableComment(DatabaseMetaData metadata, TablePath path)
            throws SQLException {
        try (ResultSet rs = metadata.getTables(
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
                && !logicalDatabaseName.equalsIgnoreCase(sourceDatabase))) {
            targetSchema = defaultSchema;
        }
        return TablePath.of(
                logicalDatabaseName,
                targetSchema,
                tablePath.getTableName());
    }

    private String schema(String schemaName) {
        return hasText(schemaName) ? schemaName.trim() : defaultSchema;
    }

    private void checkDatabase(String requested) {
        if (hasText(requested)
                && !logicalDatabaseName.equalsIgnoreCase(requested.trim())) {
            throw new IllegalArgumentException(
                    "YashanDB JDBC 逻辑 database 只支持：" + logicalDatabaseName
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
                    "找不到 YashanDB JDBC Driver：" + config.getDriverClass(), e);
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

    private static String resolveDefaultSchema(String configured, String username) {
        if (hasText(configured)) {
            return configured.trim();
        }
        if (hasText(username)) {
            return username.trim().toUpperCase(Locale.ROOT);
        }
        throw new IllegalArgumentException("YashanDB 需要配置 schema 或 username");
    }

    private static boolean isInternalTable(String tableName) {
        String upper = tableName.toUpperCase(Locale.ROOT);
        return upper.startsWith("OL$")
                || upper.startsWith("WRM$")
                || upper.startsWith("WRH$")
                || upper.startsWith("WRI$")
                || upper.startsWith("YLS$");
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
