package com.link.up.connector.jdbc.catalog.oracle;

import com.link.up.api.table.catalog.*;
import com.link.up.api.table.catalog.exception.*;
import com.link.up.connector.jdbc.catalog.JdbcCatalogConfig;
import com.link.up.connector.jdbc.core.dialect.oracle.OracleJdbcUrl;
import com.link.up.connector.jdbc.core.dialect.oracle.OracleTypeMapper;

import java.sql.*;
import java.util.*;

/**
 * Oracle offline JDBC Catalog.
 *
 * <p>Uses JDBC metadata for bounded Source discovery and table-level DDL for
 * Sink preparation. Database/service creation is intentionally out of scope.
 */
public final class OracleCatalog implements WritableCatalog {

    public static final String DIALECT = "oracle";
    public static final String TABLE_OPTION_DIALECT = "dialect";

    private static final String SELECT_COLUMNS_SQL =
            "SELECT c.COLUMN_NAME, c.DATA_TYPE, c.DATA_LENGTH, c.CHAR_LENGTH, "
                    + "c.DATA_PRECISION, c.DATA_SCALE, c.NULLABLE, c.DATA_DEFAULT, "
                    + "cc.COMMENTS AS COLUMN_COMMENT "
                    + "FROM ALL_TAB_COLUMNS c LEFT JOIN ALL_COL_COMMENTS cc "
                    + "ON cc.OWNER=c.OWNER AND cc.TABLE_NAME=c.TABLE_NAME "
                    + "AND cc.COLUMN_NAME=c.COLUMN_NAME "
                    + "WHERE c.OWNER=? AND c.TABLE_NAME=? ORDER BY c.COLUMN_ID";

    private final String catalogName;
    private final JdbcCatalogConfig config;
    private final String databaseName;
    private final String defaultSchema;
    private final OracleTypeMapper typeMapper = new OracleTypeMapper();
    private volatile boolean opened;

    public OracleCatalog(String catalogName, JdbcCatalogConfig config, String defaultSchema) {
        if (!hasText(catalogName)) {
            throw new IllegalArgumentException("catalogName must not be empty");
        }
        this.catalogName = catalogName.trim();
        this.config = Objects.requireNonNull(config, "config must not be null");
        this.databaseName = OracleJdbcUrl.databaseName(config.getUrl());
        this.defaultSchema = defaultSchema(defaultSchema, config.getUsername());
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
                throw new CatalogException("Oracle Catalog 连接校验失败：" + config.getUrl());
            }
            opened = true;
        } catch (SQLException e) {
            throw new CatalogException("Oracle Catalog 连接失败：" + config.getUrl(), e);
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
            throw new CatalogException("获取 Oracle Schema 列表失败", e);
        }
    }

    @Override
    public List<TablePath> listTables(String database, String schemaName)
            throws CatalogException {
        checkDatabase(database);
        checkOpened();
        String schema = schema(schemaName);
        try (Connection connection = connection();
             ResultSet rs = connection.getMetaData()
                     .getTables(null, schema, "%", new String[]{"TABLE"})) {
            List<TablePath> tables = new ArrayList<TablePath>();
            while (rs.next()) {
                String table = normalize(rs.getString("TABLE_NAME"));
                if (table != null && !table.startsWith("BIN$")) {
                    tables.add(TablePath.of(databaseName, schema, table));
                }
            }
            tables.sort(Comparator.comparing(TablePath::getTableName));
            return tables;
        } catch (SQLException e) {
            throw new CatalogException("获取 Oracle 表列表失败，schema=" + schema, e);
        }
    }

    @Override
    public boolean tableExists(TablePath tablePath) throws CatalogException {
        checkOpened();
        TablePath path = normalizePath(tablePath);
        try (Connection connection = connection();
             ResultSet rs = connection.getMetaData().getTables(
                     null, path.getSchemaName(), path.getTableName(),
                     new String[]{"TABLE"})) {
            return rs.next();
        } catch (SQLException e) {
            throw new CatalogException("检查 Oracle 表是否存在失败，table=" + path, e);
        }
    }

    @Override
    public CatalogTable getTable(TablePath tablePath)
            throws CatalogException, TableNotFoundException {
        checkOpened();
        TablePath path = normalizePath(tablePath);
        try (Connection connection = connection()) {
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
            String comment = tableComment(meta, path);
            if (hasText(comment)) {
                table.comment(comment);
            }
            return table.build();
        } catch (TableNotFoundException e) {
            throw e;
        } catch (SQLException e) {
            throw new CatalogException("获取 Oracle 表结构失败，table=" + path, e);
        }
    }

    @Override
    public void createDatabase(String database, boolean ignoreIfExists)
            throws CatalogException, DatabaseAlreadyExistsException {
        throw new UnsupportedOperationException(
                "Oracle JDBC Offline Catalog 不负责 CREATE DATABASE");
    }

    @Override
    public void dropDatabase(String database, boolean ignoreIfNotExists)
            throws CatalogException, DatabaseNotFoundException {
        throw new UnsupportedOperationException(
                "Oracle JDBC Offline Catalog 不负责 DROP DATABASE");
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
        OracleCreateTableSqlBuilder builder =
                new OracleCreateTableSqlBuilder(path, ddlTable, typeMapper);
        try (Connection connection = connection()) {
            for (String sql : builder.buildStatements()) {
                execute(connection, sql);
            }
        } catch (SQLException e) {
            throw new CatalogException("创建 Oracle 表失败，table=" + path, e);
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
        String definition = new OracleCreateTableSqlBuilder(path, table, typeMapper)
                .buildColumnDefinition(column, false);
        try (Connection connection = connection()) {
            execute(connection, "ALTER TABLE " + quoteTable(path) + " ADD " + definition);
            if (hasText(column.getComment())) {
                execute(connection, "COMMENT ON COLUMN " + quoteTable(path) + "."
                        + quote(column.getName()) + " IS '"
                        + literal(column.getComment()) + "'");
            }
        } catch (SQLException e) {
            throw new CatalogException(
                    "增加 Oracle 字段失败，table=" + path + ", column=" + column.getName(), e);
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

    private void tableDdl(TablePath tablePath, boolean ignoreIfNotExists,
                          String prefix, String operation)
            throws CatalogException, TableNotFoundException {
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
            throw new CatalogException(operation + " Oracle 表失败，table=" + path, e);
        }
    }

    private List<Column> columns(Connection connection, TablePath path) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(SELECT_COLUMNS_SQL)) {
            statement.setString(1, path.getSchemaName());
            statement.setString(2, path.getTableName());
            try (ResultSet rs = statement.executeQuery()) {
                List<Column> columns = new ArrayList<Column>();
                while (rs.next()) {
                    columns.add(typeMapper.toColumn(rs));
                }
                return columns;
            }
        }
    }

    private PrimaryKey primaryKey(DatabaseMetaData meta, TablePath path) throws SQLException {
        try (ResultSet rs = meta.getPrimaryKeys(
                null, path.getSchemaName(), path.getTableName())) {
            String name = null;
            List<KeyColumn> keys = new ArrayList<KeyColumn>();
            while (rs.next()) {
                if (name == null) {
                    name = rs.getString("PK_NAME");
                }
                keys.add(new KeyColumn(
                        rs.getShort("KEY_SEQ"), rs.getString("COLUMN_NAME")));
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

    private String tableComment(DatabaseMetaData meta, TablePath path) throws SQLException {
        try (ResultSet rs = meta.getTables(
                null, path.getSchemaName(), path.getTableName(),
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
        if (hasText(requested) && !databaseName.equalsIgnoreCase(requested.trim())) {
            throw new IllegalArgumentException(
                    "Oracle JDBC 连接只支持当前 service/database：" + databaseName
                            + "，requested=" + requested);
        }
    }

    private Connection connection() throws SQLException {
        return DriverManager.getConnection(
                config.getUrl(), config.toConnectionProperties());
    }

    private void loadDriver() {
        if (!hasText(config.getDriverClass())) {
            return;
        }
        try {
            Class.forName(config.getDriverClass());
        } catch (ClassNotFoundException e) {
            throw new CatalogException(
                    "找不到 Oracle JDBC Driver：" + config.getDriverClass(), e);
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
        throw new IllegalArgumentException("Oracle 需要配置 schema 或 username");
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
