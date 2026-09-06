package com.link.up.connector.jdbc.catalog.hana;

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
import com.link.up.connector.jdbc.core.dialect.hana.HanaJdbcUrl;
import com.link.up.connector.jdbc.core.dialect.hana.HanaTypeMapper;

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
import java.util.Properties;

/** SAP HANA offline JDBC Catalog for bounded Source and batch Sink jobs. */
public final class HanaCatalog implements WritableCatalog {

    public static final String DIALECT = "hana";
    public static final String TABLE_OPTION_DIALECT = "dialect";

    private static final String CURRENT_SCHEMA_SQL =
            "SELECT CURRENT_SCHEMA FROM DUMMY";
    private static final String CURRENT_DATABASE_SQL =
            "SELECT DATABASE_NAME FROM SYS.M_DATABASE";
    private static final String[] TABLE_TYPES = new String[]{"TABLE", "VIEW"};

    private final String catalogName;
    private final JdbcCatalogConfig config;
    private final String configuredDefaultSchema;
    private final String configuredDatabaseName;
    private final HanaTypeMapper typeMapper = new HanaTypeMapper();
    private volatile boolean opened;

    public HanaCatalog(
            String catalogName,
            JdbcCatalogConfig config,
            String defaultSchema,
            String databaseName) {
        if (!hasText(catalogName)) {
            throw new IllegalArgumentException("catalogName must not be empty");
        }
        this.catalogName = catalogName.trim();
        this.config = Objects.requireNonNull(config, "config must not be null");
        if (!HanaJdbcUrl.accepts(config.getUrl())) {
            throw new IllegalArgumentException(
                    "非法 SAP HANA JDBC URL：" + config.getUrl());
        }
        this.configuredDefaultSchema = normalize(defaultSchema);
        this.configuredDatabaseName = normalize(databaseName);
    }

    @Override
    public String name() {
        return catalogName;
    }

    @Override
    public Optional<String> getDefaultDatabase() {
        return Optional.ofNullable(configuredDatabaseName);
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
                        "SAP HANA Catalog 连接校验失败：" + config.getUrl());
            }
            opened = true;
        } catch (SQLException e) {
            throw new CatalogException(
                    "SAP HANA Catalog 连接失败：" + config.getUrl(), e);
        }
    }

    @Override
    public synchronized void close() {
        opened = false;
    }

    @Override
    public List<String> listDatabases() throws CatalogException {
        checkOpened();
        try (Connection connection = connection()) {
            String database = resolveDatabaseName(connection, null);
            return database == null
                    ? Collections.<String>emptyList()
                    : Collections.singletonList(database);
        } catch (SQLException e) {
            throw new CatalogException("获取 SAP HANA database 失败", e);
        }
    }

    @Override
    public List<String> listSchemas(String databaseName) throws CatalogException {
        checkOpened();
        try (Connection connection = connection()) {
            resolveDatabaseName(connection, databaseName);
            DatabaseMetaData metadata = connection.getMetaData();
            List<String> schemas = new ArrayList<String>();
            try (ResultSet rs = metadata.getSchemas()) {
                while (rs.next()) {
                    String schema = normalize(rs.getString("TABLE_SCHEM"));
                    if (schema != null && !isSystemSchema(schema)) {
                        schemas.add(schema);
                    }
                }
            }
            Collections.sort(schemas);
            return schemas;
        } catch (SQLException e) {
            throw new CatalogException("获取 SAP HANA Schema 列表失败", e);
        }
    }

    @Override
    public List<TablePath> listTables(
            String databaseName,
            String schemaName) throws CatalogException {
        checkOpened();
        try (Connection connection = connection()) {
            String database = resolveDatabaseName(connection, databaseName);
            String schema = resolveSchema(connection, schemaName);
            DatabaseMetaData metadata = connection.getMetaData();
            List<TablePath> tables = new ArrayList<TablePath>();
            try (ResultSet rs = metadata.getTables(null, schema, "%", TABLE_TYPES)) {
                while (rs.next()) {
                    String table = normalize(rs.getString("TABLE_NAME"));
                    if (table != null) {
                        tables.add(TablePath.of(database, schema, table));
                    }
                }
            }
            tables.sort(Comparator.comparing(TablePath::getTableName));
            return tables;
        } catch (SQLException e) {
            throw new CatalogException(
                    "获取 SAP HANA 表列表失败，schema=" + schemaName, e);
        }
    }

    @Override
    public boolean tableExists(TablePath tablePath) throws CatalogException {
        checkOpened();
        try (Connection connection = connection()) {
            TablePath path = normalizePath(connection, tablePath);
            try (ResultSet rs = connection.getMetaData().getTables(
                    null,
                    path.getSchemaName(),
                    path.getTableName(),
                    TABLE_TYPES)) {
                return rs.next();
            }
        } catch (SQLException e) {
            throw new CatalogException(
                    "检查 SAP HANA 表是否存在失败，table=" + tablePath, e);
        }
    }

    @Override
    public CatalogTable getTable(TablePath tablePath)
            throws CatalogException, TableNotFoundException {
        checkOpened();
        try (Connection connection = connection()) {
            TablePath path = normalizePath(connection, tablePath);
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
                    "获取 SAP HANA 表结构失败，table=" + tablePath, e);
        }
    }

    /** HANA tenant databases are selected by the JDBC endpoint and are not DDL-managed here. */
    @Override
    public void createDatabase(String databaseName, boolean ignoreIfExists)
            throws CatalogException, DatabaseAlreadyExistsException {
        throw new UnsupportedOperationException(
                "SAP HANA JDBC Offline Catalog 不负责 CREATE DATABASE；请预先创建 tenant database/schema");
    }

    @Override
    public void dropDatabase(String databaseName, boolean ignoreIfNotExists)
            throws CatalogException, DatabaseNotFoundException {
        throw new UnsupportedOperationException(
                "SAP HANA JDBC Offline Catalog 不负责 DROP DATABASE");
    }

    @Override
    public void createTable(CatalogTable table, boolean ignoreIfExists)
            throws CatalogException, DatabaseNotFoundException, TableAlreadyExistsException {
        checkOpened();
        try (Connection connection = connection()) {
            TablePath path = normalizePath(connection, table.getTablePath());
            if (tableExists(path)) {
                if (ignoreIfExists) {
                    return;
                }
                throw new TableAlreadyExistsException(catalogName, path);
            }

            CatalogTable ddlTable = table.getTablePath().equals(path)
                    ? table
                    : table.withPath(path);
            HanaCreateTableSqlBuilder builder =
                    new HanaCreateTableSqlBuilder(path, ddlTable, typeMapper);
            for (String sql : builder.buildStatements()) {
                execute(connection, sql);
            }
        } catch (TableAlreadyExistsException e) {
            throw e;
        } catch (SQLException e) {
            throw new CatalogException(
                    "创建 SAP HANA 表失败，table=" + table.getTablePath(), e);
        }
    }

    @Override
    public void addColumn(TablePath tablePath, Column column)
            throws CatalogException, TableNotFoundException {
        checkOpened();
        try (Connection connection = connection()) {
            TablePath path = normalizePath(connection, tablePath);
            if (!tableExists(path)) {
                throw new TableNotFoundException(catalogName, path);
            }

            boolean preserveSourceType = "true".equalsIgnoreCase(
                    column.getAttributes().get(HanaTypeMapper.NATIVE_ATTRIBUTE));
            String definition = new HanaCreateTableSqlBuilder(
                    path,
                    CatalogTable.builder(
                                    path,
                                    TableSchema.builder().column(column).build())
                            .build(),
                    typeMapper)
                    .buildColumnDefinition(column, preserveSourceType);
            execute(connection,
                    "ALTER TABLE " + HanaCreateTableSqlBuilder.quoteTable(path)
                            + " ADD (" + definition + ")");
            if (hasText(column.getComment())) {
                execute(connection,
                        "COMMENT ON COLUMN " + HanaCreateTableSqlBuilder.quoteTable(path)
                                + "." + HanaCreateTableSqlBuilder.quoteIdentifier(column.getName())
                                + " IS '" + HanaCreateTableSqlBuilder.escapeLiteral(column.getComment()) + "'");
            }
        } catch (TableNotFoundException e) {
            throw e;
        } catch (SQLException e) {
            throw new CatalogException(
                    "增加 SAP HANA 字段失败，table=" + tablePath
                            + "，column=" + column.getName(), e);
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
        try (Connection connection = connection()) {
            TablePath path = normalizePath(connection, tablePath);
            if (!tableExists(path)) {
                if (ignoreIfNotExists) {
                    return;
                }
                throw new TableNotFoundException(catalogName, path);
            }
            execute(connection, prefix + HanaCreateTableSqlBuilder.quoteTable(path));
        } catch (TableNotFoundException e) {
            throw e;
        } catch (SQLException e) {
            throw new CatalogException(operation + " SAP HANA 表失败，table=" + tablePath, e);
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
                    name = normalize(rs.getString("PK_NAME"));
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
                TABLE_TYPES)) {
            return rs.next() ? normalize(rs.getString("REMARKS")) : null;
        }
    }

    private TablePath normalizePath(Connection connection, TablePath tablePath)
            throws SQLException {
        Objects.requireNonNull(tablePath, "tablePath must not be null");
        String database = resolveDatabaseName(connection, tablePath.getDatabaseName());
        String schema = resolveSchema(connection, tablePath.getSchemaName());
        return TablePath.of(database, schema, tablePath.getTableName());
    }

    private String resolveSchema(Connection connection, String requestedSchema)
            throws SQLException {
        if (hasText(requestedSchema)) {
            return requestedSchema.trim();
        }
        if (hasText(configuredDefaultSchema)) {
            return configuredDefaultSchema;
        }
        try (PreparedStatement statement = connection.prepareStatement(CURRENT_SCHEMA_SQL);
             ResultSet rs = statement.executeQuery()) {
            if (rs.next()) {
                String schema = normalize(rs.getString(1));
                if (schema != null) {
                    return schema;
                }
            }
        }
        throw new CatalogException(
                "无法解析 SAP HANA 当前 schema，请显式配置 schema/currentSchema");
    }

    private String resolveDatabaseName(Connection connection, String requestedDatabase)
            throws SQLException {
        String discovered = configuredDatabaseName;

        if (discovered == null) {
            try (PreparedStatement statement = connection.prepareStatement(CURRENT_DATABASE_SQL);
                 ResultSet rs = statement.executeQuery()) {
                if (rs.next()) {
                    discovered = normalize(rs.getString(1));
                }
            } catch (SQLException ignored) {
                discovered = normalize(connection.getCatalog());
            }
        }

        if (hasText(requestedDatabase)
                && hasText(discovered)
                && !requestedDatabase.trim().equalsIgnoreCase(discovered)) {
            throw new IllegalArgumentException(
                    "SAP HANA 表路径中的 database 与当前 JDBC 连接不一致，current="
                            + discovered + "，requested=" + requestedDatabase);
        }
        if (hasText(discovered)) {
            return discovered;
        }
        return hasText(requestedDatabase) ? requestedDatabase.trim() : null;
    }

    private Connection connection() throws SQLException {
        Properties properties = config.toConnectionProperties();
        if (configuredDefaultSchema != null
                && !containsKeyIgnoreCase(properties, "currentSchema")) {
            properties.setProperty("currentSchema", configuredDefaultSchema);
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
                    "找不到 SAP HANA JDBC Driver：" + config.getDriverClass(), e);
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

    private static boolean isSystemSchema(String schema) {
        String normalized = schema.toUpperCase(Locale.ROOT);
        return "SYS".equals(normalized)
                || normalized.startsWith("_SYS_")
                || "SAP_HANA_INTERNAL".equals(normalized);
    }

    private static boolean containsKeyIgnoreCase(Properties properties, String key) {
        for (Object propertyKey : properties.keySet()) {
            if (propertyKey != null && key.equalsIgnoreCase(propertyKey.toString())) {
                return true;
            }
        }
        return false;
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
