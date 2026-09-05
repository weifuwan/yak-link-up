package com.link.up.connector.jdbc.catalog.iris;

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
import com.link.up.connector.jdbc.core.dialect.iris.IrisJdbcUrl;
import com.link.up.connector.jdbc.core.dialect.iris.IrisTypeMapper;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;

/** InterSystems IRIS offline JDBC Catalog scoped to one namespace. */
public final class IrisCatalog implements WritableCatalog {

    public static final String DIALECT = "iris";
    public static final String TABLE_OPTION_DIALECT = "dialect";
    private static final String DEFAULT_SCHEMA = "SQLUser";

    private final String catalogName;
    private final JdbcCatalogConfig config;
    private final String namespaceName;
    private final String defaultSchema;
    private final IrisTypeMapper typeMapper = new IrisTypeMapper();
    private volatile boolean opened;

    public IrisCatalog(
            String catalogName,
            JdbcCatalogConfig config,
            String connectorSchema) {

        if (!hasText(catalogName)) {
            throw new IllegalArgumentException("catalogName must not be empty");
        }
        this.catalogName = catalogName.trim();
        this.config = Objects.requireNonNull(config, "config must not be null");
        this.namespaceName = IrisJdbcUrl.namespaceName(config.getUrl());
        if (!hasText(namespaceName)) {
            throw new IllegalArgumentException(
                    "IRIS JDBC URL 必须包含 namespace，例如 jdbc:IRIS://127.0.0.1:1972/USER");
        }
        this.defaultSchema = hasText(connectorSchema)
                ? connectorSchema.trim()
                : DEFAULT_SCHEMA;
    }

    @Override
    public String name() {
        return catalogName;
    }

    @Override
    public Optional<String> getDefaultDatabase() {
        // TablePath calls this logical level "database"; for IRIS it is the
        // namespace selected by the JDBC URL.
        return Optional.of(namespaceName);
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
                        "IRIS Catalog 连接校验失败：" + config.getUrl());
            }
            opened = true;
        } catch (SQLException e) {
            throw new CatalogException(
                    "IRIS Catalog 连接失败：" + config.getUrl(), e);
        }
    }

    @Override
    public synchronized void close() {
        opened = false;
    }

    @Override
    public List<String> listDatabases() {
        checkOpened();
        return Collections.singletonList(namespaceName);
    }

    @Override
    public List<String> listSchemas(String database) throws CatalogException {
        checkNamespace(database);
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
            throw new CatalogException("获取 IRIS Schema 列表失败", e);
        }
    }

    @Override
    public List<TablePath> listTables(
            String database,
            String schemaName) throws CatalogException {

        checkNamespace(database);
        checkOpened();
        String schema = schema(schemaName);
        try (Connection connection = connection();
             ResultSet rs = connection.getMetaData().getTables(
                     null, schema, "%", new String[]{"TABLE"})) {
            List<TablePath> tables = new ArrayList<TablePath>();
            while (rs.next()) {
                String table = normalize(rs.getString("TABLE_NAME"));
                String tableSchema = normalize(rs.getString("TABLE_SCHEM"));
                if (table != null && !isSystemSchema(tableSchema)) {
                    tables.add(TablePath.of(
                            namespaceName,
                            tableSchema == null ? schema : tableSchema,
                            table));
                }
            }
            tables.sort(Comparator.comparing(TablePath::getTableName));
            return tables;
        } catch (SQLException e) {
            throw new CatalogException(
                    "获取 IRIS 表列表失败，schema=" + schema, e);
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
                    "检查 IRIS 表是否存在失败，table=" + path, e);
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
            PrimaryKey primaryKey = primaryKey(meta, path, columns);
            TableSchema schema = TableSchema.builder()
                    .columns(columns)
                    .primaryKey(primaryKey)
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
                    "获取 IRIS 表结构失败，table=" + path, e);
        }
    }

    @Override
    public void createDatabase(String database, boolean ignoreIfExists)
            throws CatalogException, DatabaseAlreadyExistsException {
        throw new UnsupportedOperationException(
                "IRIS JDBC Offline Catalog 不负责创建 Namespace；一个 job 固定使用 JDBC URL 中的 namespace");
    }

    @Override
    public void dropDatabase(String database, boolean ignoreIfNotExists)
            throws CatalogException, DatabaseNotFoundException {
        throw new UnsupportedOperationException(
                "IRIS JDBC Offline Catalog 不负责删除 Namespace；一个 job 固定使用 JDBC URL 中的 namespace");
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
        IrisCreateTableSqlBuilder builder =
                new IrisCreateTableSqlBuilder(path, ddlTable, typeMapper);
        try (Connection connection = connection()) {
            for (String sql : builder.buildStatements()) {
                execute(connection, sql);
            }
        } catch (SQLException e) {
            throw new CatalogException(
                    "创建 IRIS 表失败，table=" + path, e);
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
        String definition = new IrisCreateTableSqlBuilder(
                path, current, typeMapper)
                .buildColumnDefinition(column, false);
        try (Connection connection = connection()) {
            execute(connection,
                    "ALTER TABLE " + quoteTable(path)
                            + " ADD COLUMN " + definition);
        } catch (SQLException e) {
            throw new CatalogException(
                    "增加 IRIS 字段失败，table=" + path
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
                    operation + " IRIS 表失败，table=" + path, e);
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

    private PrimaryKey primaryKey(
            DatabaseMetaData meta,
            TablePath path,
            List<Column> visibleColumns) throws SQLException {

        Set<String> visible = new HashSet<String>();
        for (Column column : visibleColumns) {
            visible.add(column.getName().toUpperCase(Locale.ROOT));
        }
        try (ResultSet rs = meta.getPrimaryKeys(
                null,
                path.getSchemaName(),
                path.getTableName())) {
            String name = null;
            List<KeyColumn> keys = new ArrayList<KeyColumn>();
            while (rs.next()) {
                String column = rs.getString("COLUMN_NAME");
                if (!hasText(column)
                        || !visible.contains(column.trim().toUpperCase(Locale.ROOT))) {
                    // IRIS may expose a public RowID as an implicit JDBC PK. Do
                    // not inject a hidden/non-selected RowID into Link Up schema.
                    continue;
                }
                if (name == null) {
                    name = rs.getString("PK_NAME");
                }
                keys.add(new KeyColumn(rs.getShort("KEY_SEQ"), column));
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
        String sourceNamespace = tablePath.getDatabaseName();
        String targetSchema = tablePath.getSchemaName();
        if (!hasText(targetSchema)
                || (hasText(sourceNamespace)
                && !namespaceName.equalsIgnoreCase(sourceNamespace))) {
            targetSchema = defaultSchema;
        }
        return TablePath.of(namespaceName, targetSchema, tablePath.getTableName());
    }

    private String schema(String schemaName) {
        return hasText(schemaName) ? schemaName.trim() : defaultSchema;
    }

    private void checkNamespace(String requested) {
        if (hasText(requested)
                && !namespaceName.equalsIgnoreCase(requested.trim())) {
            throw new IllegalArgumentException(
                    "IRIS JDBC job 只支持 URL 当前 namespace：" + namespaceName
                            + "，requested=" + requested);
        }
    }

    private Connection connection() throws SQLException {
        Properties properties = config.toConnectionProperties();
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
                    "找不到 IRIS JDBC Driver：" + config.getDriverClass(), e);
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
        if (!hasText(schema)) {
            return true;
        }
        String normalized = schema.toLowerCase(Locale.ROOT);
        return normalized.startsWith("%")
                || "information_schema".equals(normalized);
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
