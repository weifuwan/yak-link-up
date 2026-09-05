package com.link.up.connector.jdbc.catalog.xugu;

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
import com.link.up.connector.jdbc.core.dialect.xugu.XuguJdbcUrl;
import com.link.up.connector.jdbc.core.dialect.xugu.XuguTypeMapper;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;

/** XuguDB offline JDBC Catalog scoped to the database selected by the URL. */
public final class XuguCatalog implements WritableCatalog {

    public static final String DIALECT = "xugu";
    public static final String TABLE_OPTION_DIALECT = "dialect";

    private static final String CURRENT_SCHEMA_SQL = "SELECT CURRENT_SCHEMA()";

    private final String catalogName;
    private final JdbcCatalogConfig config;
    private final String databaseName;
    private final String configuredSchema;
    private final boolean connectorSchemaExplicit;
    private final XuguTypeMapper typeMapper = new XuguTypeMapper();

    private volatile String defaultSchema;
    private volatile boolean opened;

    public XuguCatalog(
            String catalogName,
            JdbcCatalogConfig config,
            String connectorSchema) {

        if (!hasText(catalogName)) {
            throw new IllegalArgumentException("catalogName must not be empty");
        }
        this.catalogName = catalogName.trim();
        this.config = Objects.requireNonNull(config, "config must not be null");
        this.databaseName = XuguJdbcUrl.databaseName(config.getUrl());
        if (!hasText(databaseName)) {
            throw new IllegalArgumentException(
                    "XuguDB JDBC URL 必须包含数据库名，例如 jdbc:xugu://127.0.0.1:5138/SYSTEM");
        }

        String jdbcMode = XuguJdbcUrl.compatibleMode(
                config.getUrl(), config.getProperties());
        String jdbcSchema = XuguJdbcUrl.currentSchema(
                config.getUrl(), config.getProperties());
        if (hasText(jdbcSchema)) {
            this.configuredSchema = XuguJdbcUrl.normalizeSessionIdentifier(
                    jdbcSchema, jdbcMode);
            this.connectorSchemaExplicit = false;
        } else if (hasText(connectorSchema)) {
            // Connector schema is an explicit physical object name. Do not pass
            // it back through current_schema, which would apply compatibility-
            // mode case folding to an already-resolved identifier.
            this.configuredSchema = connectorSchema.trim();
            this.connectorSchemaExplicit = true;
        } else {
            this.configuredSchema = null;
            this.connectorSchemaExplicit = false;
        }
        this.defaultSchema = hasText(configuredSchema)
                ? configuredSchema
                : fallbackSchema(config);
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
                        "XuguDB Catalog 连接校验失败：" + config.getUrl());
            }
            if (!connectorSchemaExplicit) {
                // URL/properties current_schema and implicit user defaults are
                // session identifiers. Ask the server for the effective name so
                // compatible-mode case folding is represented exactly.
                String effectiveSchema = readCurrentSchema(connection);
                if (hasText(effectiveSchema)) {
                    defaultSchema = effectiveSchema;
                }
            }
            opened = true;
        } catch (SQLException e) {
            throw new CatalogException(
                    "XuguDB Catalog 连接失败：" + config.getUrl(), e);
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
                if (schema != null
                        && !"INFORMATION_SCHEMA".equalsIgnoreCase(schema)) {
                    schemas.add(schema);
                }
            }
            Collections.sort(schemas);
            return schemas;
        } catch (SQLException e) {
            throw new CatalogException("获取 XuguDB Schema 列表失败", e);
        }
    }

    @Override
    public List<TablePath> listTables(
            String database,
            String schemaName) throws CatalogException {

        checkDatabase(database);
        checkOpened();
        String schema = schema(schemaName);
        try (Connection connection = connection()) {
            DatabaseMetaData meta = connection.getMetaData();
            String schemaPattern = exactMetadataPattern(meta, schema);
            String tablePattern = metadataUsesLike() ? "%" : null;
            try (ResultSet rs = meta.getTables(
                    null,
                    schemaPattern,
                    tablePattern,
                    new String[]{"TABLE"})) {
                List<TablePath> tables = new ArrayList<TablePath>();
                while (rs.next()) {
                    String rowSchema = normalize(rs.getString("TABLE_SCHEM"));
                    String table = normalize(rs.getString("TABLE_NAME"));
                    if (table != null && schema.equals(rowSchema)) {
                        tables.add(TablePath.of(databaseName, schema, table));
                    }
                }
                tables.sort(Comparator.comparing(TablePath::getTableName));
                return tables;
            }
        } catch (SQLException e) {
            throw new CatalogException(
                    "获取 XuguDB 表列表失败，schema=" + schema, e);
        }
    }

    @Override
    public boolean tableExists(TablePath tablePath) throws CatalogException {
        checkOpened();
        TablePath path = normalizePath(tablePath);
        try (Connection connection = connection()) {
            DatabaseMetaData meta = connection.getMetaData();
            try (ResultSet rs = meta.getTables(
                    null,
                    exactMetadataPattern(meta, path.getSchemaName()),
                    exactMetadataPattern(meta, path.getTableName()),
                    new String[]{"TABLE"})) {
                while (rs.next()) {
                    if (metadataRowMatches(rs, path)) {
                        return true;
                    }
                }
                return false;
            }
        } catch (SQLException e) {
            throw new CatalogException(
                    "检查 XuguDB 表是否存在失败，table=" + path, e);
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
                    "获取 XuguDB 表结构失败，table=" + path, e);
        }
    }

    @Override
    public void createDatabase(String database, boolean ignoreIfExists)
            throws CatalogException, DatabaseAlreadyExistsException {
        throw new UnsupportedOperationException(
                "XuguDB JDBC Offline Catalog 不负责 CREATE DATABASE；一个 job 固定使用 JDBC URL 中的 database");
    }

    @Override
    public void dropDatabase(String database, boolean ignoreIfNotExists)
            throws CatalogException, DatabaseNotFoundException {
        throw new UnsupportedOperationException(
                "XuguDB JDBC Offline Catalog 不负责 DROP DATABASE；一个 job 固定使用 JDBC URL 中的 database");
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
        XuguCreateTableSqlBuilder builder =
                new XuguCreateTableSqlBuilder(path, ddlTable, typeMapper);
        try (Connection connection = connection()) {
            for (String sql : builder.buildStatements()) {
                execute(connection, sql);
            }
        } catch (SQLException e) {
            throw new CatalogException(
                    "创建 XuguDB 表失败，table=" + path, e);
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
        String definition = new XuguCreateTableSqlBuilder(
                path, current, typeMapper)
                .buildColumnDefinition(column, false);
        try (Connection connection = connection()) {
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
                    "增加 XuguDB 字段失败，table=" + path
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
                    operation + " XuguDB 表失败，table=" + path, e);
        }
    }

    private List<Column> columns(DatabaseMetaData meta, TablePath path)
            throws SQLException {
        String columnPattern = metadataUsesLike() ? "%" : null;
        try (ResultSet rs = meta.getColumns(
                null,
                exactMetadataPattern(meta, path.getSchemaName()),
                exactMetadataPattern(meta, path.getTableName()),
                columnPattern)) {
            List<Column> columns = new ArrayList<Column>();
            while (rs.next()) {
                if (metadataRowMatches(rs, path)) {
                    columns.add(typeMapper.toColumn(rs));
                }
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
                String column = rs.getString("COLUMN_NAME");
                if (hasText(column)) {
                    keys.add(new KeyColumn(rs.getShort("KEY_SEQ"), column));
                }
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
                exactMetadataPattern(meta, path.getSchemaName()),
                exactMetadataPattern(meta, path.getTableName()),
                new String[]{"TABLE"})) {
            while (rs.next()) {
                if (metadataRowMatches(rs, path)) {
                    return normalize(rs.getString("REMARKS"));
                }
            }
            return null;
        }
    }

    private String readCurrentSchema(Connection connection) {
        try (PreparedStatement statement = connection.prepareStatement(CURRENT_SCHEMA_SQL);
             ResultSet rs = statement.executeQuery()) {
            return rs.next() ? normalize(rs.getString(1)) : null;
        } catch (SQLException ignored) {
            return null;
        }
    }

    private TablePath normalizePath(TablePath tablePath) {
        Objects.requireNonNull(tablePath, "tablePath must not be null");
        String sourceDatabase = tablePath.getDatabaseName();
        if (hasText(sourceDatabase)
                && !databaseName.equalsIgnoreCase(sourceDatabase)) {
            throw new IllegalArgumentException(
                    "XuguDB JDBC job 只支持 URL 当前 database：" + databaseName
                            + "，requested=" + sourceDatabase);
        }

        String targetSchema = tablePath.getSchemaName();
        if (!hasText(targetSchema)) {
            targetSchema = defaultSchema;
        }
        if (!hasText(targetSchema)) {
            throw new IllegalArgumentException(
                    "无法解析 XuguDB 默认 schema；请配置 schema/username/current_schema");
        }
        return TablePath.of(databaseName, targetSchema, tablePath.getTableName());
    }

    private String schema(String schemaName) {
        String result = hasText(schemaName) ? schemaName.trim() : defaultSchema;
        if (!hasText(result)) {
            throw new IllegalArgumentException("XuguDB schema must not be empty");
        }
        return result;
    }

    private void checkDatabase(String requested) {
        if (hasText(requested)
                && !databaseName.equalsIgnoreCase(requested.trim())) {
            throw new IllegalArgumentException(
                    "XuguDB JDBC job 只支持 URL 当前 database：" + databaseName
                            + "，requested=" + requested);
        }
    }

    private boolean metadataUsesLike() {
        String configured = propertyIgnoreCase(
                config.toConnectionProperties(), "useLike");
        return !hasText(configured)
                || Boolean.parseBoolean(configured.trim());
    }

    private String exactMetadataPattern(
            DatabaseMetaData meta,
            String value) throws SQLException {
        if (!metadataUsesLike()) {
            return value;
        }
        return escapeMetadataPattern(value, meta.getSearchStringEscape());
    }

    static String escapeMetadataPattern(String value, String escape) {
        if (!hasText(value) || !hasText(escape)) {
            return value;
        }
        String escaped = value.replace(escape, escape + escape);
        escaped = escaped.replace("%", escape + "%");
        return escaped.replace("_", escape + "_");
    }

    private static boolean metadataRowMatches(ResultSet rs, TablePath path)
            throws SQLException {
        String rowSchema = normalize(rs.getString("TABLE_SCHEM"));
        String rowTable = normalize(rs.getString("TABLE_NAME"));
        return Objects.equals(path.getSchemaName(), rowSchema)
                && Objects.equals(path.getTableName(), rowTable);
    }

    private Connection connection() throws SQLException {
        Properties properties = config.toConnectionProperties();
        if (!containsPropertyIgnoreCase(properties, "useLike")) {
            properties.setProperty("useLike", "true");
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
                    "找不到 XuguDB JDBC Driver：" + config.getDriverClass(), e);
        }
    }

    private static void execute(Connection connection, String sql)
            throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.execute(sql);
        }
    }

    private void checkOpened() {
        if (!opened) {
            throw new IllegalStateException("Catalog 尚未打开，请先调用 open()");
        }
    }

    private static String fallbackSchema(JdbcCatalogConfig config) {
        String user = config.getUsername();
        if (!hasText(user)) {
            user = XuguJdbcUrl.user(config.getUrl(), config.getProperties());
        }
        String mode = XuguJdbcUrl.compatibleMode(
                config.getUrl(), config.getProperties());
        return XuguJdbcUrl.normalizeSessionIdentifier(user, mode);
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

    private static String propertyIgnoreCase(
            Properties properties,
            String key) {
        for (Object propertyKey : properties.keySet()) {
            if (propertyKey != null
                    && key.equalsIgnoreCase(String.valueOf(propertyKey).trim())) {
                Object value = properties.get(propertyKey);
                return value == null ? null : String.valueOf(value);
            }
        }
        return null;
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
