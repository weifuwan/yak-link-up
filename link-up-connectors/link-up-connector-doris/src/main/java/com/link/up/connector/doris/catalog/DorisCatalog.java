package com.link.up.connector.doris.catalog;

import com.link.up.api.table.catalog.*;
import com.link.up.api.table.catalog.exception.*;
import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Properties;

/**
 * Doris Catalog。
 *
 * <p>Doris 兼容 MySQL 协议，通过 JDBC 查询
 * INFORMATION_SCHEMA 实现元数据发现。
 *
 * <p>当前支持：
 *
 * <ol>
 *   <li>数据库发现；
 *   <li>数据表发现；
 *   <li>表结构读取；
 *   <li>主键读取；
 *   <li>建库、删库；
 *   <li>建表、删表、清表。
 * </ol>
 *
 * <p>不缓存 Connection，避免：
 *
 * <ol>
 *   <li>长连接失效；
 *   <li>多线程共享 Connection；
 *   <li>Catalog 长时间占用数据库资源。
 * </ol>
 */
@Slf4j
public final class DorisCatalog
        implements WritableCatalog {

    public static final String TABLE_OPTION_DIALECT =
            "dialect";

    private static final String LIST_DATABASES_SQL =
            "SELECT SCHEMA_NAME "
                    + "FROM INFORMATION_SCHEMA.SCHEMATA "
                    + "ORDER BY SCHEMA_NAME";

    private static final String DATABASE_EXISTS_SQL =
            "SELECT 1 "
                    + "FROM INFORMATION_SCHEMA.SCHEMATA "
                    + "WHERE SCHEMA_NAME = ?";

    private static final String LIST_TABLES_SQL =
            "SELECT TABLE_NAME "
                    + "FROM INFORMATION_SCHEMA.TABLES "
                    + "WHERE TABLE_SCHEMA = ? "
                    + "AND TABLE_TYPE = 'BASE TABLE' "
                    + "ORDER BY TABLE_NAME";

    private static final String TABLE_EXISTS_SQL =
            "SELECT 1 "
                    + "FROM INFORMATION_SCHEMA.TABLES "
                    + "WHERE TABLE_SCHEMA = ? "
                    + "AND TABLE_NAME = ? "
                    + "AND TABLE_TYPE = 'BASE TABLE'";

    private static final String SELECT_COLUMNS_SQL =
            "SELECT "
                    + "COLUMN_NAME, "
                    + "DATA_TYPE, "
                    + "COLUMN_TYPE, "
                    + "CHARACTER_MAXIMUM_LENGTH, "
                    + "NUMERIC_PRECISION, "
                    + "NUMERIC_SCALE, "
                    + "DATETIME_PRECISION, "
                    + "IS_NULLABLE, "
                    + "COLUMN_DEFAULT, "
                    + "COLUMN_COMMENT, "
                    + "CHARACTER_SET_NAME, "
                    + "COLLATION_NAME "
                    + "FROM INFORMATION_SCHEMA.COLUMNS "
                    + "WHERE TABLE_SCHEMA = ? "
                    + "AND TABLE_NAME = ? "
                    + "ORDER BY ORDINAL_POSITION";

    private static final String SELECT_PRIMARY_KEY_SQL =
            "SELECT "
                    + "CONSTRAINT_NAME, "
                    + "COLUMN_NAME "
                    + "FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE "
                    + "WHERE TABLE_SCHEMA = ? "
                    + "AND TABLE_NAME = ? "
                    + "ORDER BY ORDINAL_POSITION";

    private final String catalogName;
    private final DorisCatalogConfig config;
    private final DorisTypeMapper typeMapper;

    private volatile boolean opened;

    public DorisCatalog(
            String catalogName,
            DorisCatalogConfig config) {

        if (catalogName == null
                || catalogName.trim().isEmpty()) {

            throw new IllegalArgumentException(
                    "catalogName must not be empty");
        }

        this.catalogName = catalogName.trim();
        this.config = config;
        this.typeMapper = new DorisTypeMapper();
    }

    private static String quoteIdentifier(
            String identifier) {

        if (identifier == null
                || identifier.trim().isEmpty()) {

            throw new IllegalArgumentException(
                    "identifier must not be empty");
        }

        return "`"
                + identifier.replace("`", "``")
                + "`";
    }

    private static String quoteTable(TablePath tablePath) {
        return quoteIdentifier(
                tablePath.getDatabaseName())
                + "."
                + quoteIdentifier(
                tablePath.getTableName());
    }

    private static String normalize(String value) {
        if (value == null) {
            return null;
        }

        String normalized = value.trim();
        return normalized.isEmpty() ? null : normalized;
    }

    private static boolean hasText(String value) {
        return normalize(value) != null;
    }

    // ── Catalog 生命周期 ──────────────────────────────────

    @Override
    public String name() {
        return catalogName;
    }

    @Override
    public Optional<String> getDefaultDatabase() {
        return Optional.ofNullable(
                config.getDefaultDatabase());
    }

    @Override
    public synchronized void open()
            throws CatalogException {

        if (opened) {
            return;
        }

        loadDriver();

        try (Connection connection =
                     newConnection(config.getDefaultJdbcUrl())) {

            if (!connection.isValid(5)) {
                throw new CatalogException(
                        "Doris Catalog 连接校验失败："
                                + config.getDefaultJdbcUrl());
            }

            opened = true;

            log.info(
                    "Doris Catalog 已连接，catalog={}, url={}",
                    catalogName,
                    config.getDefaultJdbcUrl());

        } catch (SQLException e) {
            throw new CatalogException(
                    "Doris Catalog 连接失败："
                            + config.getDefaultJdbcUrl(),
                    e);
        }
    }

    @Override
    public synchronized void close()
            throws CatalogException {

        opened = false;

        log.info(
                "Doris Catalog 已关闭，catalog={}",
                catalogName);
    }

    // ── 数据库发现 ──────────────────────────────────────────

    @Override
    public List<String> listDatabases()
            throws CatalogException {

        checkOpened();

        try (Connection connection =
                     openRootConnection();
             PreparedStatement statement =
                     connection.prepareStatement(
                             LIST_DATABASES_SQL);
             ResultSet resultSet =
                     statement.executeQuery()) {

            List<String> databases =
                    new ArrayList<>();

            while (resultSet.next()) {
                databases.add(
                        resultSet.getString(1));
            }

            return databases;

        } catch (SQLException e) {
            throw new CatalogException(
                    "获取 Doris 数据库列表失败",
                    e);
        }
    }

    @Override
    public List<String> listSchemas(String databaseName) {
        return Collections.emptyList();
    }

    // ── 表发现 ──────────────────────────────────────────

    @Override
    public List<TablePath> listTables(
            String databaseName,
            String schemaName)
            throws CatalogException {

        checkOpened();

        String database =
                resolveDatabaseName(
                        databaseName,
                        schemaName);

        if (!databaseExists(database)) {
            throw new DatabaseNotFoundException(
                    catalogName,
                    database);
        }

        try (Connection connection =
                     openRootConnection();
             PreparedStatement statement =
                     connection.prepareStatement(
                             LIST_TABLES_SQL)) {

            statement.setString(1, database);

            try (ResultSet resultSet =
                         statement.executeQuery()) {

                List<TablePath> tables =
                        new ArrayList<>();

                while (resultSet.next()) {
                    tables.add(
                            TablePath.of(
                                    database,
                                    resultSet.getString(
                                            "TABLE_NAME")));
                }

                return tables;
            }

        } catch (SQLException e) {
            throw new CatalogException(
                    "获取 Doris 表列表失败，database="
                            + database,
                    e);
        }
    }

    @Override
    public boolean tableExists(TablePath tablePath)
            throws CatalogException {

        checkOpened();

        TablePath normalized =
                normalizeTablePath(tablePath);

        try (Connection connection =
                     openRootConnection();
             PreparedStatement statement =
                     connection.prepareStatement(
                             TABLE_EXISTS_SQL)) {

            statement.setString(
                    1,
                    normalized.getDatabaseName());

            statement.setString(
                    2,
                    normalized.getTableName());

            try (ResultSet resultSet =
                         statement.executeQuery()) {

                return resultSet.next();
            }

        } catch (SQLException e) {
            throw new CatalogException(
                    "检查 Doris 表是否存在失败，table="
                            + normalized,
                    e);
        }
    }

    // ── 表结构读取 ──────────────────────────────────────────

    @Override
    public CatalogTable getTable(TablePath tablePath)
            throws CatalogException,
            TableNotFoundException {

        checkOpened();

        TablePath normalized =
                normalizeTablePath(tablePath);

        try (Connection connection =
                     openRootConnection()) {

            List<Column> columns =
                    readColumns(connection, normalized);

            if (columns.isEmpty()) {
                throw new TableNotFoundException(
                        catalogName,
                        normalized);
            }

            PrimaryKey primaryKey =
                    readPrimaryKey(connection, normalized);

            TableSchema tableSchema =
                    TableSchema.builder()
                            .columns(columns)
                            .primaryKey(primaryKey)
                            .build();

            return CatalogTable.builder(
                            normalized,
                            tableSchema)
                    .option(TABLE_OPTION_DIALECT, "doris")
                    .build();

        } catch (TableNotFoundException e) {
            throw e;
        } catch (SQLException e) {
            throw new CatalogException(
                    "获取 Doris 表结构失败，table="
                            + normalized,
                    e);
        }
    }

    // ── DDL 操作 ──────────────────────────────────────────

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
                        + quoteIdentifier(databaseName);

        try (Connection connection =
                     openRootConnection()) {

            execute(connection, sql);

        } catch (SQLException e) {
            throw new CatalogException(
                    "创建 Doris 数据库失败，database="
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
                        + quoteIdentifier(databaseName);

        try (Connection connection =
                     openRootConnection()) {

            execute(connection, sql);

        } catch (SQLException e) {
            throw new CatalogException(
                    "删除 Doris 数据库失败，database="
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
                normalizeTablePath(
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

        String sql =
                new DorisCreateTableSqlBuilder(
                        tablePath,
                        table,
                        typeMapper)
                        .build();

        try (Connection connection =
                     openDatabaseConnection(databaseName)) {

            execute(connection, sql);

        } catch (SQLException e) {
            throw new CatalogException(
                    "创建 Doris 表失败，table="
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
                normalizeTablePath(tablePath);

        if (!tableExists(normalized)) {
            throw new TableNotFoundException(
                    catalogName,
                    normalized);
        }

        CatalogTable table = getTable(normalized);

        String definition =
                new DorisCreateTableSqlBuilder(
                                normalized,
                                table,
                                typeMapper)
                        .buildColumnDefinition(column);

        String sql =
                "ALTER TABLE "
                        + quoteTable(normalized)
                        + " ADD COLUMN "
                        + definition;

        try (Connection connection =
                     openDatabaseConnection(
                             normalized.getDatabaseName())) {

            execute(connection, sql);

        } catch (SQLException e) {
            throw new CatalogException(
                    "增加 Doris 字段失败，table="
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
                normalizeTablePath(tablePath);

        if (!tableExists(normalized)) {
            if (ignoreIfNotExists) {
                return;
            }

            throw new TableNotFoundException(
                    catalogName,
                    normalized);
        }

        String sql =
                "DROP TABLE "
                        + quoteTable(normalized);

        try (Connection connection =
                     openDatabaseConnection(
                             normalized.getDatabaseName())) {

            execute(connection, sql);

        } catch (SQLException e) {
            throw new CatalogException(
                    "删除 Doris 表失败，table="
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
                normalizeTablePath(tablePath);

        if (!tableExists(normalized)) {
            if (ignoreIfNotExists) {
                return;
            }

            throw new TableNotFoundException(
                    catalogName,
                    normalized);
        }

        String sql =
                "TRUNCATE TABLE "
                        + quoteTable(normalized);

        try (Connection connection =
                     openDatabaseConnection(
                             normalized.getDatabaseName())) {

            execute(connection, sql);

        } catch (SQLException e) {
            throw new CatalogException(
                    "清空 Doris 表失败，table="
                            + normalized,
                    e);
        }
    }

    // ── 内部方法 ──────────────────────────────────────────

    public boolean databaseExists(String databaseName)
            throws CatalogException {

        if (!hasText(databaseName)) {
            return false;
        }

        checkOpened();

        try (Connection connection =
                     openRootConnection();
             PreparedStatement statement =
                     connection.prepareStatement(
                             DATABASE_EXISTS_SQL)) {

            statement.setString(1, databaseName);

            try (ResultSet resultSet =
                         statement.executeQuery()) {

                return resultSet.next();
            }

        } catch (SQLException e) {
            throw new CatalogException(
                    "检查 Doris 数据库是否存在失败，database="
                            + databaseName,
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
                    tablePath.getTableName());

            try (ResultSet resultSet =
                         statement.executeQuery()) {

                List<Column> columns =
                        new ArrayList<>();

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
                    tablePath.getTableName());

            try (ResultSet resultSet =
                         statement.executeQuery()) {

                String primaryKeyName = null;
                List<String> columns =
                        new ArrayList<>();

                while (resultSet.next()) {
                    String constraintName =
                            resultSet.getString(
                                    "CONSTRAINT_NAME");

                    // Doris 主键约束名通常为 'PRIMARY'，
                    // 但也可能是其他名称，优先匹配 'PRIMARY'，
                    // 否则取第一个约束。
                    if (primaryKeyName == null) {
                        primaryKeyName = constraintName;
                    }

                    // 如果已找到 PRIMARY 约束，只收集该约束的列
                    if ("PRIMARY".equalsIgnoreCase(primaryKeyName)) {
                        if (!"PRIMARY".equalsIgnoreCase(constraintName)) {
                            continue;
                        }
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

    private Connection openRootConnection()
            throws SQLException {

        checkOpened();
        return newConnection(config.getRootJdbcUrl());
    }

    private Connection openDatabaseConnection(
            String databaseName)
            throws SQLException {

        checkOpened();
        return newConnection(
                config.buildDatabaseUrl(databaseName));
    }

    private Connection newConnection(String url)
            throws SQLException {

        Properties properties =
                config.toConnectionProperties();

        return DriverManager.getConnection(
                url,
                properties);
    }

    private void execute(
            Connection connection,
            String sql)
            throws SQLException {

        log.info("执行 Doris Catalog SQL：{}", sql);

        try (PreparedStatement statement =
                     connection.prepareStatement(sql)) {

            statement.execute();
        }
    }

    /**
     * 执行任意 DDL 语句（供 SinkPreparer 调用）。
     */
    public void executeDdl(String sql) throws CatalogException {
        checkOpened();
        try (Connection connection = openRootConnection()) {
            execute(connection, sql);
        } catch (SQLException e) {
            throw new CatalogException("执行 Doris DDL 失败：" + sql, e);
        }
    }

    private void loadDriver() {
        try {
            Class.forName(
                    "com.mysql.cj.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            throw new CatalogException(
                    "找不到 MySQL JDBC Driver，"
                            + "Doris Catalog 依赖 MySQL 驱动",
                    e);
        }
    }

    private void checkOpened() {
        if (!opened) {
            throw new IllegalStateException(
                    "Catalog 尚未打开，请先调用 open()");
        }
    }

    private String resolveDatabaseName(
            String databaseName,
            String schemaName) {

        if (hasText(databaseName)) {
            return databaseName.trim();
        }

        if (hasText(schemaName)) {
            return schemaName.trim();
        }

        return getDefaultDatabase()
                .orElseThrow(
                        () ->
                                new IllegalArgumentException(
                                        "没有指定 Doris 数据库"));
    }

    private TablePath normalizeTablePath(
            TablePath tablePath) {

        String databaseName =
                normalize(tablePath.getDatabaseName());

        if (databaseName == null) {
            databaseName =
                    normalize(tablePath.getSchemaName());
        }

        if (databaseName == null) {
            databaseName =
                    config.getDefaultDatabase();
        }

        if (databaseName == null) {
            throw new IllegalArgumentException(
                    "没有指定数据库，table="
                            + tablePath
                            + "，配置中也没有默认数据库");
        }

        return TablePath.of(
                databaseName,
                tablePath.getTableName());
    }
}
