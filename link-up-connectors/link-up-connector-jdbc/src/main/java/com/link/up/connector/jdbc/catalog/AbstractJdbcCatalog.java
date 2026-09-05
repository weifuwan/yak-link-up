package com.link.up.connector.jdbc.catalog;

import com.link.up.api.table.catalog.Catalog;
import com.link.up.api.table.catalog.TablePath;
import com.link.up.api.table.catalog.exception.CatalogException;
import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;

/**
 * JDBC Catalog 基础实现。
 * <p>
 * 主要负责：
 * <p>
 * 1. JDBC Driver 加载；
 * 2. JDBC URL 解析；
 * 3. 连接创建；
 * 4. Catalog 生命周期校验；
 * 5. 通用 SQL 执行。
 * <p>
 * 不缓存 Connection，避免：
 * <p>
 * 1. 长连接失效；
 * 2. 多线程共享 Connection；
 * 3. Catalog 长时间占用数据库资源。
 */
@Slf4j
public abstract class AbstractJdbcCatalog
        implements Catalog {

    protected final String catalogName;
    protected final JdbcCatalogConfig config;
    protected final JdbcUrlInfo urlInfo;

    private volatile boolean opened;

    /**
     * 保留 MySQL 默认行为，已有 MySQL Catalog 无需改动。
     */
    protected AbstractJdbcCatalog(
            String catalogName,
            JdbcCatalogConfig config) {

        this(
                catalogName,
                config,
                JdbcUrlInfo.parseMySql(
                        config.getUrl()));
    }

    /**
     * 新数据库可以提供自己的 JDBC URL 解析结果，复用公共 Catalog 生命周期。
     */
    protected AbstractJdbcCatalog(
            String catalogName,
            JdbcCatalogConfig config,
            JdbcUrlInfo urlInfo) {

        if (catalogName == null
                || catalogName.trim().isEmpty()) {

            throw new IllegalArgumentException(
                    "catalogName must not be empty");
        }

        this.catalogName =
                catalogName.trim();

        this.config =
                Objects.requireNonNull(
                        config,
                        "config must not be null");

        this.urlInfo =
                Objects.requireNonNull(
                        urlInfo,
                        "urlInfo must not be null");
    }

    protected static String quoteIdentifier(
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

    protected static String quoteTable(
            TablePath tablePath) {

        return quoteIdentifier(
                tablePath.getDatabaseName())
                + "."
                + quoteIdentifier(
                tablePath.getTableName());
    }

    private static String normalize(
            String value) {

        if (value == null) {
            return null;
        }

        String normalized =
                value.trim();

        return normalized.isEmpty()
                ? null
                : normalized;
    }

    @Override
    public String name() {
        return catalogName;
    }

    @Override
    public Optional<String> getDefaultDatabase() {
        return Optional.ofNullable(
                urlInfo.getDefaultDatabase());
    }

    @Override
    public synchronized void open()
            throws CatalogException {

        if (opened) {
            return;
        }

        loadDriver();

        try (Connection connection =
                     newConnection(
                             config.getUrl())) {

            if (!connection.isValid(5)) {
                throw new CatalogException(
                        "JDBC Catalog 连接校验失败："
                                + config.getUrl());
            }

            opened = true;

            log.info(
                    "JDBC Catalog 已连接，catalog={}, url={}",
                    catalogName,
                    config.getUrl());

        } catch (SQLException e) {
            throw new CatalogException(
                    "JDBC Catalog 连接失败："
                            + config.getUrl(),
                    e);
        }
    }

    @Override
    public synchronized void close() {
        /*
         * 当前实现不缓存 Connection，
         * 因此没有需要主动释放的长期资源。
         */
        opened = false;

        log.info(
                "JDBC Catalog 已关闭，catalog={}",
                catalogName);
    }

    /**
     * 创建连接到 JDBC URL 中默认数据库的连接。
     */
    protected final Connection openDefaultConnection()
            throws SQLException {

        checkOpened();

        return newConnection(
                config.getUrl());
    }

    /**
     * 创建不指定数据库的连接。
     *
     * <p>MySQL 主要用于数据库管理。其他数据库如果不支持无数据库 URL，
     * 可以在自己的 Catalog 中使用 openDefaultConnection/openDatabaseConnection。
     */
    protected final Connection openRootConnection()
            throws SQLException {

        checkOpened();

        return newConnection(
                urlInfo.getRootUrl());
    }

    /**
     * 创建指定数据库连接。
     */
    protected final Connection openDatabaseConnection(
            String databaseName)
            throws SQLException {

        checkOpened();

        return newConnection(
                urlInfo.buildDatabaseUrl(
                        databaseName));
    }

    protected final String getDatabaseUrl(
            String databaseName) {

        return urlInfo.buildDatabaseUrl(
                databaseName);
    }

    /**
     * 获取 TablePath 中的数据库。
     * <p>
     * 如果没有显式指定，则使用 JDBC URL 中的默认数据库。
     *
     * <p>该方法保留 MySQL database/schema 兼容语义；
     * 使用独立 Schema 的数据库应在自身 Catalog 中规范化 TablePath。
     */
    protected final String resolveDatabase(
            TablePath tablePath) {

        String databaseName =
                normalize(
                        tablePath.getDatabaseName());

        if (databaseName == null) {
            databaseName =
                    normalize(
                            tablePath.getSchemaName());
        }

        if (databaseName == null) {
            databaseName =
                    normalize(
                            urlInfo.getDefaultDatabase());
        }

        if (databaseName == null) {
            throw new IllegalArgumentException(
                    "没有指定数据库，table="
                            + tablePath
                            + "，JDBC URL 中也没有默认数据库");
        }

        return databaseName;
    }

    /**
     * 将 TablePath 规范化为 MySQL database.table。
     */
    protected final TablePath normalizeTablePath(
            TablePath tablePath) {

        return TablePath.of(
                resolveDatabase(
                        tablePath),
                tablePath.getTableName());
    }

    /**
     * 执行 DDL 或普通 SQL。
     */
    protected final void execute(
            Connection connection,
            String sql)
            throws SQLException {

        log.info(
                "执行 Catalog SQL：{}",
                sql);

        try (PreparedStatement statement =
                     connection.prepareStatement(
                             sql)) {

            statement.execute();
        }
    }

    protected final Connection newConnection(
            String url)
            throws SQLException {

        Properties properties =
                config.toConnectionProperties();

        return DriverManager.getConnection(
                url,
                properties);
    }

    private void loadDriver() {
        String driverClass =
                config.getDriverClass();

        if (driverClass == null) {
            return;
        }

        try {
            Class.forName(
                    driverClass);
        } catch (ClassNotFoundException e) {
            throw new CatalogException(
                    "找不到 JDBC Driver："
                            + driverClass,
                    e);
        }
    }

    protected final void checkOpened() {
        if (!opened) {
            throw new IllegalStateException(
                    "Catalog 尚未打开，请先调用 open()");
        }
    }

    /**
     * JDBC URL 解析结果。
     *
     * <p>当前为 MySQL 和 PostgreSQL 的 network-style JDBC URL 提供解析；
     * Oracle 等 URL 形态不同的数据库应增加自己的解析入口。
     */
    protected static final class JdbcUrlInfo {

        private final String rootUrl;
        private final String defaultDatabase;
        private final String suffix;

        private JdbcUrlInfo(
                String rootUrl,
                String defaultDatabase,
                String suffix) {

            this.rootUrl = rootUrl;
            this.defaultDatabase = defaultDatabase;
            this.suffix = suffix;
        }

        public static JdbcUrlInfo parseMySql(
                String jdbcUrl) {

            return parseNetworkUrl(
                    jdbcUrl,
                    "jdbc:mysql://",
                    "MySQL");
        }

        public static JdbcUrlInfo parsePostgres(
                String jdbcUrl) {

            return parseNetworkUrl(
                    jdbcUrl,
                    "jdbc:postgresql://",
                    "PostgreSQL");
        }

        private static JdbcUrlInfo parseNetworkUrl(
                String jdbcUrl,
                String prefix,
                String databaseName) {

            if (jdbcUrl == null
                    || !jdbcUrl.startsWith(
                    prefix)) {

                throw new IllegalArgumentException(
                        "非法 "
                                + databaseName
                                + " JDBC URL："
                                + jdbcUrl);
            }

            int queryIndex =
                    jdbcUrl.indexOf('?');

            String mainUrl =
                    queryIndex >= 0
                            ? jdbcUrl.substring(
                            0,
                            queryIndex)
                            : jdbcUrl;

            String suffix =
                    queryIndex >= 0
                            ? jdbcUrl.substring(
                            queryIndex)
                            : "";

            int hostStart =
                    prefix.length();

            int databaseSeparator =
                    mainUrl.indexOf(
                            '/',
                            hostStart);

            if (databaseSeparator < 0) {
                return new JdbcUrlInfo(
                        mainUrl + "/",
                        null,
                        suffix);
            }

            String rootUrl =
                    mainUrl.substring(
                            0,
                            databaseSeparator + 1);

            String database =
                    mainUrl.substring(
                            databaseSeparator + 1);

            if (database.trim().isEmpty()) {
                database = null;
            }

            return new JdbcUrlInfo(
                    rootUrl,
                    database,
                    suffix);
        }

        public String getRootUrl() {
            return rootUrl + suffix;
        }

        public String getDefaultDatabase() {
            return defaultDatabase;
        }

        public String buildDatabaseUrl(
                String databaseName) {

            if (databaseName == null
                    || databaseName.trim()
                    .isEmpty()) {

                return getRootUrl();
            }

            return rootUrl
                    + databaseName.trim()
                    + suffix;
        }
    }
}
