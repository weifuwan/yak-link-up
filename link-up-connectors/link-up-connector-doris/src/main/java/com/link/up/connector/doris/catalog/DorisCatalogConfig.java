package com.link.up.connector.doris.catalog;

import java.io.Serializable;
import java.util.Objects;
import java.util.Properties;

/**
 * Doris Catalog 连接配置。
 *
 * <p>Doris 兼容 MySQL 协议，Catalog 层通过 JDBC
 * 查询 INFORMATION_SCHEMA 实现元数据发现。
 */
public final class DorisCatalogConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String fenodes;
    private final int queryPort;
    private final String username;
    private final String password;
    private final String defaultDatabase;

    public DorisCatalogConfig(
            String fenodes,
            int queryPort,
            String username,
            String password,
            String defaultDatabase) {

        this.fenodes = requireText(fenodes, "fenodes");
        this.queryPort = queryPort;
        this.username = requireText(username, "username");
        this.password = password;
        this.defaultDatabase = normalize(defaultDatabase);
    }

    private static String normalize(String value) {
        if (value == null) {
            return null;
        }

        String normalized = value.trim();
        return normalized.isEmpty() ? null : normalized;
    }

    private static String requireText(
            String value,
            String fieldName) {

        String normalized = normalize(value);

        if (normalized == null) {
            throw new IllegalArgumentException(
                    fieldName + " must not be empty");
        }

        return normalized;
    }

    /**
     * 取第一个 FE 节点的 host。
     */
    public String getFirstFeHost() {
        String firstNode = fenodes.split(",")[0].trim();

        int colonIndex = firstNode.indexOf(':');

        if (colonIndex > 0) {
            return firstNode.substring(0, colonIndex);
        }

        return firstNode;
    }

    /**
     * 构建不指定数据库的 JDBC URL（root URL）。
     */
    public String getRootJdbcUrl() {
        return "jdbc:mysql://"
                + getFirstFeHost()
                + ":"
                + queryPort
                + "/";
    }

    /**
     * 构建指定数据库的 JDBC URL。
     */
    public String buildDatabaseUrl(String databaseName) {
        if (databaseName == null
                || databaseName.trim().isEmpty()) {

            return getRootJdbcUrl();
        }

        return "jdbc:mysql://"
                + getFirstFeHost()
                + ":"
                + queryPort
                + "/"
                + databaseName.trim();
    }

    /**
     * 构建带默认数据库的 JDBC URL。
     */
    public String getDefaultJdbcUrl() {
        if (defaultDatabase != null) {
            return buildDatabaseUrl(defaultDatabase);
        }

        return getRootJdbcUrl();
    }

    /**
     * 构造 JDBC 连接属性。
     */
    public Properties toConnectionProperties() {
        Properties properties = new Properties();

        properties.setProperty("user", username);

        if (password != null) {
            properties.setProperty("password", password);
        }

        /*
         * 禁止驱动把 tinyint(1) 自动转换为 Boolean，
         * 交由类型映射层统一决定。
         */
        properties.putIfAbsent(
                "tinyInt1isBit",
                "false");

        return properties;
    }

    // ── Getters ──────────────────────────────────────────

    public String getFenodes() {
        return fenodes;
    }

    public int getQueryPort() {
        return queryPort;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    public String getDefaultDatabase() {
        return defaultDatabase;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (!(obj instanceof DorisCatalogConfig)) {
            return false;
        }

        DorisCatalogConfig that =
                (DorisCatalogConfig) obj;

        return queryPort == that.queryPort
                && Objects.equals(fenodes, that.fenodes)
                && Objects.equals(username, that.username)
                && Objects.equals(password, that.password)
                && Objects.equals(
                defaultDatabase,
                that.defaultDatabase);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                fenodes,
                queryPort,
                username,
                password,
                defaultDatabase);
    }
}
