package com.link.up.connector.jdbc.core.dialect.oceanbase;

import java.util.Locale;

/**
 * Small parser for OceanBase network-style JDBC URLs.
 */
public final class OceanBaseJdbcUrl {

    private static final String PREFIX = "jdbc:oceanbase:";

    private OceanBaseJdbcUrl() {
    }

    public static boolean accepts(String url) {
        return url != null
                && url.trim()
                .toLowerCase(Locale.ROOT)
                .startsWith(PREFIX);
    }

    /**
     * Extracts the database path from URLs such as:
     * jdbc:oceanbase://127.0.0.1:2881/app?useUnicode=true
     */
    public static String databaseName(String url) {
        if (!accepts(url)) {
            throw new IllegalArgumentException(
                    "非法 OceanBase JDBC URL：" + url);
        }

        String normalized = url.trim();
        int protocol = normalized.indexOf("://");
        if (protocol < 0) {
            return null;
        }

        int slash = normalized.indexOf('/', protocol + 3);
        if (slash < 0 || slash == normalized.length() - 1) {
            return null;
        }

        int end = normalized.length();

        int query = normalized.indexOf('?', slash + 1);
        if (query >= 0) {
            end = query;
        }

        int fragment = normalized.indexOf('#', slash + 1);
        if (fragment >= 0 && fragment < end) {
            end = fragment;
        }

        int semicolon = normalized.indexOf(';', slash + 1);
        if (semicolon >= 0 && semicolon < end) {
            end = semicolon;
        }

        String database =
                normalized.substring(slash + 1, end).trim();

        return database.isEmpty() ? null : database;
    }

    /**
     * OceanBase MySQL mode speaks the MySQL protocol. The MySQL Catalog can
     * therefore be reused by translating only the JDBC scheme while keeping
     * host, port, database and query parameters unchanged.
     */
    public static String toMySqlUrl(String url) {
        if (!accepts(url)) {
            throw new IllegalArgumentException(
                    "非法 OceanBase JDBC URL：" + url);
        }

        String normalized = url.trim();
        return "jdbc:mysql:"
                + normalized.substring(PREFIX.length());
    }
}
