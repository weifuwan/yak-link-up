package com.link.up.connector.jdbc.core.dialect.gbase.gbase8c;

import java.util.Locale;

/** Dedicated GBase 8c JDBC URL helpers for bounded/offline reads. */
public final class GBase8cJdbcUrl {

    private static final String PREFIX = "jdbc:gbase8c://";

    private GBase8cJdbcUrl() {
    }

    public static boolean accepts(String url) {
        return url != null
                && url.trim().toLowerCase(Locale.ROOT).startsWith(PREFIX);
    }

    public static String databaseName(String url) {
        if (!accepts(url)) {
            return null;
        }

        String value = url.trim();
        int queryIndex = value.indexOf('?');
        String main = queryIndex >= 0 ? value.substring(0, queryIndex) : value;
        int databaseSeparator = main.indexOf('/', PREFIX.length());
        if (databaseSeparator < 0 || databaseSeparator == main.length() - 1) {
            return null;
        }

        String database = main.substring(databaseSeparator + 1).trim();
        return database.isEmpty() ? null : database;
    }
}
