package com.link.up.connector.jdbc.core.dialect.db2;

import java.util.Locale;

/** DB2 LUW JDBC URL helper. Supports network URLs and catalog aliases. */
public final class Db2JdbcUrl {

    private static final String PREFIX = "jdbc:db2:";
    private static final String NETWORK_PREFIX = "jdbc:db2://";

    private Db2JdbcUrl() {
    }

    public static boolean accepts(String url) {
        return url != null
                && url.trim()
                .toLowerCase(Locale.ROOT)
                .startsWith(PREFIX);
    }

    public static String databaseName(String url) {
        if (!accepts(url)) {
            return null;
        }

        String value = url.trim();
        String lower = value.toLowerCase(Locale.ROOT);
        int start;

        if (lower.startsWith(NETWORK_PREFIX)) {
            int slash = value.indexOf('/', NETWORK_PREFIX.length());
            if (slash < 0 || slash == value.length() - 1) {
                return null;
            }
            start = slash + 1;
        } else {
            start = PREFIX.length();
            if (start >= value.length()) {
                return null;
            }
        }

        int end = value.length();
        char[] delimiters = new char[]{':', ';', '?', '#'};
        for (char delimiter : delimiters) {
            int index = value.indexOf(delimiter, start);
            if (index >= 0 && index < end) {
                end = index;
            }
        }

        String database = value.substring(start, end).trim();
        return database.isEmpty() ? null : database;
    }
}
