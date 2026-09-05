package com.link.up.connector.jdbc.core.dialect.db2;

import java.util.Locale;
import java.util.Map;

/** DB2 LUW JDBC URL helper. Supports network URLs and catalog aliases. */
public final class Db2JdbcUrl {

    private static final String PREFIX = "jdbc:db2:";
    private static final String NETWORK_PREFIX = "jdbc:db2://";
    private static final String CURRENT_SCHEMA = "currentSchema";
    private static final String JCC_CURRENT_SCHEMA = "db2.jcc.currentSchema";

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

    /**
     * Resolves JCC currentSchema. Explicit connector properties take precedence
     * over the URL property so callers can override a shared JDBC URL safely.
     */
    public static String currentSchema(String url, Map<String, String> properties) {
        String configured = property(properties, CURRENT_SCHEMA);
        if (configured == null) {
            configured = property(properties, JCC_CURRENT_SCHEMA);
        }
        return configured != null ? configured : urlProperty(url, CURRENT_SCHEMA);
    }

    public static String currentSchema(String url) {
        return currentSchema(url, null);
    }

    private static String property(Map<String, String> properties, String name) {
        if (properties == null || properties.isEmpty()) {
            return null;
        }
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            if (entry.getKey() != null && entry.getKey().equalsIgnoreCase(name)) {
                return normalize(entry.getValue());
            }
        }
        return null;
    }

    private static String urlProperty(String url, String name) {
        if (!accepts(url)) {
            return null;
        }

        String value = url.trim();
        String lower = value.toLowerCase(Locale.ROOT);
        String token = name.toLowerCase(Locale.ROOT) + "=";
        int from = 0;

        while (from < lower.length()) {
            int index = lower.indexOf(token, from);
            if (index < 0) {
                return null;
            }
            if (index == 0 || isPropertyDelimiter(value.charAt(index - 1))) {
                int start = index + token.length();
                int end = start;
                while (end < value.length() && !isValueDelimiter(value.charAt(end))) {
                    end++;
                }
                return normalize(value.substring(start, end));
            }
            from = index + token.length();
        }
        return null;
    }

    private static boolean isPropertyDelimiter(char value) {
        return value == ':' || value == ';' || value == '?' || value == '&';
    }

    private static boolean isValueDelimiter(char value) {
        return value == ';' || value == '?' || value == '#' || value == '&' || value == ':';
    }

    private static String normalize(String value) {
        if (value == null) {
            return null;
        }
        String normalized = value.trim();
        return normalized.isEmpty() ? null : normalized;
    }
}
