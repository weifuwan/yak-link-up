package com.link.up.connector.jdbc.core.dialect.highgo;

import java.util.Locale;
import java.util.Map;

/** HighGo JDBC URL parser used by offline catalog and target-path normalization. */
public final class HighGoJdbcUrl {

    private static final String PREFIX = "jdbc:highgo://";

    private HighGoJdbcUrl() {
    }

    public static boolean accepts(String url) {
        return url != null
                && url.trim().toLowerCase(Locale.ROOT).startsWith(PREFIX);
    }

    public static String databaseName(String url) {
        if (!accepts(url)) {
            return null;
        }

        String normalized = url.trim();
        int hostStart = PREFIX.length();
        int slash = normalized.indexOf('/', hostStart);
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

        String database = normalized.substring(slash + 1, end).trim();
        return database.isEmpty() ? null : database;
    }

    /**
     * Resolve currentSchema using JDBC-effective precedence. URL options are
     * treated as stronger than the Properties argument for PG-compatible drivers.
     */
    public static String currentSchema(
            String url,
            Map<String, String> properties) {

        String fromUrl = queryValue(url, "currentSchema");
        if (hasText(fromUrl)) {
            return firstSchema(fromUrl);
        }

        String fromProperties = propertyIgnoreCase(properties, "currentSchema");
        return hasText(fromProperties) ? firstSchema(fromProperties) : null;
    }

    /** Builds a URL for another database on the same HighGo server. */
    public static String withDatabase(String url, String database) {
        if (!accepts(url)) {
            throw new IllegalArgumentException("非法 HighGo JDBC URL：" + url);
        }
        if (!hasText(database)) {
            throw new IllegalArgumentException("database must not be empty");
        }

        String normalized = url.trim();
        int query = normalized.indexOf('?');
        String suffix = query >= 0 ? normalized.substring(query) : "";
        String main = query >= 0 ? normalized.substring(0, query) : normalized;
        int fragment = main.indexOf('#');
        if (fragment >= 0) {
            suffix = main.substring(fragment) + suffix;
            main = main.substring(0, fragment);
        }

        int slash = main.indexOf('/', PREFIX.length());
        String root = slash >= 0 ? main.substring(0, slash + 1) : main + "/";
        return root + database.trim() + suffix;
    }

    private static String queryValue(String url, String key) {
        if (!accepts(url)) {
            return null;
        }
        int query = url.indexOf('?');
        if (query < 0 || query == url.length() - 1) {
            return null;
        }
        String value = url.substring(query + 1);
        int fragment = value.indexOf('#');
        if (fragment >= 0) {
            value = value.substring(0, fragment);
        }
        for (String pair : value.split("&")) {
            int equals = pair.indexOf('=');
            if (equals <= 0) {
                continue;
            }
            String pairKey = pair.substring(0, equals).trim();
            if (key.equalsIgnoreCase(pairKey)) {
                String pairValue = pair.substring(equals + 1).trim();
                return pairValue.isEmpty() ? null : pairValue;
            }
        }
        return null;
    }

    private static String propertyIgnoreCase(
            Map<String, String> properties,
            String key) {
        if (properties == null || properties.isEmpty()) {
            return null;
        }
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            if (entry.getKey() != null
                    && key.equalsIgnoreCase(entry.getKey().trim())) {
                return entry.getValue();
            }
        }
        return null;
    }

    private static String firstSchema(String value) {
        String trimmed = value.trim();
        int comma = trimmed.indexOf(',');
        String first = comma >= 0 ? trimmed.substring(0, comma).trim() : trimmed;
        return first.isEmpty() ? null : first;
    }

    private static boolean hasText(String value) {
        return value != null && !value.trim().isEmpty();
    }
}
