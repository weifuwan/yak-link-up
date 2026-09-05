package com.link.up.connector.jdbc.core.dialect.kingbase;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.Locale;
import java.util.Map;

/** Utilities for KingbaseES JDBC URLs. */
public final class KingbaseJdbcUrl {

    private static final String PREFIX = "jdbc:kingbase8:";
    private static final String NETWORK_PREFIX = "jdbc:kingbase8://";

    private KingbaseJdbcUrl() {
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
        String database;
        if (value.toLowerCase(Locale.ROOT).startsWith(NETWORK_PREFIX)) {
            int slash = value.indexOf('/', NETWORK_PREFIX.length());
            if (slash < 0 || slash == value.length() - 1) {
                return null;
            }
            int end = parameterStart(value, slash + 1);
            database = value.substring(slash + 1, end).trim();
        } else {
            int start = PREFIX.length();
            if (start >= value.length()) {
                return null;
            }
            int end = parameterStart(value, start);
            database = value.substring(start, end).trim();
            if (database.startsWith("//")) {
                return null;
            }
        }

        return database.isEmpty() ? null : decode(database);
    }

    /** Explicit connector properties override URL query parameters. */
    public static String currentSchema(String url, Map<String, String> properties) {
        String property = propertyIgnoreCase(properties, "currentSchema");
        if (hasText(property)) {
            return property.trim();
        }
        if (!accepts(url)) {
            return null;
        }

        int query = url.indexOf('?');
        if (query < 0 || query == url.length() - 1) {
            return null;
        }

        String parameters = url.substring(query + 1);
        int fragment = parameters.indexOf('#');
        if (fragment >= 0) {
            parameters = parameters.substring(0, fragment);
        }

        for (String item : parameters.split("&")) {
            int equals = item.indexOf('=');
            if (equals <= 0) {
                continue;
            }
            String key = decode(item.substring(0, equals)).trim();
            if ("currentSchema".equalsIgnoreCase(key)) {
                String value = decode(item.substring(equals + 1)).trim();
                return value.isEmpty() ? null : value;
            }
        }
        return null;
    }

    private static int parameterStart(String value, int from) {
        int end = value.length();
        int query = value.indexOf('?', from);
        if (query >= 0) {
            end = query;
        }
        int fragment = value.indexOf('#', from);
        if (fragment >= 0 && fragment < end) {
            end = fragment;
        }
        return end;
    }

    private static String propertyIgnoreCase(Map<String, String> properties, String key) {
        if (properties == null || properties.isEmpty()) {
            return null;
        }
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            if (entry.getKey() != null && key.equalsIgnoreCase(entry.getKey().trim())) {
                return entry.getValue();
            }
        }
        return null;
    }

    private static String decode(String value) {
        try {
            return URLDecoder.decode(value, "UTF-8");
        } catch (UnsupportedEncodingException impossible) {
            throw new IllegalStateException("UTF-8 is not supported", impossible);
        } catch (IllegalArgumentException invalidEncoding) {
            return value;
        }
    }

    private static boolean hasText(String value) {
        return value != null && !value.trim().isEmpty();
    }
}
