package com.link.up.connector.jdbc.core.dialect.opengauss;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.Locale;
import java.util.Map;

/** Utilities for network-style openGauss JDBC URLs. */
public final class OpenGaussJdbcUrl {

    private static final String PREFIX = "jdbc:opengauss://";

    private OpenGaussJdbcUrl() {
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
        int hostStart = PREFIX.length();
        int slash = value.indexOf('/', hostStart);
        if (slash < 0 || slash == value.length() - 1) {
            return null;
        }
        int end = value.length();
        int query = value.indexOf('?', slash + 1);
        if (query >= 0) {
            end = query;
        }
        int fragment = value.indexOf('#', slash + 1);
        if (fragment >= 0 && fragment < end) {
            end = fragment;
        }
        String database = value.substring(slash + 1, end).trim();
        return database.isEmpty() ? null : decode(database);
    }

    /**
     * Resolves the JDBC currentSchema parameter. Explicit connector properties
     * override URL query parameters.
     */
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
