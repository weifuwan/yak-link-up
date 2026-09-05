package com.link.up.connector.jdbc.core.dialect.dameng;

import java.util.Locale;
import java.util.Map;

/** Utilities for Dameng JDBC URLs. */
public final class DamengJdbcUrl {

    private static final String PREFIX = "jdbc:dm://";

    private DamengJdbcUrl() {
    }

    public static boolean accepts(String url) {
        return url != null
                && url.trim().toLowerCase(Locale.ROOT).startsWith("jdbc:dm:");
    }

    /**
     * Resolves the session/default schema from explicit connection properties,
     * URL query properties and finally the optional /schemaName URL path.
     */
    public static String schema(String url, Map<String, String> properties) {
        String propertySchema = property(properties, "schema");
        if (hasText(propertySchema)) {
            return propertySchema.trim();
        }

        String querySchema = queryProperty(url, "schema");
        if (hasText(querySchema)) {
            return querySchema.trim();
        }

        return pathSchema(url);
    }

    static String pathSchema(String url) {
        if (!accepts(url)) {
            return null;
        }

        String value = url.trim();
        if (!value.toLowerCase(Locale.ROOT).startsWith(PREFIX)) {
            return null;
        }

        int query = value.indexOf('?');
        String main = query >= 0 ? value.substring(0, query) : value;
        int authorityStart = PREFIX.length();
        int slash = main.indexOf('/', authorityStart);
        if (slash < 0 || slash == main.length() - 1) {
            return null;
        }

        String schema = main.substring(slash + 1).trim();
        return hasText(schema) ? schema : null;
    }

    private static String queryProperty(String url, String key) {
        if (url == null) {
            return null;
        }
        int query = url.indexOf('?');
        if (query < 0 || query == url.length() - 1) {
            return null;
        }
        String[] entries = url.substring(query + 1).split("&");
        for (String entry : entries) {
            int separator = entry.indexOf('=');
            if (separator <= 0) {
                continue;
            }
            String name = entry.substring(0, separator).trim();
            if (key.equalsIgnoreCase(name)) {
                String value = entry.substring(separator + 1).trim();
                return hasText(value) ? value : null;
            }
        }
        return null;
    }

    private static String property(Map<String, String> properties, String key) {
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

    private static boolean hasText(String value) {
        return value != null && !value.trim().isEmpty();
    }
}
