package com.link.up.connector.jdbc.core.dialect.xugu;

import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.Locale;
import java.util.Map;

/** Utilities for XuguDB JDBC URL semantics. */
public final class XuguJdbcUrl {

    private static final String PREFIX = "jdbc:xugu://";

    private XuguJdbcUrl() {
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
        String value = normalized.substring(slash + 1, end).trim();
        return value.isEmpty() ? null : decode(value);
    }

    /** URL parameters take precedence over Properties, matching JDBC URL semantics. */
    public static String currentSchema(String url, Map<String, String> properties) {
        String fromUrl = queryValue(url, "current_schema");
        if (hasText(fromUrl)) {
            return firstSchema(fromUrl);
        }
        String property = propertyIgnoreCase(properties, "current_schema");
        if (!hasText(property)) {
            property = propertyIgnoreCase(properties, "schema");
        }
        return hasText(property) ? firstSchema(property) : null;
    }

    public static String user(String url, Map<String, String> properties) {
        String fromUrl = queryValue(url, "user");
        if (hasText(fromUrl)) {
            return fromUrl.trim();
        }
        String property = propertyIgnoreCase(properties, "user");
        return hasText(property) ? property.trim() : null;
    }

    public static String compatibleMode(String url, Map<String, String> properties) {
        String fromUrl = queryValue(url, "compatiblemode");
        if (hasText(fromUrl)) {
            return fromUrl.trim();
        }
        String property = propertyIgnoreCase(properties, "compatiblemode");
        return hasText(property) ? property.trim() : null;
    }

    /**
     * Xugu session parameters such as CURRENT_SCHEMA are interpreted with the
     * active compatibility-mode identifier rules. Quoted names remain exact.
     */
    public static String normalizeSessionIdentifier(
            String value,
            String compatibleMode) {

        if (!hasText(value)) {
            return null;
        }
        String normalized = value.trim();
        if (normalized.length() >= 2
                && normalized.charAt(0) == '\"'
                && normalized.charAt(normalized.length() - 1) == '\"') {
            return normalized.substring(1, normalized.length() - 1)
                    .replace("\"\"", "\"");
        }
        String mode = hasText(compatibleMode)
                ? compatibleMode.trim().toUpperCase(Locale.ROOT)
                : "NONE";
        if ("POSTGRESQL".equals(mode)) {
            return normalized.toLowerCase(Locale.ROOT);
        }
        if ("MYSQL".equals(mode)) {
            return normalized;
        }
        return normalized.toUpperCase(Locale.ROOT);
    }

    private static String queryValue(String url, String key) {
        if (!accepts(url)) {
            return null;
        }
        int query = url.indexOf('?');
        if (query < 0 || query == url.length() - 1) {
            return null;
        }
        String queryString = url.substring(query + 1);
        for (String item : queryString.split("&")) {
            int equals = item.indexOf('=');
            if (equals <= 0) {
                continue;
            }
            String itemKey = decode(item.substring(0, equals)).trim();
            if (key.equalsIgnoreCase(itemKey)) {
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
            if (entry.getKey() != null
                    && key.equalsIgnoreCase(entry.getKey().trim())) {
                return entry.getValue();
            }
        }
        return null;
    }

    private static String firstSchema(String value) {
        String normalized = value.trim();
        int comma = normalized.indexOf(',');
        return comma >= 0
                ? normalized.substring(0, comma).trim()
                : normalized;
    }

    private static String decode(String value) {
        try {
            return URLDecoder.decode(value, StandardCharsets.UTF_8.name());
        } catch (Exception ignored) {
            return value;
        }
    }

    private static boolean hasText(String value) {
        return value != null && !value.trim().isEmpty();
    }
}
