package com.link.up.connector.jdbc.core.dialect.duckdb;

import java.util.Locale;
import java.util.Map;

/** Utilities for DuckDB JDBC URL semantics. */
public final class DuckDbJdbcUrl {

    private static final String PREFIX = "jdbc:duckdb:";
    private static final String MEMORY_PREFIX = "memory:";
    private static final String DUCKLAKE_PREFIX = "ducklake:";

    private DuckDbJdbcUrl() {
    }

    public static boolean accepts(String url) {
        return url != null
                && url.trim().toLowerCase(Locale.ROOT).startsWith(PREFIX);
    }

    /**
     * DuckDB's anonymous in-memory URL creates a connection-private database.
     * The shared JDBC connector necessarily uses more than one connection
     * across catalog/planning/read/write phases, so this form is unsafe.
     */
    public static boolean isConnectionPrivateMemory(String url) {
        if (!accepts(url)) {
            return false;
        }
        String database = databaseSpec(url);
        return database.isEmpty()
                || MEMORY_PREFIX.equalsIgnoreCase(database);
    }

    public static boolean isNamedMemory(String url) {
        String database = databaseSpec(url);
        return database.toLowerCase(Locale.ROOT).startsWith(MEMORY_PREFIX)
                && database.length() > MEMORY_PREFIX.length();
    }

    public static boolean isDuckLake(String url) {
        return databaseSpec(url)
                .toLowerCase(Locale.ROOT)
                .startsWith(DUCKLAKE_PREFIX);
    }

    /**
     * The connector requires catalog/planning/read/write connections to reach
     * the same DuckDB instance. Disabling the JDBC instance cache breaks that
     * invariant for named-memory databases and is unsafe for file-backed jobs.
     */
    public static boolean isInstanceCacheDisabled(
            String url,
            Map<String, String> properties) {

        String urlValue = optionValue(url, "jdbc_instance_cache");
        if (hasText(urlValue)) {
            return "false".equalsIgnoreCase(urlValue.trim());
        }
        String property = propertyIgnoreCase(properties, "jdbc_instance_cache");
        return hasText(property) && "false".equalsIgnoreCase(property.trim());
    }

    /** Named-memory jobs need pinning because catalog/read/write phases may have no overlapping connection. */
    public static boolean isPinDbEnabled(
            String url,
            Map<String, String> properties) {

        String urlValue = optionValue(url, "jdbc_pin_db");
        if (hasText(urlValue)) {
            return "true".equalsIgnoreCase(urlValue.trim());
        }
        String property = propertyIgnoreCase(properties, "jdbc_pin_db");
        return hasText(property) && "true".equalsIgnoreCase(property.trim());
    }

    public static boolean isUnpinnedNamedMemory(
            String url,
            Map<String, String> properties) {
        return isNamedMemory(url) && !isPinDbEnabled(url, properties);
    }

    /**
     * Logical catalog/database name used by TablePath. Persistent DuckDB
     * databases are named after the filename without its extension; in-memory
     * databases use DuckDB's default catalog name "memory".
     */
    public static String databaseName(String url) {
        if (!accepts(url)) {
            return null;
        }
        String database = databaseSpec(url);
        if (database.isEmpty()
                || database.toLowerCase(Locale.ROOT).startsWith(MEMORY_PREFIX)) {
            return "memory";
        }
        if (database.toLowerCase(Locale.ROOT).startsWith(DUCKLAKE_PREFIX)) {
            return "ducklake";
        }

        String normalized = database.replace('\\', '/');
        int slash = normalized.lastIndexOf('/');
        String fileName = slash >= 0
                ? normalized.substring(slash + 1)
                : normalized;
        if (fileName.isEmpty()) {
            return null;
        }
        int dot = fileName.lastIndexOf('.');
        if (dot > 0) {
            fileName = fileName.substring(0, dot);
        }
        return fileName.isEmpty() ? null : fileName;
    }

    /**
     * Resolve the explicit DuckDB schema setting. DuckDB JDBC gives URL
     * options precedence over Properties when the same key is provided.
     */
    public static String configuredSchema(String url, Map<String, String> properties) {
        String urlValue = optionValue(url, "schema");
        if (hasText(urlValue)) {
            return urlValue.trim();
        }
        String property = propertyIgnoreCase(properties, "schema");
        return hasText(property) ? property.trim() : null;
    }

    private static String optionValue(String url, String key) {
        if (!accepts(url)) {
            return null;
        }
        String normalized = url.trim();
        int firstOption = normalized.indexOf(';', PREFIX.length());
        if (firstOption < 0 || firstOption == normalized.length() - 1) {
            return null;
        }
        String options = normalized.substring(firstOption + 1);
        for (String item : options.split(";")) {
            int equals = item.indexOf('=');
            if (equals <= 0) {
                continue;
            }
            String itemKey = item.substring(0, equals).trim();
            if (key.equalsIgnoreCase(itemKey)) {
                String value = item.substring(equals + 1).trim();
                return value.isEmpty() ? null : value;
            }
        }
        return null;
    }

    private static String databaseSpec(String url) {
        if (!accepts(url)) {
            return "";
        }
        String value = url.trim().substring(PREFIX.length());
        int option = value.indexOf(';');
        if (option >= 0) {
            value = value.substring(0, option);
        }
        return value.trim();
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

    private static boolean hasText(String value) {
        return value != null && !value.trim().isEmpty();
    }
}
