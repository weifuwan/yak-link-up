package com.link.up.connector.jdbc.core.dialect.sqlserver;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/** Minimal parser for Microsoft SQL Server JDBC URLs. */
public final class SqlServerJdbcUrl {

    private static final String PREFIX = "jdbc:sqlserver://";

    private SqlServerJdbcUrl() {
    }

    public static boolean accepts(String url) {
        return url != null
                && url.trim().toLowerCase(Locale.ROOT).startsWith(PREFIX);
    }

    /** Returns databaseName/database from the URL, or null when the driver default is used. */
    public static String databaseName(String url) {
        Parsed parsed = parse(url);
        for (String property : parsed.properties) {
            int separator = property.indexOf('=');
            if (separator <= 0) {
                continue;
            }
            String key = property.substring(0, separator).trim();
            if (!"databasename".equalsIgnoreCase(key)
                    && !"database".equalsIgnoreCase(key)) {
                continue;
            }
            String value = property.substring(separator + 1).trim();
            return unescape(value);
        }
        return null;
    }

    /** Replaces or appends databaseName while preserving the other URL properties. */
    public static String withDatabase(String url, String databaseName) {
        if (!hasText(databaseName)) {
            throw new IllegalArgumentException("databaseName must not be empty");
        }
        Parsed parsed = parse(url);
        List<String> properties = new ArrayList<String>(parsed.properties.size() + 1);
        boolean replaced = false;
        for (String property : parsed.properties) {
            int separator = property.indexOf('=');
            if (separator > 0) {
                String key = property.substring(0, separator).trim();
                if ("databasename".equalsIgnoreCase(key)
                        || "database".equalsIgnoreCase(key)) {
                    if (!replaced) {
                        properties.add("databaseName=" + escape(databaseName.trim()));
                        replaced = true;
                    }
                    continue;
                }
            }
            if (hasText(property)) {
                properties.add(property);
            }
        }
        if (!replaced) {
            properties.add("databaseName=" + escape(databaseName.trim()));
        }
        StringBuilder result = new StringBuilder(parsed.serverPart);
        for (String property : properties) {
            result.append(';').append(property);
        }
        return result.toString();
    }

    private static Parsed parse(String url) {
        if (!accepts(url)) {
            throw new IllegalArgumentException("非法 SQL Server JDBC URL：" + url);
        }
        String normalized = url.trim();
        int propertyStart = findPropertyStart(normalized);
        if (propertyStart < 0) {
            return new Parsed(normalized, new ArrayList<String>());
        }
        return new Parsed(
                normalized.substring(0, propertyStart),
                splitProperties(normalized.substring(propertyStart + 1)));
    }

    private static int findPropertyStart(String value) {
        int braces = 0;
        for (int i = PREFIX.length(); i < value.length(); i++) {
            char c = value.charAt(i);
            if (c == '{') {
                braces++;
            } else if (c == '}' && braces > 0) {
                braces--;
            } else if (c == ';' && braces == 0) {
                return i;
            }
        }
        return -1;
    }

    private static List<String> splitProperties(String value) {
        List<String> result = new ArrayList<String>();
        StringBuilder current = new StringBuilder();
        int braces = 0;
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            if (c == '{') {
                braces++;
                current.append(c);
            } else if (c == '}' && braces > 0) {
                braces--;
                current.append(c);
            } else if (c == ';' && braces == 0) {
                result.add(current.toString().trim());
                current.setLength(0);
            } else {
                current.append(c);
            }
        }
        if (current.length() > 0) {
            result.add(current.toString().trim());
        }
        return result;
    }

    private static String escape(String value) {
        if (value.indexOf(';') < 0
                && value.indexOf('=') < 0
                && value.indexOf('{') < 0
                && value.indexOf('}') < 0) {
            return value;
        }
        return "{" + value.replace("}", "}}") + "}";
    }

    private static String unescape(String value) {
        if (value.length() >= 2 && value.charAt(0) == '{'
                && value.charAt(value.length() - 1) == '}') {
            return value.substring(1, value.length() - 1).replace("}}", "}").trim();
        }
        return value.trim();
    }

    private static boolean hasText(String value) {
        return value != null && !value.trim().isEmpty();
    }

    private static final class Parsed {
        private final String serverPart;
        private final List<String> properties;

        private Parsed(String serverPart, List<String> properties) {
            this.serverPart = serverPart;
            this.properties = properties;
        }
    }
}
