package com.link.up.connector.jdbc.core.dialect.iris;

import java.util.Locale;

/** Utilities for InterSystems IRIS JDBC URL semantics. */
public final class IrisJdbcUrl {

    private static final String PREFIX = "jdbc:iris://";

    private IrisJdbcUrl() {
    }

    public static boolean accepts(String url) {
        return url != null
                && url.trim().toLowerCase(Locale.ROOT).startsWith(PREFIX);
    }

    /**
     * Returns the IRIS namespace selected by the JDBC URL.
     *
     * <p>IRIS uses jdbc:IRIS://host:port/namespace. Optional driver arguments
     * may follow the namespace after another slash and are not part of the
     * logical namespace/catalog name.</p>
     */
    public static String namespaceName(String url) {
        if (!accepts(url)) {
            return null;
        }
        String value = url.trim();
        int start = PREFIX.length();
        int firstSlash = value.indexOf('/', start);
        if (firstSlash < 0 || firstSlash == value.length() - 1) {
            return null;
        }
        int end = value.indexOf('/', firstSlash + 1);
        String namespace = end < 0
                ? value.substring(firstSlash + 1)
                : value.substring(firstSlash + 1, end);
        namespace = namespace.trim();
        return namespace.isEmpty() ? null : namespace;
    }
}
