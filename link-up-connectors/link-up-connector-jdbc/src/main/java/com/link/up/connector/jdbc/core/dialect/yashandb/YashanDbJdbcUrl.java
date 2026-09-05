package com.link.up.connector.jdbc.core.dialect.yashandb;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.Locale;

/** Utilities for YashanDB network JDBC URLs. */
public final class YashanDbJdbcUrl {

    private static final String PREFIX = "jdbc:yasdb://";

    private YashanDbJdbcUrl() {
    }

    public static boolean accepts(String url) {
        return url != null
                && url.trim().toLowerCase(Locale.ROOT).startsWith(PREFIX);
    }

    /**
     * Returns the required database_name path token from the JDBC URL.
     *
     * <p>YashanDB documents this value as a compatibility parameter without
     * database-switching semantics. Yak Link Up therefore uses it only as a
     * logical catalog identifier; SQL objects are always addressed as
     * schema.table.</p>
     */
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

    private static String decode(String value) {
        try {
            return URLDecoder.decode(value, "UTF-8");
        } catch (UnsupportedEncodingException impossible) {
            throw new IllegalStateException("UTF-8 is not supported", impossible);
        } catch (IllegalArgumentException invalidEncoding) {
            return value;
        }
    }
}
