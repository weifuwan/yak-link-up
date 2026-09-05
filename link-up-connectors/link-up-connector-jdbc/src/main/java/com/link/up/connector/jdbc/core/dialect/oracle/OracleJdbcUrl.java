package com.link.up.connector.jdbc.core.dialect.oracle;

import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Minimal Oracle thin JDBC URL parser used for table-path normalization.
 *
 * <p>It understands Easy Connect service names, SID-style URLs, TNS aliases
 * and basic DESCRIPTION strings. The extracted value is only a logical
 * database identifier for metadata/reporting; Oracle SQL still addresses
 * objects as schema.table.</p>
 */
public final class OracleJdbcUrl {

    private static final String PREFIX =
            "jdbc:oracle:thin:@";

    private static final Pattern SERVICE_NAME =
            Pattern.compile(
                    "(?i)SERVICE_NAME\\s*=\\s*([^\\)]+)");

    private static final Pattern SID =
            Pattern.compile(
                    "(?i)SID\\s*=\\s*([^\\)]+)");

    private OracleJdbcUrl() {
    }

    public static boolean accepts(String url) {
        return url != null
                && url.trim()
                .toLowerCase(Locale.ROOT)
                .startsWith(PREFIX);
    }

    public static String databaseName(String url) {
        if (!accepts(url)) {
            throw new IllegalArgumentException(
                    "非法 Oracle JDBC URL：" + url);
        }

        String target =
                url.trim().substring(PREFIX.length());

        int query = target.indexOf('?');
        if (query >= 0) {
            target = target.substring(0, query);
        }

        if (target.startsWith("(")) {
            String service = descriptorValue(
                    SERVICE_NAME,
                    target);
            if (hasText(service)) {
                return service.trim();
            }

            String sid = descriptorValue(
                    SID,
                    target);
            if (hasText(sid)) {
                return sid.trim();
            }

            return "oracle";
        }

        if (target.startsWith("//")) {
            int serviceSeparator =
                    target.indexOf('/', 2);

            if (serviceSeparator >= 0
                    && serviceSeparator
                    < target.length() - 1) {
                return target.substring(
                        serviceSeparator + 1)
                        .trim();
            }
        }

        int slash = target.lastIndexOf('/');
        if (slash >= 0
                && slash < target.length() - 1) {
            return target.substring(
                    slash + 1)
                    .trim();
        }

        int colon = target.lastIndexOf(':');
        if (colon >= 0
                && colon < target.length() - 1) {
            return target.substring(
                    colon + 1)
                    .trim();
        }

        return hasText(target)
                ? target.trim()
                : "oracle";
    }

    private static String descriptorValue(
            Pattern pattern,
            String value) {

        Matcher matcher =
                pattern.matcher(value);

        return matcher.find()
                ? matcher.group(1)
                : null;
    }

    private static boolean hasText(
            String value) {

        return value != null
                && !value.trim().isEmpty();
    }
}
