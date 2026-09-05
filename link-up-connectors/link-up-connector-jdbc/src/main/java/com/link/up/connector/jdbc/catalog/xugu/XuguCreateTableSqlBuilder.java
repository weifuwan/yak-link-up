package com.link.up.connector.jdbc.catalog.xugu;

import com.link.up.api.table.catalog.CatalogTable;
import com.link.up.api.table.catalog.Column;
import com.link.up.api.table.catalog.PrimaryKey;
import com.link.up.api.table.catalog.TablePath;
import com.link.up.api.table.catalog.TableSchema;
import com.link.up.api.table.type.SqlType;
import com.link.up.connector.jdbc.core.dialect.xugu.XuguTypeMapper;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/** Builds XuguDB CREATE TABLE statements for offline sink preparation. */
public final class XuguCreateTableSqlBuilder {

    private static final int MAX_IDENTIFIER_BYTES = 127;
    private static final Pattern NUMBER_PATTERN =
            Pattern.compile("[-+]?\\d+(\\.\\d+)?");

    private final TablePath tablePath;
    private final CatalogTable catalogTable;
    private final XuguTypeMapper typeMapper;

    public XuguCreateTableSqlBuilder(
            TablePath tablePath,
            CatalogTable catalogTable,
            XuguTypeMapper typeMapper) {
        this.tablePath = tablePath;
        this.catalogTable = catalogTable;
        this.typeMapper = typeMapper;
    }

    public String build() {
        return String.join("\n", buildStatements());
    }

    public List<String> buildStatements() {
        List<String> statements = new ArrayList<String>();
        statements.add(buildCreateTable());
        if (hasText(catalogTable.getComment())) {
            statements.add(
                    "COMMENT ON TABLE " + quoteTable(tablePath)
                            + " IS '" + literal(catalogTable.getComment()) + "';");
        }
        for (Column column : catalogTable.getTableSchema().getColumns()) {
            if (hasText(column.getComment())) {
                statements.add(
                        "COMMENT ON COLUMN " + quoteTable(tablePath) + "."
                                + quote(column.getName()) + " IS '"
                                + literal(column.getComment()) + "';");
            }
        }
        return Collections.unmodifiableList(statements);
    }

    public String buildColumnDefinition(
            Column column,
            boolean preserveSourceType) {

        List<String> parts = new ArrayList<String>();
        parts.add(quote(column.getName()));
        boolean preserve = preserveSourceType && shouldPreserveSourceType(column);
        parts.add(typeMapper.toDatabaseType(column, preserve));

        if (column.isAutoIncrement()
                && isInteger(column.getDataType().getSqlType())) {
            parts.add("IDENTITY(1,1)");
        }
        if (!column.isNullable()) {
            parts.add("NOT NULL");
        }
        if (!column.isAutoIncrement()
                && column.getDefaultValue() != null) {
            parts.add("DEFAULT "
                    + formatDefaultValue(column, preserve));
        }
        return String.join(" ", parts);
    }

    private String buildCreateTable() {
        TableSchema schema = catalogTable.getTableSchema();
        boolean preserveSourceType = XuguCatalog.DIALECT.equalsIgnoreCase(
                catalogTable.getOptions().get(XuguCatalog.TABLE_OPTION_DIALECT));
        List<String> definitions = new ArrayList<String>();
        for (Column column : schema.getColumns()) {
            definitions.add(buildColumnDefinition(column, preserveSourceType));
        }
        PrimaryKey primaryKey = schema.getPrimaryKey();
        if (primaryKey != null) {
            definitions.add(buildPrimaryKey(primaryKey));
        }
        return "CREATE TABLE " + quoteTable(tablePath)
                + " (\n    " + String.join(",\n    ", definitions) + "\n);";
    }

    private static boolean shouldPreserveSourceType(Column column) {
        String sourceType = column.getSourceType();
        if (!hasText(sourceType)) {
            return false;
        }
        String normalized = sourceType.trim()
                .toLowerCase(Locale.ROOT)
                .replaceAll("\\s+", " ");
        int parenthesis = normalized.indexOf('(');
        String base = parenthesis >= 0
                ? normalized.substring(0, parenthesis).trim()
                : normalized;
        if (normalized.endsWith("[]")) {
            return false;
        }
        return !"array".equals(base)
                && !"rowid".equals(base)
                && !"point".equals(base)
                && !"line".equals(base)
                && !"lseg".equals(base)
                && !"box".equals(base)
                && !"path".equals(base)
                && !"polygon".equals(base)
                && !"circle".equals(base)
                && !"interval".equals(base)
                && !base.startsWith("interval ");
    }

    private static String buildPrimaryKey(PrimaryKey primaryKey) {
        String columns = primaryKey.getColumnNames().stream()
                .map(XuguCreateTableSqlBuilder::quote)
                .collect(Collectors.joining(", "));
        if (hasText(primaryKey.getName())) {
            return "CONSTRAINT " + quote(shortIdentifier(primaryKey.getName()))
                    + " PRIMARY KEY (" + columns + ")";
        }
        return "PRIMARY KEY (" + columns + ")";
    }

    private static String formatDefaultValue(
            Column column,
            boolean preserveSourceType) {

        Object value = column.getDefaultValue();
        if (value instanceof Number) {
            return value.toString();
        }
        if (value instanceof Boolean) {
            return (Boolean) value ? "TRUE" : "FALSE";
        }
        String text = String.valueOf(value).trim();
        if (preserveSourceType) {
            return text;
        }
        String upper = text.toUpperCase(Locale.ROOT);
        if ("NULL".equals(upper)
                || "CURRENT_TIMESTAMP".equals(upper)
                || "CURRENT_DATE".equals(upper)
                || "CURRENT_TIME".equals(upper)
                || "SYSDATE".equals(upper)
                || "NOW()".equals(upper)
                || upper.startsWith("CURRENT_TIMESTAMP(")) {
            return text;
        }
        if (isNumeric(column.getDataType().getSqlType())
                && NUMBER_PATTERN.matcher(text).matches()) {
            return text;
        }
        return "'" + literal(text) + "'";
    }

    private static String shortIdentifier(String value) {
        String normalized = value.trim();
        if (normalized.getBytes(StandardCharsets.UTF_8).length
                <= MAX_IDENTIFIER_BYTES) {
            return normalized;
        }

        String hash = Integer.toHexString(normalized.hashCode());
        while (hash.length() < 8) {
            hash = "0" + hash;
        }
        if (hash.length() > 8) {
            hash = hash.substring(hash.length() - 8);
        }

        // Reserve one byte for '_' and eight ASCII bytes for the stable hash.
        int byteBudget = MAX_IDENTIFIER_BYTES - 9;
        int usedBytes = 0;
        StringBuilder prefix = new StringBuilder();
        for (int offset = 0; offset < normalized.length();) {
            int codePoint = normalized.codePointAt(offset);
            String part = new String(Character.toChars(codePoint));
            int partBytes = part.getBytes(StandardCharsets.UTF_8).length;
            if (usedBytes + partBytes > byteBudget) {
                break;
            }
            prefix.append(part);
            usedBytes += partBytes;
            offset += Character.charCount(codePoint);
        }
        return prefix.toString() + "_" + hash;
    }

    private static boolean isInteger(SqlType type) {
        return type == SqlType.TINYINT
                || type == SqlType.SMALLINT
                || type == SqlType.INT
                || type == SqlType.BIGINT;
    }

    private static boolean isNumeric(SqlType type) {
        return isInteger(type)
                || type == SqlType.FLOAT
                || type == SqlType.DOUBLE
                || type == SqlType.DECIMAL;
    }

    private static String quoteTable(TablePath path) {
        if (path == null || !hasText(path.getSchemaName())) {
            throw new IllegalArgumentException(
                    "XuguDB tablePath must contain schema: " + path);
        }
        return quote(path.getSchemaName()) + "." + quote(path.getTableName());
    }

    private static String quote(String value) {
        if (!hasText(value)) {
            throw new IllegalArgumentException("identifier must not be empty");
        }
        return "\"" + value.trim().replace("\"", "\"\"") + "\"";
    }

    private static String literal(String value) {
        return value.replace("'", "''");
    }

    private static boolean hasText(String value) {
        return value != null && !value.trim().isEmpty();
    }
}
