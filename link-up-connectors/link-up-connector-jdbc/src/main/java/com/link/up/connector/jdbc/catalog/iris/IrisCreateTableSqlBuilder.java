package com.link.up.connector.jdbc.catalog.iris;

import com.link.up.api.table.catalog.CatalogTable;
import com.link.up.api.table.catalog.Column;
import com.link.up.api.table.catalog.PrimaryKey;
import com.link.up.api.table.catalog.TablePath;
import com.link.up.api.table.catalog.TableSchema;
import com.link.up.api.table.type.SqlType;
import com.link.up.connector.jdbc.core.dialect.iris.IrisTypeMapper;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/** Builds IRIS CREATE TABLE statements for offline sink preparation. */
public final class IrisCreateTableSqlBuilder {

    private static final Pattern NUMBER_PATTERN =
            Pattern.compile("[-+]?\\d+(\\.\\d+)?");

    private final TablePath tablePath;
    private final CatalogTable catalogTable;
    private final IrisTypeMapper typeMapper;

    public IrisCreateTableSqlBuilder(
            TablePath tablePath,
            CatalogTable catalogTable,
            IrisTypeMapper typeMapper) {

        this.tablePath = tablePath;
        this.catalogTable = catalogTable;
        this.typeMapper = typeMapper;
    }

    public String build() {
        return String.join("\n", buildStatements());
    }

    public List<String> buildStatements() {
        return Collections.singletonList(buildCreateTable());
    }

    public String buildColumnDefinition(
            Column column,
            boolean preserveSourceType) {

        List<String> parts = new ArrayList<String>();
        parts.add(quote(column.getName()));

        if (column.isAutoIncrement()
                && isInteger(column.getDataType().getSqlType())) {
            // SERIAL permits source IDs to be inserted explicitly, unlike
            // IDENTITY, while still maintaining an automatic counter.
            parts.add("SERIAL");
        } else {
            parts.add(typeMapper.toDatabaseType(column, preserveSourceType));
        }

        if (hasText(column.getComment())) {
            parts.add("%DESCRIPTION '" + literal(column.getComment()) + "'");
        }

        parts.add(column.isNullable() ? "NULL" : "NOT NULL");

        if (!column.isAutoIncrement()
                && column.getDefaultValue() != null) {
            parts.add("DEFAULT " + formatDefaultValue(column, preserveSourceType));
        }
        return String.join(" ", parts);
    }

    private String buildCreateTable() {
        TableSchema schema = catalogTable.getTableSchema();
        List<String> definitions = new ArrayList<String>();

        if (hasText(catalogTable.getComment())) {
            definitions.add(
                    "%DESCRIPTION '" + literal(catalogTable.getComment()) + "'");
        }

        boolean preserveSourceType = IrisCatalog.DIALECT.equalsIgnoreCase(
                catalogTable.getOptions().get(IrisCatalog.TABLE_OPTION_DIALECT));

        for (Column column : schema.getColumns()) {
            definitions.add(buildColumnDefinition(column, preserveSourceType));
        }

        PrimaryKey primaryKey = schema.getPrimaryKey();
        if (primaryKey != null) {
            definitions.add(buildPrimaryKey(primaryKey));
        }

        return "CREATE TABLE "
                + quoteTable(tablePath)
                + " (\n    "
                + String.join(",\n    ", definitions)
                + "\n);";
    }

    private static String buildPrimaryKey(PrimaryKey primaryKey) {
        String columns = primaryKey.getColumnNames().stream()
                .map(IrisCreateTableSqlBuilder::quote)
                .collect(Collectors.joining(", "));
        if (hasText(primaryKey.getName())) {
            return "CONSTRAINT " + quote(primaryKey.getName())
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
            return (Boolean) value ? "1" : "0";
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
                || upper.startsWith("CURRENT_TIMESTAMP(")) {
            return text;
        }
        if (isNumeric(column.getDataType().getSqlType())
                && NUMBER_PATTERN.matcher(text).matches()) {
            return text;
        }
        return "'" + literal(text) + "'";
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
                    "IRIS tablePath must contain schema: " + path);
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
