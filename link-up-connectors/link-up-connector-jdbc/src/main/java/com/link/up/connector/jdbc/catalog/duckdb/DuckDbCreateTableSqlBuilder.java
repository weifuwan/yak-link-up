package com.link.up.connector.jdbc.catalog.duckdb;

import com.link.up.api.table.catalog.CatalogTable;
import com.link.up.api.table.catalog.Column;
import com.link.up.api.table.catalog.PrimaryKey;
import com.link.up.api.table.catalog.TablePath;
import com.link.up.api.table.catalog.TableSchema;
import com.link.up.api.table.type.SqlType;
import com.link.up.connector.jdbc.core.dialect.duckdb.DuckDbTypeMapper;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/** DuckDB offline CREATE TABLE builder. */
public final class DuckDbCreateTableSqlBuilder {

    private static final Pattern NUMBER_PATTERN =
            Pattern.compile("[-+]?\\d+(\\.\\d+)?([eE][-+]?\\d+)?");

    private final TablePath tablePath;
    private final CatalogTable catalogTable;
    private final DuckDbTypeMapper typeMapper;

    public DuckDbCreateTableSqlBuilder(
            TablePath tablePath,
            CatalogTable catalogTable,
            DuckDbTypeMapper typeMapper) {
        this.tablePath = tablePath;
        this.catalogTable = catalogTable;
        this.typeMapper = typeMapper;
    }

    public String build() {
        return buildStatements().stream()
                .map(sql -> sql.endsWith(";") ? sql : sql + ";")
                .collect(Collectors.joining("\n"));
    }

    public List<String> buildStatements() {
        List<String> statements = new ArrayList<String>();
        boolean preserveSourceType =
                DuckDbCatalog.DIALECT.equalsIgnoreCase(
                        catalogTable.getOptions().get(
                                DuckDbCatalog.TABLE_OPTION_DIALECT));

        for (Column column : catalogTable.getTableSchema().getColumns()) {
            if (column.isAutoIncrement()) {
                statements.add(buildCreateSequence(column));
            }
        }

        statements.add(buildCreateTable(preserveSourceType));

        if (hasText(catalogTable.getComment())) {
            statements.add(
                    "COMMENT ON TABLE " + quoteTable(tablePath)
                            + " IS '" + escapeLiteral(catalogTable.getComment()) + "'");
        }
        for (Column column : catalogTable.getTableSchema().getColumns()) {
            if (hasText(column.getComment())) {
                statements.add(
                        "COMMENT ON COLUMN " + quoteTable(tablePath) + "."
                                + quoteIdentifier(column.getName())
                                + " IS '" + escapeLiteral(column.getComment()) + "'");
            }
        }
        return Collections.unmodifiableList(statements);
    }

    public String buildColumnDefinition(
            Column column,
            boolean preserveSourceType) {

        List<String> parts = new ArrayList<String>();
        parts.add(quoteIdentifier(column.getName()));
        parts.add(typeMapper.toDatabaseType(column, preserveSourceType));

        if (column.isAutoIncrement()) {
            if (!isInteger(column.getDataType().getSqlType())) {
                throw new IllegalArgumentException(
                        "DuckDB auto-increment 仅支持整数列，column=" + column.getName());
            }
            parts.add("DEFAULT nextval('" + sequenceReference(column) + "')");
        } else if (column.getDefaultValue() != null) {
            parts.add("DEFAULT " + formatDefaultValue(column, preserveSourceType));
        }

        if (!column.isNullable()) {
            parts.add("NOT NULL");
        }
        return String.join(" ", parts);
    }

    public String buildCreateSequence(Column column) {
        return "CREATE SEQUENCE IF NOT EXISTS "
                + quoteIdentifier(tablePath.getSchemaName()) + "."
                + quoteIdentifier(sequenceName(column));
    }

    private String buildCreateTable(boolean preserveSourceType) {
        TableSchema schema = catalogTable.getTableSchema();
        List<String> definitions = new ArrayList<String>();
        for (Column column : schema.getColumns()) {
            definitions.add(buildColumnDefinition(column, preserveSourceType));
        }

        PrimaryKey primaryKey = schema.getPrimaryKey();
        if (primaryKey != null) {
            definitions.add(buildPrimaryKey(primaryKey));
        }

        return "CREATE TABLE " + quoteTable(tablePath)
                + " (\n    "
                + String.join(",\n    ", definitions)
                + "\n)";
    }

    private String buildPrimaryKey(PrimaryKey primaryKey) {
        String columns = primaryKey.getColumnNames().stream()
                .map(DuckDbCreateTableSqlBuilder::quoteIdentifier)
                .collect(Collectors.joining(", "));
        if (!hasText(primaryKey.getName())) {
            return "PRIMARY KEY (" + columns + ")";
        }
        return "CONSTRAINT " + quoteIdentifier(primaryKey.getName())
                + " PRIMARY KEY (" + columns + ")";
    }

    private String formatDefaultValue(Column column, boolean preserveSourceType) {
        Object value = column.getDefaultValue();
        if (preserveSourceType) {
            String expression = String.valueOf(value).trim();
            if (hasText(expression)) {
                return expression;
            }
        }
        if (value instanceof Number) {
            return value.toString();
        }
        if (value instanceof Boolean) {
            return (Boolean) value ? "TRUE" : "FALSE";
        }

        String text = String.valueOf(value).trim();
        String upper = text.toUpperCase(Locale.ROOT);
        if ("NULL".equals(upper)
                || "CURRENT_DATE".equals(upper)
                || "CURRENT_TIME".equals(upper)
                || "CURRENT_TIMESTAMP".equals(upper)
                || "NOW()".equals(upper)
                || "CURRENT_USER".equals(upper)
                || upper.startsWith("NEXTVAL(")) {
            return text;
        }
        if (isNumeric(column.getDataType().getSqlType())
                && NUMBER_PATTERN.matcher(text).matches()) {
            return text;
        }
        return "'" + escapeLiteral(text) + "'";
    }

    private String sequenceReference(Column column) {
        String qualified = quoteIdentifier(tablePath.getSchemaName())
                + "." + quoteIdentifier(sequenceName(column));
        return escapeLiteral(qualified);
    }

    private String sequenceName(Column column) {
        String raw = tablePath.getTableName() + "_" + column.getName() + "_seq";
        String safe = raw.replaceAll("[^A-Za-z0-9_]", "_");
        return safe.length() <= 120 ? safe : safe.substring(0, 120);
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
        if (!hasText(path.getSchemaName())) {
            throw new IllegalArgumentException(
                    "DuckDB CREATE TABLE 需要 schema：" + path);
        }
        return quoteIdentifier(path.getSchemaName())
                + "." + quoteIdentifier(path.getTableName());
    }

    private static String quoteIdentifier(String value) {
        if (!hasText(value)) {
            throw new IllegalArgumentException("identifier must not be empty");
        }
        return "\"" + value.trim().replace("\"", "\"\"") + "\"";
    }

    private static String escapeLiteral(String value) {
        return value.replace("'", "''");
    }

    private static boolean hasText(String value) {
        return value != null && !value.trim().isEmpty();
    }
}
