package com.link.up.connector.jdbc.catalog.sqlserver;

import com.link.up.api.table.catalog.CatalogTable;
import com.link.up.api.table.catalog.Column;
import com.link.up.api.table.catalog.PrimaryKey;
import com.link.up.api.table.catalog.TablePath;
import com.link.up.api.table.catalog.TableSchema;
import com.link.up.api.table.type.SqlType;
import com.link.up.connector.jdbc.core.dialect.sqlserver.SqlServerTypeMapper;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/** SQL Server CREATE TABLE builder for offline Sink preparation. */
public final class SqlServerCreateTableSqlBuilder {

    private static final Pattern NUMBER_PATTERN =
            Pattern.compile("[-+]?\\d+(\\.\\d+)?");

    private final TablePath tablePath;
    private final CatalogTable catalogTable;
    private final SqlServerTypeMapper typeMapper;

    public SqlServerCreateTableSqlBuilder(
            TablePath tablePath,
            CatalogTable catalogTable,
            SqlServerTypeMapper typeMapper) {
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
        TableSchema schema = catalogTable.getTableSchema();
        boolean preserveSourceType = SqlServerCatalog.DIALECT.equalsIgnoreCase(
                catalogTable.getOptions().get(SqlServerCatalog.TABLE_OPTION_DIALECT));
        List<String> definitions = new ArrayList<String>();
        for (Column column : schema.getColumns()) {
            definitions.add(buildColumnDefinition(column, preserveSourceType));
        }
        PrimaryKey primaryKey = schema.getPrimaryKey();
        if (primaryKey != null) {
            definitions.add(buildPrimaryKey(primaryKey));
        }

        List<String> statements = new ArrayList<String>();
        statements.add("CREATE TABLE " + quoteTable(tablePath) + " (\n    "
                + String.join(",\n    ", definitions) + "\n)");
        if (hasText(catalogTable.getComment())) {
            statements.add(tableComment(catalogTable.getComment()));
        }
        for (Column column : schema.getColumns()) {
            if (hasText(column.getComment())) {
                statements.add(columnComment(column));
            }
        }
        return statements;
    }

    public String buildColumnDefinition(Column column, boolean preserveSourceType) {
        List<String> parts = new ArrayList<String>();
        parts.add(quoteIdentifier(column.getName()));
        parts.add(typeMapper.toDatabaseType(column, preserveSourceType));
        if (column.isAutoIncrement()) {
            parts.add("IDENTITY(1,1)");
        } else if (column.getDefaultValue() != null) {
            parts.add("DEFAULT " + formatDefaultValue(column, preserveSourceType));
        }
        parts.add(column.isNullable() ? "NULL" : "NOT NULL");
        return String.join(" ", parts);
    }

    public String buildColumnCommentStatement(Column column) {
        return columnComment(column);
    }

    private String buildPrimaryKey(PrimaryKey primaryKey) {
        String name = primaryKey.getName();
        if (!hasText(name)) {
            name = "PK_" + tablePath.getTableName();
        }
        name = normalizeConstraintName(name);
        String columns = primaryKey.getColumnNames().stream()
                .map(SqlServerCreateTableSqlBuilder::quoteIdentifier)
                .collect(Collectors.joining(", "));
        return "CONSTRAINT " + quoteIdentifier(name) + " PRIMARY KEY (" + columns + ")";
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
            return (Boolean) value ? "1" : "0";
        }
        String text = String.valueOf(value).trim();
        String upper = text.toUpperCase(Locale.ROOT);
        if ("NULL".equals(upper)
                || "CURRENT_TIMESTAMP".equals(upper)
                || "GETDATE()".equals(upper)
                || "SYSDATETIME()".equals(upper)
                || "SYSUTCDATETIME()".equals(upper)
                || "NEWID()".equals(upper)
                || "NEWSEQUENTIALID()".equals(upper)) {
            return text;
        }
        SqlType sqlType = column.getDataType().getSqlType();
        if (isNumeric(sqlType) && NUMBER_PATTERN.matcher(text).matches()) {
            return text;
        }
        return "N'" + literal(text) + "'";
    }

    private String tableComment(String comment) {
        return "EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'"
                + literal(comment)
                + "', @level0type=N'SCHEMA', @level0name=N'" + literal(schema())
                + "', @level1type=N'TABLE', @level1name=N'" + literal(tablePath.getTableName()) + "'";
    }

    private String columnComment(Column column) {
        return "EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'"
                + literal(column.getComment())
                + "', @level0type=N'SCHEMA', @level0name=N'" + literal(schema())
                + "', @level1type=N'TABLE', @level1name=N'" + literal(tablePath.getTableName())
                + "', @level2type=N'COLUMN', @level2name=N'" + literal(column.getName()) + "'";
    }

    private String schema() {
        return hasText(tablePath.getSchemaName()) ? tablePath.getSchemaName() : "dbo";
    }

    private static boolean isNumeric(SqlType sqlType) {
        return sqlType == SqlType.TINYINT
                || sqlType == SqlType.SMALLINT
                || sqlType == SqlType.INT
                || sqlType == SqlType.BIGINT
                || sqlType == SqlType.FLOAT
                || sqlType == SqlType.DOUBLE
                || sqlType == SqlType.DECIMAL;
    }

    private static String normalizeConstraintName(String value) {
        String normalized = value.trim().replaceAll("[^A-Za-z0-9_@$#]", "_");
        return normalized.length() > 128 ? normalized.substring(0, 128) : normalized;
    }

    private static String quoteTable(TablePath path) {
        String table = quoteIdentifier(path.getTableName());
        String schema = hasText(path.getSchemaName())
                ? quoteIdentifier(path.getSchemaName()) + "." : "";
        String database = hasText(path.getDatabaseName())
                ? quoteIdentifier(path.getDatabaseName()) + "." : "";
        return database + schema + table;
    }

    private static String quoteIdentifier(String value) {
        if (!hasText(value)) {
            throw new IllegalArgumentException("identifier must not be empty");
        }
        return "[" + value.trim().replace("]", "]]" ) + "]";
    }

    private static String literal(String value) {
        return value.replace("'", "''");
    }

    private static boolean hasText(String value) {
        return value != null && !value.trim().isEmpty();
    }
}
