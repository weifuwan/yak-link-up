package com.link.up.connector.jdbc.catalog.dameng;

import com.link.up.api.table.catalog.CatalogTable;
import com.link.up.api.table.catalog.Column;
import com.link.up.api.table.catalog.PrimaryKey;
import com.link.up.api.table.catalog.TablePath;
import com.link.up.api.table.catalog.TableSchema;
import com.link.up.api.table.type.SqlType;
import com.link.up.connector.jdbc.core.dialect.dameng.DamengTypeMapper;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/** Dameng DM8 offline CREATE TABLE builder. */
public final class DamengCreateTableSqlBuilder {

    private static final Pattern NUMBER_PATTERN =
            Pattern.compile("[-+]?\\d+(\\.\\d+)?");
    private static final Pattern TABLESPACE_PATTERN =
            Pattern.compile("[A-Za-z0-9_$#]+(?:[A-Za-z0-9_$#-]*[A-Za-z0-9_$#])?");

    private final TablePath tablePath;
    private final CatalogTable catalogTable;
    private final DamengTypeMapper typeMapper;

    public DamengCreateTableSqlBuilder(
            TablePath tablePath,
            CatalogTable catalogTable,
            DamengTypeMapper typeMapper) {
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
        PrimaryKey primaryKey = schema.getPrimaryKey();
        Set<String> primaryKeyColumns = primaryKey == null
                ? new HashSet<String>()
                : new HashSet<String>(primaryKey.getColumnNames());
        boolean preserveSourceType = DamengCatalog.DIALECT.equalsIgnoreCase(
                catalogTable.getOptions().get(DamengCatalog.TABLE_OPTION_DIALECT));

        List<String> definitions = new ArrayList<String>();
        for (Column column : schema.getColumns()) {
            definitions.add(buildColumnDefinition(
                    column,
                    preserveSourceType,
                    primaryKeyColumns.contains(column.getName())));
        }
        if (primaryKey != null) {
            definitions.add(buildPrimaryKey(primaryKey));
        }

        StringBuilder create = new StringBuilder()
                .append("CREATE TABLE ")
                .append(quoteTable(tablePath))
                .append(" (\n    ")
                .append(String.join(",\n    ", definitions))
                .append("\n)");
        appendStorageOptions(create);

        List<String> statements = new ArrayList<String>();
        statements.add(create.toString());
        if (hasText(catalogTable.getComment())) {
            statements.add(
                    "COMMENT ON TABLE " + quoteTable(tablePath)
                            + " IS '" + escapeLiteral(catalogTable.getComment()) + "'");
        }
        for (Column column : schema.getColumns()) {
            if (hasText(column.getComment())) {
                statements.add(
                        "COMMENT ON COLUMN " + quoteTable(tablePath) + "."
                                + quoteIdentifier(column.getName())
                                + " IS '" + escapeLiteral(column.getComment()) + "'");
            }
        }
        return statements;
    }

    public String buildColumnDefinition(Column column, boolean preserveSourceType) {
        return buildColumnDefinition(column, preserveSourceType, false);
    }

    private String buildColumnDefinition(
            Column column,
            boolean preserveSourceType,
            boolean primaryKeyColumn) {
        List<String> parts = new ArrayList<String>();
        parts.add(quoteIdentifier(column.getName()));
        parts.add(typeMapper.toDatabaseType(column, preserveSourceType));

        if (column.isAutoIncrement()) {
            validateIdentityColumn(column);
            parts.add("IDENTITY(1,1)");
        } else if (column.getDefaultValue() != null) {
            parts.add("DEFAULT " + formatDefaultValue(column, preserveSourceType));
        }
        if (!column.isNullable() || primaryKeyColumn) {
            parts.add("NOT NULL");
        }
        return String.join(" ", parts);
    }

    private void appendStorageOptions(StringBuilder create) {
        String fillfactor = catalogTable.getOptions().get(DamengCatalog.TABLE_OPTION_FILLFACTOR);
        String tablespace = catalogTable.getOptions().get(DamengCatalog.TABLE_OPTION_TABLESPACE);
        List<String> storage = new ArrayList<String>();
        if (hasText(fillfactor)) {
            int value;
            try {
                value = Integer.parseInt(fillfactor.trim());
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("Dameng fillfactor 必须是 0-100 的整数：" + fillfactor, e);
            }
            if (value < 0 || value > 100) {
                throw new IllegalArgumentException("Dameng fillfactor 必须是 0-100 的整数：" + fillfactor);
            }
            storage.add("FILLFACTOR " + value);
        }
        if (hasText(tablespace)) {
            String value = tablespace.trim();
            if (!TABLESPACE_PATTERN.matcher(value).matches()) {
                throw new IllegalArgumentException("非法 Dameng tablespace：" + tablespace);
            }
            storage.add("ON " + quoteIdentifier(value));
        }
        if (!storage.isEmpty()) {
            create.append("\nSTORAGE (")
                    .append(String.join(", ", storage))
                    .append(")");
        }
    }

    private static void validateIdentityColumn(Column column) {
        SqlType type = column.getDataType().getSqlType();
        boolean supported = type == SqlType.INT || type == SqlType.BIGINT;
        if (type == SqlType.DECIMAL) {
            supported = column.getScale() == null || column.getScale() == 0;
        }
        if (!supported) {
            throw new IllegalArgumentException(
                    "Dameng IDENTITY 仅支持 INT/BIGINT/DECIMAL(scale=0)，column="
                            + column.getName() + "，type=" + type);
        }
    }

    private String buildPrimaryKey(PrimaryKey primaryKey) {
        String columns = primaryKey.getColumnNames().stream()
                .map(DamengCreateTableSqlBuilder::quoteIdentifier)
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
            return (Boolean) value ? "1" : "0";
        }

        String text = String.valueOf(value).trim();
        String upper = text.toUpperCase(Locale.ROOT);
        if ("NULL".equals(upper)
                || upper.startsWith("CURRENT ")
                || upper.startsWith("CURRENT_")
                || upper.startsWith("SYSDATE")
                || upper.startsWith("SYSTIMESTAMP")
                || "USER".equals(upper)) {
            return text;
        }
        if (isNumeric(column.getDataType().getSqlType())
                && NUMBER_PATTERN.matcher(text).matches()) {
            return text;
        }
        return "'" + escapeLiteral(text) + "'";
    }

    private static boolean isNumeric(SqlType type) {
        return type == SqlType.TINYINT
                || type == SqlType.SMALLINT
                || type == SqlType.INT
                || type == SqlType.BIGINT
                || type == SqlType.FLOAT
                || type == SqlType.DOUBLE
                || type == SqlType.DECIMAL;
    }

    private static String quoteTable(TablePath path) {
        if (!hasText(path.getSchemaName())) {
            throw new IllegalArgumentException("Dameng CREATE TABLE 需要 schema：" + path);
        }
        return quoteIdentifier(path.getSchemaName())
                + "."
                + quoteIdentifier(path.getTableName());
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
