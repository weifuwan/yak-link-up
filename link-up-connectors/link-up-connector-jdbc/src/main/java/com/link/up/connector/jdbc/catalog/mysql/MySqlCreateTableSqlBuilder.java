package com.link.up.connector.jdbc.catalog.mysql;

import com.link.up.api.table.catalog.*;
import com.link.up.api.table.type.SqlType;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * MySQL CREATE TABLE SQL 构造器。
 * <p>
 * 第一版只生成：
 * <p>
 * 1. 字段；
 * 2. 非空约束；
 * 3. 默认值；
 * 4. 自增；
 * 5. 字段注释；
 * 6. 主键；
 * 7. 引擎、字符集、排序规则、表注释。
 */
public final class MySqlCreateTableSqlBuilder {

    private static final Pattern NUMBER_PATTERN =
            Pattern.compile(
                    "[-+]?\\d+(\\.\\d+)?");

    private final TablePath tablePath;
    private final CatalogTable catalogTable;
    private final MySqlTypeMapper typeMapper;

    public MySqlCreateTableSqlBuilder(
            TablePath tablePath,
            CatalogTable catalogTable,
            MySqlTypeMapper typeMapper) {

        this.tablePath = tablePath;
        this.catalogTable = catalogTable;
        this.typeMapper = typeMapper;
    }

    private static boolean isNumeric(
            SqlType sqlType) {

        return sqlType == SqlType.TINYINT
                || sqlType == SqlType.SMALLINT
                || sqlType == SqlType.INT
                || sqlType == SqlType.BIGINT
                || sqlType == SqlType.FLOAT
                || sqlType == SqlType.DOUBLE
                || sqlType == SqlType.DECIMAL;
    }

    private static String quoteTable(
            TablePath tablePath) {

        return quoteIdentifier(
                tablePath.getDatabaseName())
                + "."
                + quoteIdentifier(
                tablePath.getTableName());
    }

    private static String quoteIdentifier(
            String value) {

        return "`"
                + value.replace("`", "``")
                + "`";
    }

    private static String escapeString(
            String value) {

        return value
                .replace("\\", "\\\\")
                .replace("'", "''");
    }

    private static String getOrDefault(
            Map<String, String> options,
            String key,
            String defaultValue) {

        String value = options.get(key);

        return hasText(value)
                ? value
                : defaultValue;
    }

    private static boolean hasText(
            String value) {

        return value != null
                && !value.trim().isEmpty();
    }

    public String build() {
        TableSchema schema =
                catalogTable.getTableSchema();

        List<String> definitions =
                new ArrayList<>();

        boolean preserveSourceType =
                "mysql".equalsIgnoreCase(
                        catalogTable.getOptions()
                                .get("dialect"));

        for (Column column :
                schema.getColumns()) {

            definitions.add(
                    buildColumn(
                            column,
                            preserveSourceType));
        }

        PrimaryKey primaryKey =
                schema.getPrimaryKey();

        if (primaryKey != null) {
            definitions.add(
                    buildPrimaryKey(primaryKey));
        }

        StringBuilder sql =
                new StringBuilder();

        sql.append("CREATE TABLE ")
                .append(quoteTable(tablePath))
                .append(" (\n    ")
                .append(
                        String.join(
                                ",\n    ",
                                definitions))
                .append("\n)");

        Map<String, String> options =
                catalogTable.getOptions();

        String engine =
                getOrDefault(
                        options,
                        MySqlCatalog.TABLE_OPTION_ENGINE,
                        "InnoDB");

        String charset =
                getOrDefault(
                        options,
                        MySqlCatalog.TABLE_OPTION_CHARSET,
                        "utf8mb4");

        String collate =
                options.get(
                        MySqlCatalog.TABLE_OPTION_COLLATE);

        sql.append(" ENGINE=")
                .append(engine);

        sql.append(" DEFAULT CHARSET=")
                .append(charset);

        if (hasText(collate)) {
            sql.append(" COLLATE=")
                    .append(collate);
        }

        if (hasText(catalogTable.getComment())) {
            sql.append(" COMMENT='")
                    .append(
                            escapeString(
                                    catalogTable.getComment()))
                    .append('\'');
        }

        return sql.append(';')
                .toString();
    }

    private String buildColumn(
            Column column,
            boolean preserveSourceType) {

        List<String> definitions =
                new ArrayList<>();

        definitions.add(
                quoteIdentifier(
                        column.getName()));

        definitions.add(
                typeMapper.toMySqlType(
                        column,
                        preserveSourceType));

        String generated = column.getAttributes().get("generated");

        if (hasText(generated)) {
            definitions.add("GENERATED ALWAYS AS (" + generated + ")");
            String storage = column.getAttributes().get("generated_storage");
            definitions.add("STORED".equalsIgnoreCase(storage) ? "STORED" : "VIRTUAL");
        } else {
            definitions.add(column.isNullable() ? "NULL" : "NOT NULL");
        }

        if (!hasText(generated)
                && !column.isAutoIncrement()
                && column.getDefaultValue() != null) {

            definitions.add(
                    "DEFAULT "
                            + formatDefaultValue(
                            column));
        }

        if (!hasText(generated) && column.isAutoIncrement()) {
            definitions.add(
                    "AUTO_INCREMENT");
        }

        String extra =
                column.getAttributes()
                        .get("extra");

        if (hasText(extra)
                && extra
                .toLowerCase(Locale.ROOT)
                .contains("on update")) {

            int updateIndex =
                    extra.toLowerCase(Locale.ROOT)
                            .indexOf("on update");

            definitions.add(
                    extra.substring(updateIndex));
        }

        if (hasText(column.getComment())) {
            definitions.add(
                    "COMMENT '"
                            + escapeString(
                            column.getComment())
                            + "'");
        }

        return String.join(
                " ",
                definitions);
    }

    /**
     * Builds a reusable escaped column definition for ALTER TABLE ADD COLUMN.
     */
    public String buildColumnDefinition(Column column) {
        boolean preserveSourceType = "mysql".equalsIgnoreCase(
                catalogTable.getOptions().get("dialect"));
        return buildColumn(column, preserveSourceType);
    }

    private String buildPrimaryKey(
            PrimaryKey primaryKey) {

        String columns =
                primaryKey.getColumnNames()
                        .stream()
                        .map(
                                MySqlCreateTableSqlBuilder
                                        ::quoteIdentifier)
                        .collect(
                                Collectors.joining(", "));

        return "PRIMARY KEY (" + columns + ")";
    }

    private String formatDefaultValue(
            Column column) {

        Object value =
                column.getDefaultValue();

        if (value instanceof Number) {
            return value.toString();
        }

        if (value instanceof Boolean) {
            return (Boolean) value
                    ? "1"
                    : "0";
        }

        String text =
                String.valueOf(value).trim();

        String upper =
                text.toUpperCase(Locale.ROOT);

        if ("NULL".equals(upper)
                || "CURRENT_TIMESTAMP".equals(upper)
                || upper.startsWith(
                "CURRENT_TIMESTAMP(")
                || "CURRENT_DATE".equals(upper)
                || "CURRENT_TIME".equals(upper)) {

            return text;
        }

        /*
         * MySQL 8.0.13+ 要求表达式和函数调用
         * 形式的默认值必须用括号包裹：
         *   - curdate()              → (curdate())
         *   - now()                  → (now())
         *   - (curdate() + interval 1 day) → 已有括号，原样输出
         *
         * INFORMATION_SCHEMA 中函数调用不带外层
         * 括号，必须手动补上，否则 MySQL 报语法错误。
         */
        if (isSqlFunctionOrDefaultExpression(text)) {
            if (text.charAt(0) == '(') {
                return text;
            }
            return "(" + text + ")";
        }

        SqlType sqlType =
                column.getDataType()
                        .getSqlType();

        if (isNumeric(sqlType)
                && NUMBER_PATTERN
                .matcher(text)
                .matches()) {

            return text;
        }

        return "'"
                + escapeString(text)
                + "'";
    }

    /**
     * 判断默认值字符串是否为 SQL 函数调用
     * 或表达式，而非普通字面量。
     *
     * <p>覆盖场景：
     * <ul>
     *   <li>{@code curdate()} — 函数调用</li>
     *   <li>{@code now()} — 函数调用</li>
     *   <li>{@code (curdate() + interval 1 day)} — 表达式</li>
     *   <li>{@code (uuid())} — 括号包裹的函数调用</li>
     * </ul>
     */
    private static boolean isSqlFunctionOrDefaultExpression(
            String value) {

        if (value.isEmpty()) {
            return false;
        }

        /*
         * MySQL 8.0+ 表达式默认值以 '(' 开头，
         * 例如 (curdate() + interval 1 day)。
         */
        if (value.charAt(0) == '(') {
            return true;
        }

        /*
         * 包含 '(' 的默认值视为函数调用，
         * 例如 curdate()、now()、uuid()。
         */
        return value.contains("(");
    }
}
