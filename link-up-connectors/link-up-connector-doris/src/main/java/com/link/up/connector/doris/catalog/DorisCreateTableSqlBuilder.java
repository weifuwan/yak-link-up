package com.link.up.connector.doris.catalog;

import com.link.up.api.table.catalog.*;
import com.link.up.api.table.type.SqlType;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Doris CREATE TABLE SQL 构造器。
 *
 * <p>生成 Doris OLAP 引擎建表语句，包含：
 *
 * <ol>
 *   <li>字段定义（类型、非空、默认值）；
 *   <li>主键（UNIQUE KEY）或首字段（DUPLICATE KEY）；
 *   <li>DISTRIBUTED BY HASH；
 *   <li>PROPERTIES（replication_allocation 等）；
 *   <li>表注释。
 * </ol>
 */
public final class DorisCreateTableSqlBuilder {

    /**
     * 表选项：Doris 表模型类型。
     */
    public static final String TABLE_OPTION_KEY_TYPE =
            "doris.key-type";

    /**
     * 表选项：副本分配。
     */
    public static final String TABLE_OPTION_REPLICATION =
            "doris.replication-allocation";

    /**
     * 表选项前缀：AGGREGATE KEY 模型下各 Value 列的聚合函数。
     *
     * <p>例如 {@code doris.aggregate.fn.order_amount=SUM}，
     * 建表时该列将生成 {@code order_amount BIGINT SUM}。
     *
     * <p>支持的聚合函数：SUM, MAX, MIN, REPLACE, REPLACE_IF_NOT_NULL, HLL_UNION, BITMAP_UNION。
     */
    public static final String TABLE_OPTION_AGG_FN_PREFIX =
            "doris.aggregate.fn.";

    /**
     * 表选项前缀：透传到 Doris CREATE TABLE PROPERTIES 中的额外属性。
     *
     * <p>例如 {@code doris.property.dynamic_partition.enable=true}，
     * 建表时将在 PROPERTIES 中添加 {@code "dynamic_partition.enable" = "true"}。
     */
    public static final String TABLE_OPTION_PROPERTY_PREFIX =
            "doris.property.";

    private static final Pattern NUMBER_PATTERN =
            Pattern.compile(
                    "[-+]?\\d+(\\.\\d+)?");

    private final TablePath tablePath;
    private final CatalogTable catalogTable;
    private final DorisTypeMapper typeMapper;
    private final int buckets;
    private final String resolvedKeyType;
    private final List<String> resolvedKeyColumns;

    public DorisCreateTableSqlBuilder(
            TablePath tablePath,
            CatalogTable catalogTable,
            DorisTypeMapper typeMapper) {

        this(tablePath, catalogTable, typeMapper, 10);
    }

    public DorisCreateTableSqlBuilder(
            TablePath tablePath,
            CatalogTable catalogTable,
            DorisTypeMapper typeMapper,
            int buckets) {

        this.tablePath = tablePath;
        this.catalogTable = catalogTable;
        this.typeMapper = typeMapper;
        this.buckets = buckets > 0 ? buckets : 10;

        // 预解析 key type 和 key columns，供 buildColumn 使用
        this.resolvedKeyType = resolveKeyType();
        this.resolvedKeyColumns = resolveKeyColumns();
    }

    /**
     * 确定 Key 类型：优先从 options 读取，其次根据主键推断。
     */
    private String resolveKeyType() {
        Map<String, String> options = catalogTable.getOptions();
        String configuredKeyType = options != null ? options.get(TABLE_OPTION_KEY_TYPE) : null;

        if (hasText(configuredKeyType)) {
            return configuredKeyType.toUpperCase(Locale.ROOT) + " KEY";
        }

        PrimaryKey primaryKey = catalogTable.getTableSchema().getPrimaryKey();
        if (primaryKey != null && !primaryKey.getColumnNames().isEmpty()) {
            return "UNIQUE KEY";
        }
        return "DUPLICATE KEY";
    }

    /**
     * 确定 Key 列名列表。
     */
    private List<String> resolveKeyColumns() {
        PrimaryKey primaryKey = catalogTable.getTableSchema().getPrimaryKey();
        if (primaryKey != null && !primaryKey.getColumnNames().isEmpty()) {
            return primaryKey.getColumnNames();
        }
        // DUPLICATE KEY 无主键时使用首字段
        List<String> single = new ArrayList<>(1);
        single.add(catalogTable.getTableSchema().getColumn(0).getName());
        return single;
    }

    private static String quoteIdentifier(String value) {
        return "`"
                + value.replace("`", "``")
                + "`";
    }

    private static String quoteTable(TablePath tablePath) {
        return quoteIdentifier(
                tablePath.getDatabaseName())
                + "."
                + quoteIdentifier(
                tablePath.getTableName());
    }

    private static String escapeString(String value) {
        return value
                .replace("\\", "\\\\")
                .replace("'", "''");
    }

    private static boolean hasText(String value) {
        return value != null
                && !value.trim().isEmpty();
    }

    private static String getOrDefault(
            Map<String, String> options,
            String key,
            String defaultValue) {

        String value = options.get(key);

        return hasText(value) ? value : defaultValue;
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

    public String build() {
        TableSchema schema =
                catalogTable.getTableSchema();

        List<String> definitions =
                new ArrayList<>();

        boolean preserveSourceType =
                "doris".equalsIgnoreCase(
                        catalogTable.getOptions()
                                .get("dialect"));

        boolean isAggregateKey = "AGGREGATE KEY".equals(resolvedKeyType);

        for (Column column : schema.getColumns()) {
            definitions.add(
                    buildColumn(column, preserveSourceType, isAggregateKey));
        }

        StringBuilder sql = new StringBuilder();

        sql.append("CREATE TABLE ")
                .append(quoteTable(tablePath))
                .append(" (\n    ")
                .append(
                        String.join(
                                ",\n    ",
                                definitions))
                .append("\n)");

        String keyType = resolvedKeyType;
        String keyColumns = resolvedKeyColumns
                .stream()
                .map(DorisCreateTableSqlBuilder::quoteIdentifier)
                .collect(Collectors.joining(", "));

        sql.append(" ENGINE=OLAP ")
                .append(keyType)
                .append("(")
                .append(keyColumns)
                .append(")");

        /*
         * DISTRIBUTED BY HASH
         *
         * 使用 Key 列作为分布键。
         */
        sql.append(" DISTRIBUTED BY HASH(")
                .append(keyColumns)
                .append(") BUCKETS ")
                .append(buckets);

        /*
         * PROPERTIES
         */
        String replication =
                getOrDefault(
                        catalogTable.getOptions(),
                        TABLE_OPTION_REPLICATION,
                        "tag.location.default: 1");

        List<String> properties = new ArrayList<>();
        properties.add("\"replication_allocation\" = \"" + replication + "\"");

        // 透传 doris.property.{key} 前缀的额外属性
        Map<String, String> options = catalogTable.getOptions();
        if (options != null) {
            for (Map.Entry<String, String> entry : options.entrySet()) {
                String key = entry.getKey();
                if (key.startsWith(TABLE_OPTION_PROPERTY_PREFIX)) {
                    String propKey = key.substring(TABLE_OPTION_PROPERTY_PREFIX.length());
                    String propValue = entry.getValue();
                    if (hasText(propKey) && propValue != null) {
                        properties.add("\"" + propKey + "\" = \"" + propValue + "\"");
                    }
                }
            }
        }

        sql.append(" PROPERTIES (")
                .append(String.join(", ", properties))
                .append(")");

        /*
         * 表注释
         */
        if (hasText(catalogTable.getComment())) {
            sql.append(" COMMENT '")
                    .append(
                            escapeString(
                                    catalogTable.getComment()))
                    .append('\'');
        }

        return sql.append(';')
                .toString();
    }

    /**
     * 构建可复用的字段定义，用于 ALTER TABLE ADD COLUMN。
     */
    public String buildColumnDefinition(Column column) {
        boolean preserveSourceType =
                "doris".equalsIgnoreCase(
                        catalogTable.getOptions()
                                .get("dialect"));

        return buildColumn(column, preserveSourceType, false);
    }

    private String buildColumn(
            Column column,
            boolean preserveSourceType,
            boolean isAggregateKey) {

        List<String> parts = new ArrayList<>();

        parts.add(
                quoteIdentifier(
                        column.getName()));

        parts.add(
                typeMapper.toDorisType(
                        column,
                        preserveSourceType));

        /*
         * AGGREGATE KEY 模型下，非 Key 列必须声明聚合函数。
         * 从 CatalogTable options 中读取 doris.aggregate.fn.{columnName}。
         */
        if (isAggregateKey && !resolvedKeyColumns.contains(column.getName())) {
            String aggFn = resolveAggregateFunction(column.getName());
            if (aggFn != null) {
                parts.add(aggFn);
            }
        }

        parts.add(
                column.isNullable()
                        ? "NULL"
                        : "NOT NULL");

        if (!column.isAutoIncrement()
                && column.getDefaultValue() != null) {

            parts.add(
                    "DEFAULT "
                            + formatDefaultValue(column));
        }

        if (hasText(column.getComment())) {
            parts.add(
                    "COMMENT '"
                            + escapeString(
                            column.getComment())
                            + "'");
        }

        return String.join(" ", parts);
    }

    /**
     * 从 CatalogTable options 中解析列的聚合函数声明。
     *
     * <p>优先读取 {@code doris.aggregate.fn.{columnName}}，
     * 未配置时根据列类型推断默认值（数值类型默认 SUM，其他 REPLACE）。
     */
    private String resolveAggregateFunction(String columnName) {
        Map<String, String> options = catalogTable.getOptions();
        if (options != null) {
            String explicit = options.get(TABLE_OPTION_AGG_FN_PREFIX + columnName);
            if (hasText(explicit)) {
                return explicit.trim().toUpperCase(Locale.ROOT);
            }
        }
        // 未显式配置时，不自动推断，返回 null（建表时 Doris 会报错提醒用户配置）
        return null;
    }

    private String formatDefaultValue(Column column) {
        Object value = column.getDefaultValue();

        if (value instanceof Number) {
            return value.toString();
        }

        if (value instanceof Boolean) {
            return (Boolean) value ? "1" : "0";
        }

        String text =
                String.valueOf(value).trim();

        String upper =
                text.toUpperCase(Locale.ROOT);

        if ("NULL".equals(upper)
                || "CURRENT_TIMESTAMP".equals(upper)
                || upper.startsWith("CURRENT_TIMESTAMP(")
                || "CURRENT_DATE".equals(upper)
                || "CURRENT_TIME".equals(upper)) {

            return text;
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
}
