package com.link.up.connector.doris.catalog;

import com.link.up.api.table.catalog.Column;
import com.link.up.api.table.type.BasicType;
import com.link.up.api.table.type.DecimalType;
import com.link.up.api.table.type.FluxDataType;
import com.link.up.api.table.type.SqlType;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Locale;

/**
 * Doris 与 Flux 类型之间的转换器。
 *
 * <p>Doris 兼容 MySQL 协议，INFORMATION_SCHEMA 中的类型名
 * 与 MySQL 类似，但存在 Doris 特有类型（如 LARGEINT、HLL、BITMAP）。
 */
public final class DorisTypeMapper {

    private static final int DEFAULT_DECIMAL_PRECISION = 38;
    private static final int MAX_DORIS_DECIMAL_PRECISION = 65;

    private static boolean hasText(String value) {
        return value != null && !value.trim().isEmpty();
    }

    private static int valueOrDefault(
            Integer value,
            int defaultValue) {

        return value == null ? defaultValue : value;
    }

    /**
     * 将 INFORMATION_SCHEMA.COLUMNS 当前行转换为 Flux Column。
     */
    public Column toColumn(
            ResultSet resultSet)
            throws SQLException {

        DorisColumnMetadata metadata =
                DorisColumnMetadata.from(resultSet);

        FluxDataType<?> fluxType =
                toFluxType(metadata);

        Column.Builder builder =
                Column.builder(
                        metadata.columnName,
                        fluxType)
                        .nullable(metadata.nullable)
                        .defaultValue(metadata.defaultValue)
                        .comment(metadata.comment)
                        .sourceType(metadata.columnType);

        SqlType sqlType =
                fluxType.getSqlType();

        if (sqlType == SqlType.STRING
                || sqlType == SqlType.BYTES) {

            builder.length(
                    metadata.characterLength);
        }

        if (sqlType == SqlType.DECIMAL) {
            builder.precision(
                    metadata.numericPrecision);

            builder.scale(
                    metadata.numericScale);
        } else if (sqlType == SqlType.TIME
                || sqlType == SqlType.TIMESTAMP
                || sqlType == SqlType.TIMESTAMP_TZ) {

            builder.precision(
                    metadata.dateTimePrecision);
        } else if (isInteger(sqlType)) {
            builder.precision(
                    metadata.numericPrecision);
        }

        if (metadata.characterSet != null) {
            builder.attribute(
                    "charset",
                    metadata.characterSet);
        }

        if (metadata.collation != null) {
            builder.attribute(
                    "collation",
                    metadata.collation);
        }

        return builder.build();
    }

    /**
     * 将 Flux 字段转换成 Doris 字段类型。
     *
     * @param preserveSourceType 当字段本身来自 Doris 时，
     *                           是否优先保留原始类型
     */
    public String toDorisType(
            Column column,
            boolean preserveSourceType) {

        if (preserveSourceType
                && hasText(column.getSourceType())) {

            return column.getSourceType();
        }

        SqlType sqlType =
                column.getDataType().getSqlType();

        switch (sqlType) {
            case STRING:
                return buildStringType(column);

            case BOOLEAN:
                return "BOOLEAN";

            case TINYINT:
                return "TINYINT";

            case SMALLINT:
                return "SMALLINT";

            case INT:
                return "INT";

            case BIGINT:
                return "BIGINT";

            case FLOAT:
                return "FLOAT";

            case DOUBLE:
                return "DOUBLE";

            case DECIMAL:
                return buildDecimalType(column);

            case BYTES:
                return buildBinaryType(column);

            case DATE:
                return "DATE";

            case TIME:
            case TIMESTAMP:
                return buildDatetimeType(column);

            case TIMESTAMP_TZ:
                /*
                 * Doris 没有完整的 TIMESTAMP WITH TIME ZONE。
                 * 当前统一映射为 DATETIME。
                 */
                return buildDatetimeType(column);

            case ARRAY:
            case ROW:
            case NULL:
            default:
                throw new IllegalArgumentException(
                        "Doris 不支持 Flux 类型："
                                + sqlType
                                + "，column="
                                + column.getName());
        }
    }

    private FluxDataType<?> toFluxType(
            DorisColumnMetadata metadata) {

        String dataType =
                metadata.dataType.toLowerCase(
                        Locale.ROOT);

        switch (dataType) {
            case "bool":
            case "boolean":
                return BasicType.BOOLEAN_TYPE;

            case "tinyint":
                return BasicType.INT_TYPE;

            case "smallint":
                return BasicType.INT_TYPE;

            case "int":
            case "integer":
                return BasicType.INT_TYPE;

            case "bigint":
                return BasicType.LONG_TYPE;

            case "largeint":
                /*
                 * Doris 特有的 128 位整数，
                 * 映射为 DECIMAL(20, 0)。
                 */
                return new DecimalType(20, 0);

            case "float":
                return BasicType.FLOAT_TYPE;

            case "double":
            case "real":
                return BasicType.DOUBLE_TYPE;

            case "decimal":
            case "numeric":
                return decimalType(metadata);

            case "date":
            case "datev2":
                return BasicType.DATE_TYPE;

            case "datetime":
            case "timestamp":
            case "datetimev2":
            case "timestampv2":
                return BasicType.TIMESTAMP_TYPE;

            case "time":
                return BasicType.TIME_TYPE;

            case "char":
            case "varchar":
            case "tinytext":
            case "text":
            case "mediumtext":
            case "longtext":
            case "string":
                return BasicType.STRING_TYPE;

            case "binary":
            case "varbinary":
            case "tinyblob":
            case "blob":
            case "mediumblob":
            case "longblob":
                return BasicType.BYTES_TYPE;

            case "json":
            case "jsonb":
                return BasicType.STRING_TYPE;

            case "array":
                return BasicType.STRING_TYPE;

            case "map":
                return BasicType.STRING_TYPE;

            case "hll":
            case "bitmap":
                /*
                 * Doris 聚合类型，Catalog 发现时
                 * 映射为 STRING。
                 */
                return BasicType.STRING_TYPE;

            default:
                throw new IllegalArgumentException(
                        "暂不支持 Doris 字段类型："
                                + metadata.columnType
                                + "，column="
                                + metadata.columnName);
        }
    }

    private DecimalType decimalType(
            DorisColumnMetadata metadata) {

        int precision =
                valueOrDefault(
                        metadata.numericPrecision,
                        DEFAULT_DECIMAL_PRECISION);

        int scale =
                valueOrDefault(
                        metadata.numericScale,
                        0);

        precision =
                Math.min(
                        precision,
                        MAX_DORIS_DECIMAL_PRECISION);

        precision =
                Math.max(
                        precision,
                        scale);

        return new DecimalType(
                precision,
                scale);
    }

    private String buildStringType(Column column) {
        Long length = column.getLength();

        if (length == null || length <= 0) {
            return "STRING";
        }

        if (length <= 65533) {
            return "VARCHAR(" + length + ")";
        }

        return "STRING";
    }

    private String buildBinaryType(Column column) {
        Long length = column.getLength();

        if (length != null
                && length > 0
                && length <= 65535) {

            return "VARBINARY(" + length + ")";
        }

        return "STRING";
    }

    private String buildDecimalType(Column column) {
        int precision =
                column.getPrecision() == null
                        ? DEFAULT_DECIMAL_PRECISION
                        : column.getPrecision();

        int scale =
                column.getScale() == null
                        ? 0
                        : column.getScale();

        precision =
                Math.min(
                        precision,
                        MAX_DORIS_DECIMAL_PRECISION);

        precision =
                Math.max(
                        precision,
                        scale);

        return "DECIMAL("
                + precision
                + ", "
                + scale
                + ")";
    }

    private String buildDatetimeType(Column column) {
        Integer precision = column.getPrecision();

        if (precision == null || precision <= 0) {
            return "DATETIME";
        }

        int safePrecision =
                Math.min(precision, 6);

        return "DATETIME("
                + safePrecision
                + ")";
    }

    private static boolean isInteger(SqlType type) {
        return type == SqlType.TINYINT
                || type == SqlType.SMALLINT
                || type == SqlType.INT
                || type == SqlType.BIGINT;
    }

    /**
     * INFORMATION_SCHEMA.COLUMNS 中的一行字段元数据。
     */
    private static final class DorisColumnMetadata {

        private final String columnName;
        private final String dataType;
        private final String columnType;
        private final Long characterLength;
        private final Integer numericPrecision;
        private final Integer numericScale;
        private final Integer dateTimePrecision;
        private final boolean nullable;
        private final Object defaultValue;
        private final String comment;
        private final String characterSet;
        private final String collation;

        private DorisColumnMetadata(
                String columnName,
                String dataType,
                String columnType,
                Long characterLength,
                Integer numericPrecision,
                Integer numericScale,
                Integer dateTimePrecision,
                boolean nullable,
                Object defaultValue,
                String comment,
                String characterSet,
                String collation) {

            this.columnName = columnName;
            this.dataType = dataType;
            this.columnType = columnType;
            this.characterLength = characterLength;
            this.numericPrecision = numericPrecision;
            this.numericScale = numericScale;
            this.dateTimePrecision = dateTimePrecision;
            this.nullable = nullable;
            this.defaultValue = defaultValue;
            this.comment = normalize(comment);
            this.characterSet = normalize(characterSet);
            this.collation = normalize(collation);
        }

        private static DorisColumnMetadata from(
                ResultSet resultSet)
                throws SQLException {

            return new DorisColumnMetadata(
                    resultSet.getString("COLUMN_NAME"),
                    resultSet.getString("DATA_TYPE"),
                    resultSet.getString("COLUMN_TYPE"),
                    getLongOrNull(
                            resultSet,
                            "CHARACTER_MAXIMUM_LENGTH"),
                    getIntegerOrNull(
                            resultSet,
                            "NUMERIC_PRECISION"),
                    getIntegerOrNull(
                            resultSet,
                            "NUMERIC_SCALE"),
                    getIntegerOrNull(
                            resultSet,
                            "DATETIME_PRECISION"),
                    "YES".equalsIgnoreCase(
                            resultSet.getString(
                                    "IS_NULLABLE")),
                    resultSet.getObject(
                            "COLUMN_DEFAULT"),
                    resultSet.getString(
                            "COLUMN_COMMENT"),
                    resultSet.getString(
                            "CHARACTER_SET_NAME"),
                    resultSet.getString(
                            "COLLATION_NAME"));
        }

        private static Long getLongOrNull(
                ResultSet resultSet,
                String name)
                throws SQLException {

            Object value =
                    resultSet.getObject(name);

            if (value == null) {
                return null;
            }

            return ((Number) value)
                    .longValue();
        }

        private static Integer getIntegerOrNull(
                ResultSet resultSet,
                String name)
                throws SQLException {

            Object value =
                    resultSet.getObject(name);

            if (value == null) {
                return null;
            }

            return ((Number) value)
                    .intValue();
        }

        private static String normalize(String value) {
            if (value == null) {
                return null;
            }

            String normalized = value.trim();

            return normalized.isEmpty()
                    ? null
                    : normalized;
        }
    }
}
