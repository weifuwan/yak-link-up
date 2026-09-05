package com.link.up.connector.jdbc.core.dialect.dameng;

import com.link.up.api.table.catalog.Column;
import com.link.up.api.table.type.BasicType;
import com.link.up.api.table.type.DecimalType;
import com.link.up.api.table.type.FluxDataType;
import com.link.up.api.table.type.SqlType;
import com.link.up.connector.jdbc.core.dialect.JdbcTypeMapper;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Locale;

/** Dameng DM8 JDBC type mapper for bounded/offline jobs. */
public final class DamengTypeMapper implements JdbcTypeMapper {

    static final int MAX_DECIMAL_PRECISION = 38;
    static final int DEFAULT_DECIMAL_SCALE = 18;
    static final int MAX_TEMPORAL_PRECISION = 6;
    static final long MAX_INLINE_BYTES = 1900L;

    @Override
    public Column map(ResultSetMetaData metadata, int columnIndex) throws SQLException {
        String name = firstText(
                metadata.getColumnLabel(columnIndex),
                metadata.getColumnName(columnIndex));
        String sourceType = metadata.getColumnTypeName(columnIndex);
        int precision = metadata.getPrecision(columnIndex);
        int scale = metadata.getScale(columnIndex);
        int jdbcType = metadata.getColumnType(columnIndex);

        FluxDataType<?> type = mapType(jdbcType, sourceType, precision, scale);
        Column.Builder builder = Column.builder(name, type)
                .nullable(metadata.isNullable(columnIndex) != ResultSetMetaData.columnNoNulls)
                .sourceType(buildSourceType(sourceType, precision, scale));
        applyProperties(builder, type, sourceType, precision, scale);
        return builder.build();
    }

    /** Maps one DatabaseMetaData#getColumns row. */
    public Column toColumn(ResultSet row) throws SQLException {
        String name = row.getString("COLUMN_NAME");
        String typeName = row.getString("TYPE_NAME");
        Integer size = integer(row, "COLUMN_SIZE");
        Integer scale = integer(row, "DECIMAL_DIGITS");
        Integer nullableValue = integer(row, "NULLABLE");
        int precision = value(size);
        int safeScale = value(scale);

        FluxDataType<?> type = mapType(
                value(integer(row, "DATA_TYPE")),
                typeName,
                precision,
                safeScale);

        Column.Builder builder = Column.builder(name, type)
                .nullable(nullableValue == null
                        || nullableValue != ResultSetMetaData.columnNoNulls)
                .defaultValue(row.getObject("COLUMN_DEF"))
                .comment(row.getString("REMARKS"))
                .sourceType(buildSourceType(typeName, precision, safeScale));

        String auto = safeString(row, "IS_AUTOINCREMENT");
        builder.autoIncrement("YES".equalsIgnoreCase(auto));
        applyProperties(builder, type, typeName, precision, safeScale);
        return builder.build();
    }

    @Override
    public String toDatabaseType(Column column) {
        return toDatabaseType(column, false);
    }

    public String toDatabaseType(Column column, boolean preserveSourceType) {
        if (preserveSourceType && canPreserve(column.getSourceType())) {
            return column.getSourceType().trim();
        }

        SqlType type = column.getDataType().getSqlType();
        switch (type) {
            case BOOLEAN:
                return "BIT";
            case TINYINT:
                return "TINYINT";
            case SMALLINT:
                return "SMALLINT";
            case INT:
                return "INT";
            case BIGINT:
                return "BIGINT";
            case FLOAT:
                return "REAL";
            case DOUBLE:
                return "DOUBLE";
            case DECIMAL:
                return decimalType(column);
            case STRING:
                return stringType(column);
            case BYTES:
                return binaryType(column);
            case DATE:
                return "DATE";
            case TIME:
                return temporalType("TIME", column.getPrecision());
            case TIMESTAMP:
                return temporalType("TIMESTAMP", column.getPrecision());
            case TIMESTAMP_TZ:
                return temporalType("DATETIME", column.getPrecision())
                        + " WITH TIME ZONE";
            default:
                throw new IllegalArgumentException(
                        "Dameng 不支持 Flux 类型：" + type + "，column=" + column.getName());
        }
    }

    /** Package-visible for focused type contract tests. */
    FluxDataType<?> mapType(
            int jdbcType,
            String sourceType,
            int precision,
            int scale) {

        String normalized = normalizeType(sourceType);
        String base = baseType(normalized);

        if (normalized.contains("WITH TIME ZONE")) {
            if (base.startsWith("TIME") && !base.startsWith("TIMESTAMP")) {
                // Flux has no TIME WITH TIME ZONE primitive. Keep the exact textual value
                // rather than silently discarding the offset.
                return BasicType.STRING_TYPE;
            }
            return BasicType.TIMESTAMP_TZ_TYPE;
        }

        if (base.startsWith("TIMESTAMP") || "DATETIME".equals(base)) {
            return BasicType.TIMESTAMP_TYPE;
        }

        switch (base) {
            case "BIT":
            case "BOOLEAN":
                return BasicType.BOOLEAN_TYPE;
            case "TINYINT":
            case "BYTE":
                return BasicType.BYTE_TYPE;
            case "SMALLINT":
                return BasicType.SHORT_TYPE;
            case "INT":
            case "INTEGER":
            case "PLS_INTEGER":
                return BasicType.INT_TYPE;
            case "BIGINT":
                return BasicType.LONG_TYPE;
            case "REAL":
                return BasicType.FLOAT_TYPE;
            case "FLOAT":
            case "DOUBLE":
            case "DOUBLE PRECISION":
                return BasicType.DOUBLE_TYPE;
            case "NUMERIC":
            case "NUMBER":
            case "DECIMAL":
            case "DEC":
                return decimal(precision, scale);
            case "CHAR":
            case "CHARACTER":
            case "VARCHAR":
            case "VARCHAR2":
            case "NCHAR":
            case "NVARCHAR":
            case "NVARCHAR2":
            case "LONGVARCHAR":
            case "CLOB":
            case "TEXT":
            case "LONG":
            case "BFILE":
            case "ROWID":
                return BasicType.STRING_TYPE;
            case "BINARY":
            case "VARBINARY":
            case "LONGVARBINARY":
            case "BLOB":
            case "IMAGE":
                return BasicType.BYTES_TYPE;
            case "DATE":
                return BasicType.DATE_TYPE;
            case "TIME":
                return BasicType.TIME_TYPE;
            default:
                break;
        }

        switch (jdbcType) {
            case Types.BOOLEAN:
            case Types.BIT:
                return BasicType.BOOLEAN_TYPE;
            case Types.TINYINT:
                return BasicType.BYTE_TYPE;
            case Types.SMALLINT:
                return BasicType.SHORT_TYPE;
            case Types.INTEGER:
                return BasicType.INT_TYPE;
            case Types.BIGINT:
                return BasicType.LONG_TYPE;
            case Types.REAL:
                return BasicType.FLOAT_TYPE;
            case Types.FLOAT:
            case Types.DOUBLE:
                return BasicType.DOUBLE_TYPE;
            case Types.NUMERIC:
            case Types.DECIMAL:
                return decimal(precision, scale);
            case Types.DATE:
                return BasicType.DATE_TYPE;
            case Types.TIME:
                return BasicType.TIME_TYPE;
            case Types.TIMESTAMP:
                return BasicType.TIMESTAMP_TYPE;
            case Types.TIMESTAMP_WITH_TIMEZONE:
                return BasicType.TIMESTAMP_TZ_TYPE;
            case Types.BINARY:
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
            case Types.BLOB:
                return BasicType.BYTES_TYPE;
            case Types.CHAR:
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
            case Types.NCHAR:
            case Types.NVARCHAR:
            case Types.LONGNVARCHAR:
            case Types.CLOB:
            case Types.NCLOB:
                return BasicType.STRING_TYPE;
            default:
                throw new IllegalArgumentException(
                        "暂不支持 Dameng 字段类型：" + sourceType + "，jdbcType=" + jdbcType);
        }
    }

    private static DecimalType decimal(int precision, int scale) {
        int safePrecision = precision <= 0 ? MAX_DECIMAL_PRECISION : precision;
        int safeScale = precision <= 0 ? DEFAULT_DECIMAL_SCALE : Math.max(0, scale);
        validateDecimal(safePrecision, safeScale, null);
        return new DecimalType(safePrecision, safeScale);
    }

    private static String decimalType(Column column) {
        Integer precisionValue = column.getPrecision();
        Integer scaleValue = column.getScale();
        if (column.getDataType() instanceof DecimalType) {
            DecimalType decimal = (DecimalType) column.getDataType();
            if (precisionValue == null) {
                precisionValue = decimal.getPrecision();
            }
            if (scaleValue == null) {
                scaleValue = decimal.getScale();
            }
        }
        int precision = precisionValue == null ? MAX_DECIMAL_PRECISION : precisionValue;
        int scale = scaleValue == null ? 0 : scaleValue;
        if (precision <= 0) {
            precision = MAX_DECIMAL_PRECISION;
        }
        validateDecimal(precision, scale, column.getName());
        return "DECIMAL(" + precision + "," + scale + ")";
    }

    private static void validateDecimal(int precision, int scale, String column) {
        if (precision > MAX_DECIMAL_PRECISION) {
            throw new IllegalArgumentException(
                    "Dameng DECIMAL precision 最大为 38"
                            + (column == null ? "" : "，column=" + column)
                            + "，precision=" + precision);
        }
        if (precision <= 0 || scale < 0 || scale > precision) {
            throw new IllegalArgumentException(
                    "Dameng DECIMAL precision/scale 非法"
                            + (column == null ? "" : "，column=" + column)
                            + "，precision=" + precision + "，scale=" + scale);
        }
    }

    private static String stringType(Column column) {
        Long length = column.getLength();
        if (length == null || length <= 0) {
            return "TEXT";
        }
        long bytes = length > Long.MAX_VALUE / 4L ? Long.MAX_VALUE : length * 4L;
        if (bytes <= MAX_INLINE_BYTES) {
            return "VARCHAR2(" + Math.max(1L, bytes) + ")";
        }
        return "TEXT";
    }

    private static String binaryType(Column column) {
        Long length = column.getLength();
        if (length == null || length <= 0 || length > MAX_INLINE_BYTES) {
            return "BLOB";
        }
        return "VARBINARY(" + Math.max(1L, length) + ")";
    }

    private static String temporalType(String type, Integer precision) {
        if (precision == null || precision <= 0) {
            return type;
        }
        if (precision > MAX_TEMPORAL_PRECISION) {
            throw new IllegalArgumentException(
                    "Dameng " + type + " precision 最大为 6，precision=" + precision);
        }
        return type + "(" + precision + ")";
    }

    private static void applyProperties(
            Column.Builder builder,
            FluxDataType<?> type,
            String sourceType,
            int precision,
            int scale) {
        SqlType sqlType = type.getSqlType();
        if (sqlType == SqlType.STRING || sqlType == SqlType.BYTES) {
            if (precision > 0) {
                builder.length((long) precision);
            }
            return;
        }
        if (sqlType == SqlType.DECIMAL && type instanceof DecimalType) {
            DecimalType decimal = (DecimalType) type;
            builder.precision(decimal.getPrecision());
            builder.scale(decimal.getScale());
            return;
        }
        if (sqlType == SqlType.TIME
                || sqlType == SqlType.TIMESTAMP
                || sqlType == SqlType.TIMESTAMP_TZ) {
            int p = Math.max(0, Math.min(
                    scale > 0 ? scale : precision,
                    MAX_TEMPORAL_PRECISION));
            if (p > 0) {
                builder.precision(p);
            }
        }
    }

    private static String buildSourceType(String sourceType, int precision, int scale) {
        String raw = sourceType == null ? "" : sourceType.trim();
        if (raw.isEmpty() || raw.contains("(")) {
            return raw;
        }
        String normalized = normalizeType(raw);
        String base = baseType(normalized);
        if (isDecimal(base)) {
            return precision > 0
                    ? raw + "(" + precision + "," + Math.max(0, scale) + ")"
                    : raw;
        }
        if (isLengthType(base)) {
            return precision > 0 ? raw + "(" + precision + ")" : raw;
        }
        if (normalized.contains("WITH TIME ZONE")) {
            int p = Math.min(Math.max(0, scale), MAX_TEMPORAL_PRECISION);
            if (p <= 0) {
                return raw;
            }
            if (base.startsWith("TIME") && !base.startsWith("TIMESTAMP")) {
                return "TIME(" + p + ") WITH TIME ZONE";
            }
            if ("DATETIME".equals(base)) {
                return "DATETIME(" + p + ") WITH TIME ZONE";
            }
            return "TIMESTAMP(" + p + ") WITH TIME ZONE";
        }
        if (base.startsWith("TIMESTAMP") || "DATETIME".equals(base) || "TIME".equals(base)) {
            return scale > 0 ? raw + "(" + Math.min(scale, MAX_TEMPORAL_PRECISION) + ")" : raw;
        }
        return raw;
    }

    private static boolean canPreserve(String sourceType) {
        String type = normalizeType(sourceType);
        if (type.isEmpty()) {
            return false;
        }
        String base = baseType(type);
        return "BIT".equals(base)
                || "BOOLEAN".equals(base)
                || "TINYINT".equals(base)
                || "BYTE".equals(base)
                || "SMALLINT".equals(base)
                || "INT".equals(base)
                || "INTEGER".equals(base)
                || "PLS_INTEGER".equals(base)
                || "BIGINT".equals(base)
                || "REAL".equals(base)
                || "FLOAT".equals(base)
                || "DOUBLE".equals(base)
                || "DOUBLE PRECISION".equals(base)
                || isDecimal(base)
                || isLengthType(base)
                || "LONGVARCHAR".equals(base)
                || "CLOB".equals(base)
                || "TEXT".equals(base)
                || "LONG".equals(base)
                || "LONGVARBINARY".equals(base)
                || "BLOB".equals(base)
                || "IMAGE".equals(base)
                || "DATE".equals(base)
                || "TIME".equals(base)
                || base.startsWith("TIMESTAMP")
                || "DATETIME".equals(base)
                || type.contains("WITH TIME ZONE");
    }

    private static boolean isDecimal(String base) {
        return "NUMERIC".equals(base)
                || "NUMBER".equals(base)
                || "DECIMAL".equals(base)
                || "DEC".equals(base);
    }

    private static boolean isLengthType(String base) {
        return "CHAR".equals(base)
                || "CHARACTER".equals(base)
                || "VARCHAR".equals(base)
                || "VARCHAR2".equals(base)
                || "NCHAR".equals(base)
                || "NVARCHAR".equals(base)
                || "NVARCHAR2".equals(base)
                || "BINARY".equals(base)
                || "VARBINARY".equals(base);
    }

    private static String baseType(String type) {
        int parenthesis = type.indexOf('(');
        String value = parenthesis >= 0 ? type.substring(0, parenthesis).trim() : type;
        if (value.startsWith("TIME ") && value.contains("WITH TIME ZONE")) {
            return "TIME";
        }
        if (value.startsWith("DATETIME ") && value.contains("WITH TIME ZONE")) {
            return "DATETIME";
        }
        if (value.startsWith("TIMESTAMP ") && value.contains("WITH TIME ZONE")) {
            return "TIMESTAMP";
        }
        return value;
    }

    private static String normalizeType(String value) {
        return value == null
                ? ""
                : value.trim().toUpperCase(Locale.ROOT).replaceAll("\\s+", " ");
    }

    private static String firstText(String first, String second) {
        String value = normalize(first);
        return value == null ? normalize(second) : value;
    }

    private static String normalize(String value) {
        if (value == null) {
            return null;
        }
        String normalized = value.trim();
        return normalized.isEmpty() ? null : normalized;
    }

    private static Integer integer(ResultSet row, String name) throws SQLException {
        Object value = row.getObject(name);
        return value == null ? null : ((Number) value).intValue();
    }

    private static int value(Integer value) {
        return value == null ? 0 : value;
    }

    private static String safeString(ResultSet row, String name) {
        try {
            return row.getString(name);
        } catch (SQLException ignored) {
            return null;
        }
    }
}
