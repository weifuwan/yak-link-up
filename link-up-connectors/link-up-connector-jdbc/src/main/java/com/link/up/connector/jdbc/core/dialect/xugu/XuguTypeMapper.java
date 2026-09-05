package com.link.up.connector.jdbc.core.dialect.xugu;

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

/** XuguDB JDBC type mapper for bounded/offline jobs. */
public final class XuguTypeMapper implements JdbcTypeMapper {

    public static final int MAX_NUMERIC_PRECISION = 38;
    public static final int DEFAULT_NUMERIC_PRECISION = 12;
    public static final int DEFAULT_NUMERIC_SCALE = 0;
    public static final int MAX_TIME_PRECISION = 3;
    public static final int MAX_TIMESTAMP_PRECISION = 6;
    public static final long MAX_VARCHAR_LENGTH = 60_000L;
    public static final long MAX_BINARY_LENGTH = 60_000L;
    public static final String NATIVE_ATTRIBUTE = "xugu_native";

    @Override
    public Column map(ResultSetMetaData metadata, int columnIndex) throws SQLException {
        String name = firstText(
                metadata.getColumnLabel(columnIndex),
                metadata.getColumnName(columnIndex));
        String sourceType = metadata.getColumnTypeName(columnIndex);
        int precision = metadata.getPrecision(columnIndex);
        int scale = metadata.getScale(columnIndex);
        FluxDataType<?> type = mapType(
                metadata.getColumnType(columnIndex), sourceType, precision, scale);

        Column.Builder builder = Column.builder(name, type)
                .nullable(metadata.isNullable(columnIndex)
                        != ResultSetMetaData.columnNoNulls)
                .sourceType(buildSourceType(sourceType, precision, scale))
                .attribute(NATIVE_ATTRIBUTE, "true");
        applyProperties(builder, type, sourceType, precision, scale);
        return builder.build();
    }

    /** Maps one JDBC DatabaseMetaData#getColumns row. */
    public Column toColumn(ResultSet row) throws SQLException {
        String name = row.getString("COLUMN_NAME");
        String typeName = row.getString("TYPE_NAME");
        Integer size = integer(row, "COLUMN_SIZE");
        Integer digits = integer(row, "DECIMAL_DIGITS");
        Integer nullable = integer(row, "NULLABLE");
        int jdbcType = value(integer(row, "DATA_TYPE"));
        int precision = value(size);
        int scale = value(digits);
        FluxDataType<?> type = mapType(jdbcType, typeName, precision, scale);
        Object defaultValue = safeObject(row, "COLUMN_DEF");

        Column.Builder builder = Column.builder(name, type)
                .nullable(nullable == null
                        || nullable != ResultSetMetaData.columnNoNulls)
                .defaultValue(defaultValue)
                .comment(safeString(row, "REMARKS"))
                .sourceType(buildSourceType(typeName, precision, scale))
                .attribute(NATIVE_ATTRIBUTE, "true");

        String auto = safeString(row, "IS_AUTOINCREMENT");
        builder.autoIncrement("YES".equalsIgnoreCase(auto)
                || isIdentityDefault(defaultValue));
        applyProperties(builder, type, typeName, precision, scale);
        return builder.build();
    }

    @Override
    public String toDatabaseType(Column column) {
        return toDatabaseType(column, false);
    }

    public String toDatabaseType(Column column, boolean preserveSourceType) {
        if (preserveSourceType && canPreserve(column)) {
            return column.getSourceType().trim();
        }

        SqlType type = column.getDataType().getSqlType();
        switch (type) {
            case BOOLEAN:
                return "BOOLEAN";
            case TINYINT:
                return "TINYINT";
            case SMALLINT:
                return "SMALLINT";
            case INT:
                return "INTEGER";
            case BIGINT:
                return "BIGINT";
            case FLOAT:
                return "FLOAT";
            case DOUBLE:
                return "DOUBLE";
            case DECIMAL:
                return decimalType(column);
            case STRING:
                // TIME WITH TIME ZONE has a compact exact-text representation;
                // avoid allocating an unbounded CLOB merely because Flux has no
                // TIME_TZ primitive.
                return isOffsetTimeSource(column) ? "VARCHAR(64)" : stringType(column);
            case BYTES:
                return binaryType(column);
            case DATE:
                return "DATE";
            case TIME:
                return temporal("TIME", column, MAX_TIME_PRECISION);
            case TIMESTAMP:
                return temporal("TIMESTAMP", column, MAX_TIMESTAMP_PRECISION);
            case TIMESTAMP_TZ:
                // Xugu JDBC batch binding of offset-aware timestamp values has a
                // known driver failure path. Preserve the offset as ISO text
                // instead of silently writing a wall-clock TIMESTAMP.
                return "VARCHAR(64)";
            default:
                throw new IllegalArgumentException(
                        "XuguDB 不支持 Flux 类型：" + type
                                + "，column=" + column.getName());
        }
    }

    /** Package-visible for focused contract tests. */
    FluxDataType<?> mapType(
            int jdbcType,
            String sourceType,
            int precision,
            int scale) {

        String type = normalize(sourceType);
        String base = baseType(type);

        if (isTimeWithTimeZone(type, base)) {
            return BasicType.STRING_TYPE;
        }
        if (isTimestampWithTimeZone(type, base)) {
            return BasicType.TIMESTAMP_TZ_TYPE;
        }
        if (type.startsWith("timestamp")
                || type.startsWith("datetime")) {
            return BasicType.TIMESTAMP_TYPE;
        }

        switch (base) {
            case "boolean":
            case "bool":
                return BasicType.BOOLEAN_TYPE;
            case "tinyint":
                return BasicType.BYTE_TYPE;
            case "smallint":
            case "short":
                return BasicType.SHORT_TYPE;
            case "integer":
            case "int":
            case "pls_integer":
            case "binary_integer":
                return BasicType.INT_TYPE;
            case "bigint":
            case "longint":
                return BasicType.LONG_TYPE;
            case "float":
            case "real":
                return BasicType.FLOAT_TYPE;
            case "double":
            case "double precision":
                return BasicType.DOUBLE_TYPE;
            case "numeric":
            case "decimal":
            case "number":
                return numericType(precision, scale);
            case "date":
                return BasicType.DATE_TYPE;
            case "time":
                return BasicType.TIME_TYPE;
            case "binary":
            case "varbinary":
            case "longvarbinary":
            case "long varbinary":
            case "blob":
            case "raw":
                return BasicType.BYTES_TYPE;
            case "bit":
            case "varbit":
            case "bit varying":
                // BIT/VARBIT are bit strings up to 60,000 bits, not Boolean/Byte.
                return BasicType.STRING_TYPE;
            default:
                break;
        }

        if (isStringLike(type, base)) {
            return BasicType.STRING_TYPE;
        }

        switch (jdbcType) {
            case Types.BOOLEAN:
                return BasicType.BOOLEAN_TYPE;
            case Types.TINYINT:
                return BasicType.BYTE_TYPE;
            case Types.SMALLINT:
                return BasicType.SHORT_TYPE;
            case Types.INTEGER:
                return BasicType.INT_TYPE;
            case Types.BIGINT:
                return BasicType.LONG_TYPE;
            case Types.FLOAT:
            case Types.REAL:
                return BasicType.FLOAT_TYPE;
            case Types.DOUBLE:
                return BasicType.DOUBLE_TYPE;
            case Types.NUMERIC:
            case Types.DECIMAL:
                return numericType(precision, scale);
            case Types.DATE:
                return BasicType.DATE_TYPE;
            case Types.TIME:
                return BasicType.TIME_TYPE;
            case Types.TIME_WITH_TIMEZONE:
                return BasicType.STRING_TYPE;
            case Types.TIMESTAMP:
                return BasicType.TIMESTAMP_TYPE;
            case Types.TIMESTAMP_WITH_TIMEZONE:
                return BasicType.TIMESTAMP_TZ_TYPE;
            case Types.BINARY:
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
            case Types.BLOB:
                return BasicType.BYTES_TYPE;
            case Types.BIT:
                return BasicType.STRING_TYPE;
            default:
                return BasicType.STRING_TYPE;
        }
    }

    private static FluxDataType<?> numericType(int precision, int scale) {
        int p = precision <= 0 ? DEFAULT_NUMERIC_PRECISION : precision;
        int s = precision <= 0 ? DEFAULT_NUMERIC_SCALE : scale;
        if (p <= 0 || p > MAX_NUMERIC_PRECISION
                || s < 0 || s > p) {
            return BasicType.STRING_TYPE;
        }
        return new DecimalType(p, s);
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
        int precision = precisionValue == null
                ? DEFAULT_NUMERIC_PRECISION : precisionValue;
        int scale = scaleValue == null
                ? DEFAULT_NUMERIC_SCALE : scaleValue;
        if (precision <= 0 || precision > MAX_NUMERIC_PRECISION) {
            throw new IllegalArgumentException(
                    "XuguDB NUMERIC precision 必须在 1..38，column="
                            + column.getName() + "，precision=" + precision);
        }
        if (scale < 0 || scale > precision) {
            throw new IllegalArgumentException(
                    "XuguDB NUMERIC scale 非法，column=" + column.getName()
                            + "，precision=" + precision + "，scale=" + scale);
        }
        return "NUMERIC(" + precision + "," + scale + ")";
    }

    private static String stringType(Column column) {
        Long length = column.getLength();
        if (length == null || length <= 0 || length > MAX_VARCHAR_LENGTH) {
            return "CLOB";
        }
        return "VARCHAR(" + length + ")";
    }

    private static String binaryType(Column column) {
        Long length = column.getLength();
        if (length == null || length <= 0 || length > MAX_BINARY_LENGTH) {
            return "BLOB";
        }
        return "BINARY(" + length + ")";
    }

    private static String temporal(String type, Column column, int maxPrecision) {
        Integer precision = column.getPrecision();
        if (precision == null || precision <= 0) {
            return type;
        }
        if (precision > maxPrecision) {
            throw new IllegalArgumentException(
                    "XuguDB " + type + " precision 最大为 " + maxPrecision
                            + "，column=" + column.getName()
                            + "，precision=" + precision);
        }
        return type + "(" + precision + ")";
    }

    private static void applyProperties(
            Column.Builder builder,
            FluxDataType<?> dataType,
            String sourceType,
            int precision,
            int scale) {

        SqlType sqlType = dataType.getSqlType();
        String type = normalize(sourceType);
        String base = baseType(type);

        if (sqlType == SqlType.STRING || sqlType == SqlType.BYTES) {
            if (!isNumericBase(base)
                    && !isTimeWithTimeZone(type, base)
                    && !isTimestampWithTimeZone(type, base)
                    && precision > 0) {
                builder.length((long) precision);
            }
            return;
        }
        if (sqlType == SqlType.DECIMAL && dataType instanceof DecimalType) {
            DecimalType decimal = (DecimalType) dataType;
            builder.precision(decimal.getPrecision());
            builder.scale(decimal.getScale());
            return;
        }
        if (sqlType == SqlType.TIME) {
            int p = Math.max(0, Math.min(scale > 0 ? scale : precision,
                    MAX_TIME_PRECISION));
            if (p > 0) {
                builder.precision(p);
            }
            return;
        }
        if (sqlType == SqlType.TIMESTAMP
                || sqlType == SqlType.TIMESTAMP_TZ) {
            int p = Math.max(0, Math.min(scale > 0 ? scale : precision,
                    MAX_TIMESTAMP_PRECISION));
            if (p > 0) {
                builder.precision(p);
            }
        }
    }

    private static String buildSourceType(String sourceType, int precision, int scale) {
        String raw = sourceType == null ? "" : sourceType.trim();
        String normalized = normalize(raw);
        String base = baseType(normalized);
        if (raw.isEmpty() || raw.contains("(")) {
            return raw;
        }
        if (isNumericBase(base)) {
            int p = precision <= 0 ? DEFAULT_NUMERIC_PRECISION : precision;
            int s = precision <= 0 ? DEFAULT_NUMERIC_SCALE : scale;
            return raw + "(" + p + "," + s + ")";
        }
        if (isLengthType(base)
                || "bit".equals(base)
                || "varbit".equals(base)
                || "bit varying".equals(base)) {
            return precision > 0 ? raw + "(" + precision + ")" : raw;
        }
        if (isTimeWithTimeZone(normalized, base)) {
            return scale > 0
                    ? "TIME(" + Math.min(scale, MAX_TIME_PRECISION) + ") WITH TIME ZONE"
                    : raw;
        }
        if (isTimestampWithTimeZone(normalized, base)) {
            return scale > 0
                    ? "TIMESTAMP(" + Math.min(scale, MAX_TIMESTAMP_PRECISION)
                    + ") WITH TIME ZONE"
                    : raw;
        }
        if (("time".equals(base) || base.startsWith("timestamp"))
                && scale > 0) {
            return raw + "(" + scale + ")";
        }
        return raw;
    }

    private static boolean canPreserve(Column column) {
        String sourceType = column.getSourceType();
        if (sourceType == null || sourceType.trim().isEmpty()) {
            return false;
        }
        String type = normalize(sourceType);
        String base = baseType(type);
        if (isTimestampWithTimeZone(type, base)
                || isTimeWithTimeZone(type, base)) {
            return false;
        }
        if (isNumericBase(base)
                && column.getDataType().getSqlType() == SqlType.STRING) {
            return false;
        }
        return "boolean".equals(base)
                || "bool".equals(base)
                || "tinyint".equals(base)
                || "smallint".equals(base)
                || "short".equals(base)
                || "integer".equals(base)
                || "int".equals(base)
                || "bigint".equals(base)
                || "longint".equals(base)
                || "float".equals(base)
                || "real".equals(base)
                || "double".equals(base)
                || "double precision".equals(base)
                || isNumericBase(base)
                || isStringLike(type, base)
                || "binary".equals(base)
                || "varbinary".equals(base)
                || "longvarbinary".equals(base)
                || "long varbinary".equals(base)
                || "blob".equals(base)
                || "raw".equals(base)
                || "bit".equals(base)
                || "varbit".equals(base)
                || "bit varying".equals(base)
                || "date".equals(base)
                || "time".equals(base)
                || base.startsWith("timestamp")
                || "datetime".equals(base);
    }

    private static boolean isOffsetTimeSource(Column column) {
        String type = normalize(column.getSourceType());
        return isTimeWithTimeZone(type, baseType(type));
    }

    private static boolean isStringLike(String type, String base) {
        return type.isEmpty()
                || base.contains("char")
                || "text".equals(base)
                || "clob".equals(base)
                || "nclob".equals(base)
                || "json".equals(base)
                || "xml".equals(base)
                || "guid".equals(base)
                || "uuid".equals(base)
                || "rowid".equals(base)
                || "interval".equals(base)
                || base.startsWith("interval ")
                || "point".equals(base)
                || "line".equals(base)
                || "lseg".equals(base)
                || "box".equals(base)
                || "path".equals(base)
                || "polygon".equals(base)
                || "circle".equals(base)
                || "array".equals(base)
                || type.endsWith("[]");
    }

    private static boolean isLengthType(String base) {
        return base.contains("char")
                || "binary".equals(base)
                || "varbinary".equals(base);
    }

    private static boolean isNumericBase(String base) {
        return "numeric".equals(base)
                || "decimal".equals(base)
                || "number".equals(base);
    }

    private static boolean isTimeWithTimeZone(String type, String base) {
        return ("time".equals(base) || base.startsWith("time "))
                && !base.startsWith("timestamp")
                && type.contains("with time zone")
                && !type.contains("without time zone");
    }

    private static boolean isTimestampWithTimeZone(String type, String base) {
        return (base.startsWith("timestamp") || base.startsWith("datetime"))
                && type.contains("with time zone")
                && !type.contains("without time zone");
    }

    private static String baseType(String type) {
        int parenthesis = type.indexOf('(');
        if (parenthesis < 0) {
            return type;
        }
        String before = type.substring(0, parenthesis).trim();
        int close = type.indexOf(')', parenthesis + 1);
        if (close >= 0 && close < type.length() - 1) {
            String suffix = type.substring(close + 1).trim();
            return suffix.isEmpty() ? before : before + " " + suffix;
        }
        return before;
    }

    private static boolean isIdentityDefault(Object defaultValue) {
        if (defaultValue == null) {
            return false;
        }
        String value = String.valueOf(defaultValue).toLowerCase(Locale.ROOT);
        return value.contains("identity") || value.contains("auto_increment");
    }

    private static String normalize(String value) {
        return value == null
                ? ""
                : value.trim().toLowerCase(Locale.ROOT).replaceAll("\\s+", " ");
    }

    private static String firstText(String first, String second) {
        return first != null && !first.trim().isEmpty() ? first : second;
    }

    private static Integer integer(ResultSet row, String name) {
        try {
            Object value = row.getObject(name);
            return value == null ? null : ((Number) value).intValue();
        } catch (Exception ignored) {
            return null;
        }
    }

    private static Object safeObject(ResultSet row, String name) {
        try {
            return row.getObject(name);
        } catch (SQLException ignored) {
            return null;
        }
    }

    private static String safeString(ResultSet row, String name) {
        try {
            return row.getString(name);
        } catch (SQLException ignored) {
            return null;
        }
    }

    private static int value(Integer value) {
        return value == null ? 0 : value;
    }
}
