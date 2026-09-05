package com.link.up.connector.jdbc.core.dialect.iris;

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

/** InterSystems IRIS JDBC type mapper for bounded/offline jobs. */
public final class IrisTypeMapper implements JdbcTypeMapper {

    static final int MAX_NUMERIC_SCALE = 18;
    static final int DEFAULT_NUMERIC_PRECISION = 15;
    static final int DEFAULT_NUMERIC_SCALE = 0;
    static final int MAX_TIME_PRECISION = 9;
    static final int MAX_POSIXTIME_PRECISION = 6;
    static final long MAX_BOUNDED_VARCHAR_LENGTH = 32_767L;
    static final long MAX_BOUNDED_BINARY_LENGTH = 32_749L;
    static final String NATIVE_ATTRIBUTE = "iris_native";

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

    /** Maps a JDBC DatabaseMetaData#getColumns row. */
    public Column toColumn(ResultSet row) throws SQLException {
        String name = row.getString("COLUMN_NAME");
        String typeName = row.getString("TYPE_NAME");
        Integer size = integer(row, "COLUMN_SIZE");
        Integer scale = integer(row, "DECIMAL_DIGITS");
        Integer nullable = integer(row, "NULLABLE");
        int jdbcType = value(integer(row, "DATA_TYPE"));
        int precision = value(size);
        int safeScale = value(scale);
        FluxDataType<?> type = mapType(jdbcType, typeName, precision, safeScale);
        Object defaultValue = safeObject(row, "COLUMN_DEF");

        Column.Builder builder = Column.builder(name, type)
                .nullable(nullable == null
                        || nullable != ResultSetMetaData.columnNoNulls)
                .defaultValue(defaultValue)
                .comment(safeString(row, "REMARKS"))
                .sourceType(buildSourceType(typeName, precision, safeScale))
                .attribute(NATIVE_ATTRIBUTE, "true");

        String auto = safeString(row, "IS_AUTOINCREMENT");
        boolean autoIncrement = "YES".equalsIgnoreCase(auto)
                || isAutoIncrementType(typeName);
        builder.autoIncrement(autoIncrement);
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
                return stringType(column);
            case BYTES:
                return binaryType(column);
            case DATE:
                return "DATE";
            case TIME:
                return timeType(column);
            case TIMESTAMP:
                return timestampType(column);
            case TIMESTAMP_TZ:
                // Core IRIS SQL has no offset-aware timestamp primitive. Keep
                // the ISO-8601 value rather than silently dropping its offset.
                return "VARCHAR(64)";
            default:
                throw new IllegalArgumentException(
                        "IRIS 不支持 Flux 类型：" + type
                                + "，column=" + column.getName());
        }
    }

    /** Package-visible for focused type contract tests. */
    FluxDataType<?> mapType(
            int jdbcType,
            String sourceType,
            int precision,
            int scale) {

        String type = normalize(sourceType);
        String base = baseType(type);

        switch (base) {
            case "null":
                return BasicType.NULL_TYPE;
            case "bit":
            case "boolean":
                return BasicType.BOOLEAN_TYPE;
            case "tinyint":
                return BasicType.BYTE_TYPE;
            case "smallint":
                return BasicType.SHORT_TYPE;
            case "mediumint":
            case "integer":
            case "int":
                return BasicType.INT_TYPE;
            case "bigint":
            case "serial":
            case "auto_increment":
            case "rowversion":
                return BasicType.LONG_TYPE;
            case "float":
                return BasicType.FLOAT_TYPE;
            case "double":
            case "real":
            case "double precision":
                return BasicType.DOUBLE_TYPE;
            case "numeric":
            case "decimal":
            case "dec":
            case "money":
            case "smallmoney":
                return numericType(precision, scale, false);
            case "number":
                return numericType(precision, scale, true);
            case "date":
                return BasicType.DATE_TYPE;
            case "time":
                return BasicType.TIME_TYPE;
            case "timestamp":
            case "posixtime":
            case "timestamp2":
            case "datetime":
            case "datetime2":
            case "smalldatetime":
                return BasicType.TIMESTAMP_TYPE;
            case "binary":
            case "binary varying":
            case "varbinary":
            case "raw":
            case "longvarbinary":
            case "blob":
            case "image":
            case "long binary":
            case "long raw":
                return BasicType.BYTES_TYPE;
            default:
                break;
        }

        if (isStringType(base)
                || "guid".equals(base)
                || "uniqueidentifier".equals(base)
                || "vector".equals(base)
                || "oref".equals(base)) {
            return BasicType.STRING_TYPE;
        }

        switch (jdbcType) {
            case Types.NULL:
                return BasicType.NULL_TYPE;
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
            case Types.FLOAT:
                return BasicType.FLOAT_TYPE;
            case Types.DOUBLE:
                return BasicType.DOUBLE_TYPE;
            case Types.NUMERIC:
            case Types.DECIMAL:
                return numericType(precision, scale, false);
            case Types.DATE:
                return BasicType.DATE_TYPE;
            case Types.TIME:
                return BasicType.TIME_TYPE;
            case Types.TIMESTAMP:
            case Types.TIMESTAMP_WITH_TIMEZONE:
                return BasicType.TIMESTAMP_TYPE;
            case Types.BINARY:
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
            case Types.BLOB:
                return BasicType.BYTES_TYPE;
            default:
                return BasicType.STRING_TYPE;
        }
    }

    private static FluxDataType<?> numericType(
            int precision,
            int scale,
            boolean numberAlias) {

        if (precision <= 0) {
            return numberAlias
                    ? BasicType.LONG_TYPE
                    : new DecimalType(
                            DEFAULT_NUMERIC_PRECISION,
                            DEFAULT_NUMERIC_SCALE);
        }
        if (!validNumericShape(precision, scale)) {
            return BasicType.STRING_TYPE;
        }
        return new DecimalType(precision, scale);
    }

    private static boolean validNumericShape(int precision, int scale) {
        return precision > 0
                && scale >= 0
                && scale <= MAX_NUMERIC_SCALE
                && scale <= precision
                && precision <= 19 + scale;
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
                ? DEFAULT_NUMERIC_PRECISION
                : precisionValue;
        int scale = scaleValue == null
                ? DEFAULT_NUMERIC_SCALE
                : scaleValue;
        if (!validNumericShape(precision, scale)) {
            throw new IllegalArgumentException(
                    "IRIS NUMERIC 必须满足 scale 0..18 且 precision <= 19 + scale，column="
                            + column.getName() + "，precision=" + precision
                            + "，scale=" + scale);
        }
        return "NUMERIC(" + precision + "," + scale + ")";
    }

    private static String stringType(Column column) {
        Long length = column.getLength();
        if (length == null || length <= 0
                || length > MAX_BOUNDED_VARCHAR_LENGTH) {
            return "LONGVARCHAR";
        }
        return "VARCHAR(" + length + ")";
    }

    private static String binaryType(Column column) {
        Long length = column.getLength();
        if (length == null || length <= 0
                || length > MAX_BOUNDED_BINARY_LENGTH) {
            return "LONGVARBINARY";
        }
        return "VARBINARY(" + length + ")";
    }

    private static String timeType(Column column) {
        Integer precision = column.getPrecision();
        if (precision == null || precision <= 0) {
            return "TIME";
        }
        if (precision > MAX_TIME_PRECISION) {
            throw new IllegalArgumentException(
                    "IRIS TIME precision 最大为 9，column="
                            + column.getName() + "，precision=" + precision);
        }
        return "TIME(" + precision + ")";
    }

    private static String timestampType(Column column) {
        Integer precision = column.getPrecision();
        if (precision != null && precision > MAX_TIME_PRECISION) {
            throw new IllegalArgumentException(
                    "IRIS TIMESTAMP2 precision 最大为 9，column="
                            + column.getName() + "，precision=" + precision);
        }
        // TIMESTAMP2 maps to %Library.TimeStamp and can preserve nanoseconds;
        // legacy TIMESTAMP/POSIXTIME only preserve up to microseconds.
        return "TIMESTAMP2";
    }

    private static void applyProperties(
            Column.Builder builder,
            FluxDataType<?> dataType,
            String sourceType,
            int precision,
            int scale) {

        SqlType sqlType = dataType.getSqlType();
        String base = baseType(normalize(sourceType));
        if (sqlType == SqlType.STRING) {
            if (isBoundedString(base) && precision > 0) {
                builder.length((long) precision);
            } else if (("guid".equals(base)
                    || "uniqueidentifier".equals(base)) && precision > 0) {
                builder.length((long) precision);
            }
            return;
        }
        if (sqlType == SqlType.BYTES) {
            if (isBoundedBinary(base) && precision > 0) {
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
            if (scale > 0) {
                builder.precision(Math.min(scale, MAX_TIME_PRECISION));
            }
            return;
        }
        if (sqlType == SqlType.TIMESTAMP) {
            int max = "timestamp".equals(base) || "posixtime".equals(base)
                    ? MAX_POSIXTIME_PRECISION
                    : MAX_TIME_PRECISION;
            if (scale > 0) {
                builder.precision(Math.min(scale, max));
            }
        }
    }

    private static String buildSourceType(
            String sourceType,
            int precision,
            int scale) {

        String raw = sourceType == null ? "" : sourceType.trim();
        String base = baseType(normalize(raw));
        if (raw.isEmpty() || raw.contains("(")) {
            return raw;
        }
        if (isNumericBase(base) && precision > 0) {
            return raw + "(" + precision + "," + scale + ")";
        }
        if (isBoundedString(base) || isBoundedBinary(base)) {
            return precision > 0 ? raw + "(" + precision + ")" : raw;
        }
        if ("time".equals(base) && scale > 0) {
            return raw + "(" + Math.min(scale, MAX_TIME_PRECISION) + ")";
        }
        return raw;
    }

    private static boolean canPreserve(String sourceType) {
        String type = normalize(sourceType);
        String base = baseType(type);
        if (type.isEmpty()
                || "rowversion".equals(base)
                || "identity".equals(base)
                || "vector".equals(base)
                || "oref".equals(base)) {
            return false;
        }
        return "bit".equals(base)
                || "tinyint".equals(base)
                || "smallint".equals(base)
                || "mediumint".equals(base)
                || "integer".equals(base)
                || "int".equals(base)
                || "bigint".equals(base)
                || "serial".equals(base)
                || "float".equals(base)
                || "double".equals(base)
                || "real".equals(base)
                || "double precision".equals(base)
                || isNumericBase(base)
                || isStringType(base)
                || "guid".equals(base)
                || "uniqueidentifier".equals(base)
                || "date".equals(base)
                || "time".equals(base)
                || "timestamp".equals(base)
                || "timestamp2".equals(base)
                || "posixtime".equals(base)
                || "datetime".equals(base)
                || "datetime2".equals(base)
                || "smalldatetime".equals(base)
                || isBoundedBinary(base)
                || isLongBinary(base);
    }

    private static boolean isNumericBase(String base) {
        return "numeric".equals(base)
                || "decimal".equals(base)
                || "dec".equals(base)
                || "number".equals(base)
                || "money".equals(base)
                || "smallmoney".equals(base);
    }

    private static boolean isStringType(String base) {
        return isBoundedString(base)
                || "ntext".equals(base)
                || "clob".equals(base)
                || "long varchar".equals(base)
                || "long".equals(base)
                || "longtext".equals(base)
                || "mediumtext".equals(base)
                || "text".equals(base)
                || "longvarchar".equals(base)
                || "sysname".equals(base);
    }

    private static boolean isBoundedString(String base) {
        return "char".equals(base)
                || "character".equals(base)
                || "char varying".equals(base)
                || "character varying".equals(base)
                || "national char".equals(base)
                || "national char varying".equals(base)
                || "national character".equals(base)
                || "national character varying".equals(base)
                || "national varchar".equals(base)
                || "nchar".equals(base)
                || "nvarchar".equals(base)
                || "varchar".equals(base)
                || "varchar2".equals(base);
    }

    private static boolean isBoundedBinary(String base) {
        return "binary".equals(base)
                || "binary varying".equals(base)
                || "varbinary".equals(base)
                || "raw".equals(base);
    }

    private static boolean isLongBinary(String base) {
        return "longvarbinary".equals(base)
                || "blob".equals(base)
                || "image".equals(base)
                || "long binary".equals(base)
                || "long raw".equals(base);
    }

    private static boolean isAutoIncrementType(String typeName) {
        String base = baseType(normalize(typeName));
        return "serial".equals(base)
                || "identity".equals(base)
                || "auto_increment".equals(base);
    }

    private static String baseType(String value) {
        int parenthesis = value.indexOf('(');
        return parenthesis >= 0
                ? value.substring(0, parenthesis).trim()
                : value;
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
