package com.link.up.connector.jdbc.core.dialect.opengauss;

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

/**
 * openGauss JDBC type mapper for bounded/offline jobs.
 *
 * <p>openGauss is PostgreSQL compatible but also exposes compatibility types
 * such as NUMBER/VARCHAR2/CLOB/TINYINT/INT16. Unconstrained NUMERIC and
 * negative-scale NUMERIC are carried as exact text because Flux DecimalType
 * cannot represent an unbounded or negative scale contract.</p>
 */
public final class OpenGaussTypeMapper implements JdbcTypeMapper {

    static final int MAX_NUMERIC_PRECISION = 1000;
    static final int MAX_TIME_PRECISION = 6;
    static final long MAX_VARCHAR_BYTES = 10_485_760L;
    private static final long MAX_SAFE_VARCHAR_CHARS = MAX_VARCHAR_BYTES / 4L;

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
        Integer nullable = integer(row, "NULLABLE");
        int jdbcType = value(integer(row, "DATA_TYPE"));
        int precision = value(size);
        int safeScale = value(scale);

        FluxDataType<?> type = mapType(jdbcType, typeName, precision, safeScale);
        Object defaultValue = row.getObject("COLUMN_DEF");

        Column.Builder builder = Column.builder(name, type)
                .nullable(nullable == null
                        || nullable != ResultSetMetaData.columnNoNulls)
                .defaultValue(defaultValue)
                .comment(safeString(row, "REMARKS"))
                .sourceType(buildSourceType(typeName, precision, safeScale));

        String auto = safeString(row, "IS_AUTOINCREMENT");
        boolean identity = "YES".equalsIgnoreCase(auto)
                || (defaultValue != null
                && String.valueOf(defaultValue)
                .trim()
                .toLowerCase(Locale.ROOT)
                .startsWith("nextval("));
        builder.autoIncrement(identity);
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
                return "REAL";
            case DOUBLE:
                return "DOUBLE PRECISION";
            case DECIMAL:
                return decimalType(column);
            case STRING:
                return stringType(column);
            case BYTES:
                return "BYTEA";
            case DATE:
                return "DATE";
            case TIME:
                return temporal("TIME", column.getPrecision());
            case TIMESTAMP:
                return temporal("TIMESTAMP", column.getPrecision());
            case TIMESTAMP_TZ:
                return temporal("TIMESTAMP", column.getPrecision()) + " WITH TIME ZONE";
            default:
                throw new IllegalArgumentException(
                        "openGauss 不支持 Flux 类型：" + type + "，column=" + column.getName());
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

        if ("time with time zone".equals(type)
                || "timetz".equals(base)) {
            // Flux has no TIME_TZ primitive. Keep the full offset-bearing value.
            return BasicType.STRING_TYPE;
        }
        if ("timestamp with time zone".equals(type)
                || "timestamptz".equals(base)) {
            return BasicType.TIMESTAMP_TZ_TYPE;
        }
        if (type.startsWith("timestamp")
                || "datetime".equals(base)
                || "smalldatetime".equals(base)) {
            return BasicType.TIMESTAMP_TYPE;
        }

        switch (base) {
            case "bool":
            case "boolean":
            case "bit":
                return BasicType.BOOLEAN_TYPE;
            case "int1":
            case "tinyint":
            case "byte":
                return BasicType.BYTE_TYPE;
            case "int2":
            case "smallint":
            case "smallserial":
            case "serial2":
                return BasicType.SHORT_TYPE;
            case "int4":
            case "integer":
            case "int":
            case "serial":
            case "serial4":
                return BasicType.INT_TYPE;
            case "int8":
            case "bigint":
            case "bigserial":
            case "serial8":
            case "oid":
                return BasicType.LONG_TYPE;
            case "int16":
                return new DecimalType(39, 0);
            case "float4":
            case "real":
                return BasicType.FLOAT_TYPE;
            case "float8":
            case "double":
            case "double precision":
            case "binary_double":
                return BasicType.DOUBLE_TYPE;
            case "numeric":
            case "decimal":
            case "dec":
            case "number":
            case "fixed":
                return numericType(precision, scale);
            case "money":
                return new DecimalType(19, 2);
            case "date":
                return BasicType.DATE_TYPE;
            case "time":
                return BasicType.TIME_TYPE;
            case "bytea":
            case "blob":
            case "raw":
            case "binary":
            case "varbinary":
            case "longvarbinary":
            case "image":
                return BasicType.BYTES_TYPE;
            default:
                break;
        }

        if (isStringLike(type, base)) {
            return BasicType.STRING_TYPE;
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
            case Types.FLOAT:
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
            default:
                return BasicType.STRING_TYPE;
        }
    }

    private static FluxDataType<?> numericType(int precision, int scale) {
        if (precision <= 0 || scale < 0) {
            return BasicType.STRING_TYPE;
        }
        if (precision > MAX_NUMERIC_PRECISION) {
            return BasicType.STRING_TYPE;
        }
        int safeScale = Math.min(scale, precision);
        return new DecimalType(precision, safeScale);
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

        int precision = precisionValue == null ? 38 : precisionValue;
        int scale = scaleValue == null ? 0 : scaleValue;

        if (precision <= 0 || precision > MAX_NUMERIC_PRECISION) {
            throw new IllegalArgumentException(
                    "openGauss NUMERIC precision 必须在 1..1000，column="
                            + column.getName() + "，precision=" + precision);
        }
        if (scale < 0 || scale > precision) {
            throw new IllegalArgumentException(
                    "openGauss NUMERIC scale 非法，column=" + column.getName()
                            + "，precision=" + precision + "，scale=" + scale);
        }
        return "NUMERIC(" + precision + "," + scale + ")";
    }

    private static String stringType(Column column) {
        Long length = column.getLength();
        if (length == null || length <= 0 || length > MAX_SAFE_VARCHAR_CHARS) {
            return "TEXT";
        }
        // Explicit CHAR semantics avoids byte-length truncation outside PG mode.
        return "VARCHAR(" + length + " CHAR)";
    }

    private static String temporal(String type, Integer precision) {
        if (precision == null || precision <= 0) {
            return type;
        }
        return type + "(" + Math.min(precision, MAX_TIME_PRECISION) + ")";
    }

    private static void applyProperties(
            Column.Builder builder,
            FluxDataType<?> dataType,
            String sourceType,
            int precision,
            int scale) {

        SqlType sqlType = dataType.getSqlType();
        String base = baseType(normalize(sourceType));

        if (sqlType == SqlType.STRING || sqlType == SqlType.BYTES) {
            if (!isNumericBase(base)
                    && !"time with time zone".equals(normalize(sourceType))
                    && !"timetz".equals(base)
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

        if (sqlType == SqlType.TIME
                || sqlType == SqlType.TIMESTAMP
                || sqlType == SqlType.TIMESTAMP_TZ) {
            int p = Math.max(0, Math.min(
                    scale > 0 ? scale : precision,
                    MAX_TIME_PRECISION));
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
            return precision > 0
                    ? raw + "(" + precision + "," + scale + ")"
                    : raw;
        }
        if (isLengthType(base)) {
            return precision > 0 ? raw + "(" + precision + ")" : raw;
        }
        if (base.startsWith("timestamp")
                || "time".equals(base)
                || "timetz".equals(base)
                || "datetime".equals(base)) {
            return scale > 0 ? raw + "(" + Math.min(scale, MAX_TIME_PRECISION) + ")" : raw;
        }
        return raw;
    }

    private static boolean isStringLike(String type, String base) {
        return type.isEmpty()
                || type.contains("char")
                || "text".equals(base)
                || "clob".equals(base)
                || "long".equals(base)
                || "name".equals(base)
                || "uuid".equals(base)
                || "json".equals(base)
                || "jsonb".equals(base)
                || "xml".equals(base)
                || "inet".equals(base)
                || "cidr".equals(base)
                || base.startsWith("macaddr")
                || "interval".equals(base)
                || "rowid".equals(base)
                || "urowid".equals(base)
                || type.startsWith("_")
                || "array".equals(base)
                || "user-defined".equals(base);
    }

    private static boolean isLengthType(String base) {
        return "char".equals(base)
                || "character".equals(base)
                || "bpchar".equals(base)
                || "varchar".equals(base)
                || "varchar2".equals(base)
                || "nvarchar".equals(base)
                || "nvarchar2".equals(base)
                || "character varying".equals(base)
                || "binary".equals(base)
                || "varbinary".equals(base)
                || "raw".equals(base);
    }

    private static boolean isNumericBase(String base) {
        return "numeric".equals(base)
                || "decimal".equals(base)
                || "dec".equals(base)
                || "number".equals(base)
                || "fixed".equals(base);
    }

    private static boolean canPreserve(String sourceType) {
        String type = normalize(sourceType);
        if (type.isEmpty()) {
            return false;
        }
        String base = baseType(type);
        return isNumericBase(base)
                || isLengthType(base)
                || isStringLike(type, base)
                || "bool".equals(base)
                || "boolean".equals(base)
                || "bit".equals(base)
                || "int1".equals(base)
                || "tinyint".equals(base)
                || "smallint".equals(base)
                || "int2".equals(base)
                || "integer".equals(base)
                || "int".equals(base)
                || "int4".equals(base)
                || "bigint".equals(base)
                || "int8".equals(base)
                || "int16".equals(base)
                || base.contains("serial")
                || "real".equals(base)
                || "float4".equals(base)
                || "float8".equals(base)
                || "double".equals(base)
                || "double precision".equals(base)
                || "binary_double".equals(base)
                || "money".equals(base)
                || "date".equals(base)
                || "time".equals(base)
                || "timetz".equals(base)
                || type.contains("time zone")
                || base.startsWith("timestamp")
                || "datetime".equals(base)
                || "bytea".equals(base)
                || "blob".equals(base)
                || "longvarbinary".equals(base)
                || "image".equals(base);
    }

    private static String baseType(String type) {
        int parenthesis = type.indexOf('(');
        return parenthesis >= 0 ? type.substring(0, parenthesis).trim() : type;
    }

    private static String normalize(String value) {
        return value == null
                ? ""
                : value.trim().toLowerCase(Locale.ROOT).replaceAll("\\s+", " ");
    }

    private static String firstText(String first, String second) {
        return first != null && !first.trim().isEmpty() ? first : second;
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
