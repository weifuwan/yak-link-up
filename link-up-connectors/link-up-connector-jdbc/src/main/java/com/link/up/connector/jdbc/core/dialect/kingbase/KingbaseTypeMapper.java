package com.link.up.connector.jdbc.core.dialect.kingbase;

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
 * KingbaseES JDBC type mapper for bounded/offline jobs.
 *
 * <p>KingbaseES is PostgreSQL compatible but also exposes MySQL/Oracle/SQLServer
 * compatibility types. Values that Flux cannot model without loss (for example
 * unconstrained NUMERIC, MONEY or TIME WITH TIME ZONE) are carried as exact text.</p>
 */
public final class KingbaseTypeMapper implements JdbcTypeMapper {

    static final int MAX_NUMERIC_PRECISION = 1000;
    static final int MAX_TIME_PRECISION = 6;
    static final long MAX_VARCHAR_LENGTH = 65_535L;
    static final String NATIVE_ATTRIBUTE = "kingbase_native";

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
        Object defaultValue = row.getObject("COLUMN_DEF");

        Column.Builder builder = Column.builder(name, type)
                .nullable(nullable == null
                        || nullable != ResultSetMetaData.columnNoNulls)
                .defaultValue(defaultValue)
                .comment(safeString(row, "REMARKS"))
                .sourceType(buildSourceType(typeName, precision, safeScale))
                .attribute(NATIVE_ATTRIBUTE, "true");

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
                        "KingbaseES 不支持 Flux 类型：" + type + "，column=" + column.getName());
        }
    }

    /** Package-visible for focused type contract tests. */
    FluxDataType<?> mapType(
            int jdbcType,
            String sourceType,
            int precision,
            int scale) {

        String type = normalize(sourceType);
        String base = baseWithoutUnsigned(baseType(type));
        boolean unsigned = type.contains("unsigned");

        if (isTimeWithTimeZone(type, base)) {
            return BasicType.STRING_TYPE;
        }
        if (isTimestampWithTimeZone(type, base)) {
            return BasicType.TIMESTAMP_TZ_TYPE;
        }
        if (type.startsWith("timestamp")
                || type.startsWith("datetime")
                || "smalldatetime".equals(base)) {
            return BasicType.TIMESTAMP_TYPE;
        }

        if ("bit".equals(base)
                || "varbit".equals(base)
                || "bit varying".equals(base)) {
            // Kingbase BIT is a bit-string type, not a boolean contract.
            return BasicType.STRING_TYPE;
        }

        if (unsigned) {
            if ("tinyint".equals(base) || "int1".equals(base)) {
                return BasicType.SHORT_TYPE;
            }
            if ("smallint".equals(base) || "int2".equals(base)
                    || "mediumint".equals(base) || "middleint".equals(base)
                    || "int3".equals(base)) {
                return BasicType.INT_TYPE;
            }
            if ("integer".equals(base) || "int".equals(base) || "int4".equals(base)) {
                return BasicType.LONG_TYPE;
            }
            if ("bigint".equals(base) || "int8".equals(base)) {
                return new DecimalType(20, 0);
            }
        }

        switch (base) {
            case "bool":
            case "boolean":
                return BasicType.BOOLEAN_TYPE;
            case "tinyint":
            case "int1":
                return BasicType.BYTE_TYPE;
            case "smallint":
            case "int2":
            case "smallserial":
            case "serial2":
                return BasicType.SHORT_TYPE;
            case "mediumint":
            case "middleint":
            case "int3":
            case "integer":
            case "int":
            case "int4":
            case "serial":
            case "serial4":
            case "year":
                return BasicType.INT_TYPE;
            case "bigint":
            case "int8":
            case "bigserial":
            case "serial8":
            case "oid":
                return BasicType.LONG_TYPE;
            case "float4":
            case "real":
            case "binary_float":
                return BasicType.FLOAT_TYPE;
            case "float":
                return precision > 0 && precision <= 24
                        ? BasicType.FLOAT_TYPE
                        : BasicType.DOUBLE_TYPE;
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
                // Display/scale depends on monetary configuration; preserve exact text.
                return BasicType.STRING_TYPE;
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
            case Types.BIT:
                return BasicType.STRING_TYPE;
            case Types.BINARY:
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
            case Types.BLOB:
                return BasicType.BYTES_TYPE;
            case Types.CHAR:
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
            case Types.CLOB:
            case Types.NCHAR:
            case Types.NVARCHAR:
            case Types.LONGNVARCHAR:
            case Types.NCLOB:
            case Types.SQLXML:
                return BasicType.STRING_TYPE;
            default:
                return BasicType.STRING_TYPE;
        }
    }

    private static FluxDataType<?> numericType(int precision, int scale) {
        if (precision <= 0 || precision > MAX_NUMERIC_PRECISION
                || scale < 0 || scale > precision) {
            return BasicType.STRING_TYPE;
        }
        return new DecimalType(precision, scale);
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
                    "KingbaseES NUMERIC precision 必须在 1..1000，column="
                            + column.getName() + "，precision=" + precision);
        }
        if (scale < 0 || scale > precision) {
            throw new IllegalArgumentException(
                    "KingbaseES NUMERIC scale 非法，column=" + column.getName()
                            + "，precision=" + precision + "，scale=" + scale);
        }
        return "NUMERIC(" + precision + "," + scale + ")";
    }

    private static String stringType(Column column) {
        Long length = column.getLength();
        if (length == null || length <= 0 || length > MAX_VARCHAR_LENGTH) {
            return "TEXT";
        }
        return "VARCHAR(" + length + ")";
    }

    private static String temporal(String type, Integer precision) {
        return precision == null || precision <= 0
                ? type
                : type + "(" + Math.min(precision, MAX_TIME_PRECISION) + ")";
    }

    private static void applyProperties(
            Column.Builder builder,
            FluxDataType<?> dataType,
            String sourceType,
            int precision,
            int scale) {

        SqlType sqlType = dataType.getSqlType();
        String normalized = normalize(sourceType);
        String base = baseWithoutUnsigned(baseType(normalized));

        if (sqlType == SqlType.STRING || sqlType == SqlType.BYTES) {
            if (!isNumericBase(base)
                    && !"money".equals(base)
                    && !isTimeWithTimeZone(normalized, base)
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
        String base = baseWithoutUnsigned(baseType(normalized));
        if (raw.isEmpty() || raw.contains("(")) {
            return raw;
        }
        if (isNumericBase(base)) {
            return precision > 0
                    ? raw + "(" + precision + "," + scale + ")"
                    : raw;
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
                    ? "TIMESTAMP(" + Math.min(scale, MAX_TIME_PRECISION) + ") WITH TIME ZONE"
                    : raw;
        }
        if (base.startsWith("timestamp")
                || "time".equals(base)
                || "datetime".equals(base)) {
            return scale > 0
                    ? raw + "(" + Math.min(scale, MAX_TIME_PRECISION) + ")"
                    : raw;
        }
        return raw;
    }

    private static boolean isTimeWithTimeZone(String type, String base) {
        return "timetz".equals(base)
                || (type.startsWith("time")
                && !type.startsWith("timestamp")
                && type.contains("time zone"));
    }

    private static boolean isTimestampWithTimeZone(String type, String base) {
        return "timestamptz".equals(base)
                || "datetimeoffset".equals(base)
                || ((type.startsWith("timestamp") || type.startsWith("datetime"))
                && type.contains("time zone"));
    }

    private static boolean isStringLike(String type, String base) {
        return type.isEmpty()
                || type.contains("char")
                || "text".equals(base)
                || "tinytext".equals(base)
                || "mediumtext".equals(base)
                || "longtext".equals(base)
                || "clob".equals(base)
                || "nclob".equals(base)
                || "long".equals(base)
                || "name".equals(base)
                || "uuid".equals(base)
                || "json".equals(base)
                || "jsonb".equals(base)
                || "xml".equals(base)
                || "xmltype".equals(base)
                || "inet".equals(base)
                || "cidr".equals(base)
                || base.startsWith("macaddr")
                || "interval".equals(base)
                || "rowid".equals(base)
                || "urowid".equals(base)
                || "bfile".equals(base)
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
                || "nchar".equals(base)
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
        String base = baseWithoutUnsigned(baseType(type));
        return isNumericBase(base)
                || isLengthType(base)
                || isStringLike(type, base)
                || isTimeWithTimeZone(type, base)
                || isTimestampWithTimeZone(type, base)
                || "bit".equals(base)
                || "varbit".equals(base)
                || "bit varying".equals(base)
                || "bool".equals(base)
                || "boolean".equals(base)
                || "tinyint".equals(base)
                || "int1".equals(base)
                || "smallint".equals(base)
                || "int2".equals(base)
                || "mediumint".equals(base)
                || "middleint".equals(base)
                || "int3".equals(base)
                || "integer".equals(base)
                || "int".equals(base)
                || "int4".equals(base)
                || "bigint".equals(base)
                || "int8".equals(base)
                || "serial".equals(base)
                || "bigserial".equals(base)
                || "real".equals(base)
                || "float".equals(base)
                || "float4".equals(base)
                || "float8".equals(base)
                || "double".equals(base)
                || "double precision".equals(base)
                || "money".equals(base)
                || "date".equals(base)
                || "time".equals(base)
                || base.startsWith("timestamp")
                || base.startsWith("datetime")
                || "bytea".equals(base)
                || "blob".equals(base)
                || "image".equals(base);
    }

    private static String baseType(String type) {
        int parenthesis = type.indexOf('(');
        return parenthesis >= 0 ? type.substring(0, parenthesis).trim() : type;
    }

    private static String baseWithoutUnsigned(String base) {
        return base.replaceAll("\\s+unsigned$", "").trim();
    }

    private static String normalize(String value) {
        return value == null
                ? ""
                : value.trim().toLowerCase(Locale.ROOT).replaceAll("\\s+", " ");
    }

    private static String firstText(String first, String second) {
        return hasText(first) ? first : second;
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

    private static boolean hasText(String value) {
        return value != null && !value.trim().isEmpty();
    }
}
