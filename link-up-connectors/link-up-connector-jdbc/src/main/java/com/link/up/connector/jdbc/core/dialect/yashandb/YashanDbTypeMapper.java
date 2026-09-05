package com.link.up.connector.jdbc.core.dialect.yashandb;

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
 * YashanDB JDBC type mapper for bounded/offline jobs.
 *
 * <p>YashanDB has Oracle-like NUMBER/LOB/date-time types, native integer
 * types, MySQL compatibility types and BIT values whose JDBC representation
 * depends on bit width. The mapper prefers exact values over lossy coercion:
 * NUMBER shapes that Flux DecimalType cannot represent and wide TIME values
 * are carried as text.</p>
 */
public final class YashanDbTypeMapper implements JdbcTypeMapper {

    static final int MAX_NUMBER_PRECISION = 38;
    static final int MAX_STORED_FRACTIONAL_SECONDS = 6;
    static final long MAX_RAW_COLUMN_LENGTH = 8_000L;
    static final long MAX_VARCHAR_BYTES = 65_534L;
    static final long MAX_SAFE_VARCHAR_CHARS = MAX_VARCHAR_BYTES / 4L;

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
                .nullable(metadata.isNullable(columnIndex)
                        != ResultSetMetaData.columnNoNulls)
                .sourceType(buildSourceType(sourceType, precision, scale));

        applyProperties(builder, type, sourceType, precision, scale);
        return builder.build();
    }

    /** Maps one {@link java.sql.DatabaseMetaData#getColumns} row. */
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
                return numberType(column);
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
                return temporalType("TIMESTAMP", column.getPrecision())
                        + " WITH TIME ZONE";
            default:
                throw new IllegalArgumentException(
                        "YashanDB 不支持 Flux 类型：" + type
                                + "，column=" + column.getName());
        }
    }

    /** Package-visible for focused type contract tests. */
    FluxDataType<?> mapType(
            int jdbcType,
            String sourceType,
            int precision,
            int scale) {

        String normalized = normalize(sourceType);
        String base = baseType(normalized);

        if (isTimeWithTimeZone(normalized)) {
            // Flux has no TIME_TZ and YashanDB TIME can exceed the 24-hour
            // LocalTime range. Preserve the server-rendered value exactly.
            return BasicType.STRING_TYPE;
        }
        if (isTimestampWithTimeZone(normalized)) {
            return BasicType.TIMESTAMP_TZ_TYPE;
        }
        if (isTimestampWithLocalTimeZone(normalized)) {
            return BasicType.TIMESTAMP_TYPE;
        }
        if ("TIMESTAMP".equals(base)
                || "DATETIME".equals(base)
                || "DATETIME2".equals(base)
                || "SMALLDATETIME".equals(base)) {
            return BasicType.TIMESTAMP_TYPE;
        }
        if ("DATETIMEOFFSET".equals(base)) {
            return BasicType.TIMESTAMP_TZ_TYPE;
        }
        if ("DATE".equals(base)) {
            // YashanDB DATE stores date and time to seconds.
            return BasicType.TIMESTAMP_TYPE;
        }
        if ("TIME".equals(base)) {
            // Native TIME accepts approximately +/-838 hours, outside
            // java.time.LocalTime. Keep it as exact text on reads.
            return BasicType.STRING_TYPE;
        }

        switch (base) {
            case "BINARY_TINYINT":
            case "TINYINT":
            case "BYTE":
                return BasicType.BYTE_TYPE;
            case "BINARY_SMALLINT":
            case "SMALLINT":
                return BasicType.SHORT_TYPE;
            case "BINARY_INTEGER":
            case "INTEGER":
            case "INT":
            case "PLS_INTEGER":
                return BasicType.INT_TYPE;
            case "MEDIUMINT":
            case "INT3":
                return BasicType.INT_TYPE;
            case "BINARY_BIGINT":
            case "BIGINT":
                return BasicType.LONG_TYPE;
            case "TINYINT UNSIGNED":
            case "UTINYINT":
                return BasicType.SHORT_TYPE;
            case "SMALLINT UNSIGNED":
            case "USMALLINT":
                return BasicType.INT_TYPE;
            case "MEDIUMINT UNSIGNED":
            case "INT UNSIGNED":
            case "INTEGER UNSIGNED":
            case "UINT":
                return BasicType.LONG_TYPE;
            case "BIGINT UNSIGNED":
            case "UBIGINT":
                return new DecimalType(20, 0);
            case "YEAR":
                return BasicType.INT_TYPE;
            case "FLOAT":
            case "REAL":
            case "BINARY_FLOAT":
                return BasicType.FLOAT_TYPE;
            case "DOUBLE":
            case "DOUBLE PRECISION":
            case "BINARY_DOUBLE":
                return BasicType.DOUBLE_TYPE;
            case "NUMBER":
            case "NUMERIC":
            case "DECIMAL":
            case "DEC":
            case "FIXED":
                return decimalOrText(precision, scale);
            case "BOOLEAN":
            case "BOOL":
                return BasicType.BOOLEAN_TYPE;
            case "BIT":
            case "BIT VARYING":
            case "VARBIT":
                return precision == 1
                        ? BasicType.BOOLEAN_TYPE
                        : BasicType.BYTES_TYPE;
            case "RAW":
            case "LONG RAW":
            case "BLOB":
            case "BINARY":
            case "VARBINARY":
            case "LONGVARBINARY":
            case "TINYBLOB":
            case "MEDIUMBLOB":
            case "LONGBLOB":
            case "IMAGE":
                return BasicType.BYTES_TYPE;
            default:
                break;
        }

        if (isStringLike(normalized, base)) {
            return BasicType.STRING_TYPE;
        }

        switch (jdbcType) {
            case Types.BOOLEAN:
                return BasicType.BOOLEAN_TYPE;
            case Types.BIT:
                return precision == 1
                        ? BasicType.BOOLEAN_TYPE
                        : BasicType.BYTES_TYPE;
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
                return decimalOrText(precision, scale);
            case Types.DATE:
                return BasicType.TIMESTAMP_TYPE;
            case Types.TIME:
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

    private static FluxDataType<?> decimalOrText(int precision, int scale) {
        // YashanDB NUMBER permits negative scale and scale greater than
        // precision. Flux DecimalType deliberately does not. Do not narrow.
        if (precision <= 0
                || precision > MAX_NUMBER_PRECISION
                || scale < 0
                || scale > precision) {
            return BasicType.STRING_TYPE;
        }
        return new DecimalType(precision, scale);
    }

    private static String numberType(Column column) {
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

        int precision = precisionValue == null ? MAX_NUMBER_PRECISION : precisionValue;
        int scale = scaleValue == null ? 0 : scaleValue;
        if (precision <= 0 || precision > MAX_NUMBER_PRECISION) {
            throw new IllegalArgumentException(
                    "YashanDB NUMBER precision 必须在 1..38，column="
                            + column.getName() + "，precision=" + precision);
        }
        if (scale < 0 || scale > precision) {
            throw new IllegalArgumentException(
                    "Flux DECIMAL 无法安全写入 YashanDB NUMBER，column="
                            + column.getName() + "，precision=" + precision
                            + "，scale=" + scale);
        }
        return "NUMBER(" + precision + "," + scale + ")";
    }

    private static String stringType(Column column) {
        Long length = column.getLength();
        if (length == null || length <= 0 || length > MAX_SAFE_VARCHAR_CHARS) {
            return "CLOB";
        }
        return "VARCHAR(" + length + " CHAR)";
    }

    private static String binaryType(Column column) {
        Long length = column.getLength();
        if (length == null || length <= 0 || length > MAX_RAW_COLUMN_LENGTH) {
            return "BLOB";
        }
        return "RAW(" + length + ")";
    }

    private static String temporalType(String type, Integer precision) {
        if (precision == null || precision <= 0) {
            return type;
        }
        if (precision > MAX_STORED_FRACTIONAL_SECONDS) {
            throw new IllegalArgumentException(
                    "YashanDB 实际只保留 6 位微秒精度，不能静默写入 "
                            + type + "(" + precision + ")");
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
        String normalized = normalize(sourceType);
        String base = baseType(normalized);

        if (sqlType == SqlType.STRING) {
            if (isBoundedCharacter(base) && precision > 0) {
                builder.length((long) precision);
            } else if ("ROWID".equals(base) && precision > 0) {
                builder.length((long) precision);
            }
            return;
        }

        if (sqlType == SqlType.BYTES) {
            if (("RAW".equals(base)
                    || "BINARY".equals(base)
                    || "VARBINARY".equals(base))
                    && precision > 0) {
                builder.length((long) precision);
            } else if (("BIT".equals(base)
                    || "BIT VARYING".equals(base)
                    || "VARBIT".equals(base))
                    && precision > 1) {
                builder.length((long) ((precision + 7) / 8));
            }
            return;
        }

        if (sqlType == SqlType.DECIMAL && dataType instanceof DecimalType) {
            DecimalType decimal = (DecimalType) dataType;
            builder.precision(decimal.getPrecision());
            builder.scale(decimal.getScale());
            return;
        }

        if (sqlType == SqlType.TIMESTAMP
                || sqlType == SqlType.TIMESTAMP_TZ) {
            int p = Math.max(0, Math.min(
                    scale > 0 ? scale : precision,
                    MAX_STORED_FRACTIONAL_SECONDS));
            if (p > 0) {
                builder.precision(p);
            }
        }
    }

    private static String buildSourceType(String sourceType, int precision, int scale) {
        String raw = sourceType == null ? "" : sourceType.trim();
        if (raw.isEmpty() || raw.indexOf('(') >= 0) {
            return raw;
        }

        String normalized = normalize(raw);
        String base = baseType(normalized);
        if (isNumberBase(base)) {
            if (precision <= 0) {
                return raw;
            }
            return raw + "(" + precision + "," + scale + ")";
        }
        if (("BIT".equals(base)
                || "BIT VARYING".equals(base)
                || "VARBIT".equals(base)
                || isBoundedCharacter(base)
                || "RAW".equals(base)
                || "BINARY".equals(base)
                || "VARBINARY".equals(base))
                && precision > 0) {
            return raw + "(" + precision + ")";
        }
        if (isTimestampWithTimeZone(normalized)
                || isTimestampWithLocalTimeZone(normalized)) {
            return scale > 0
                    ? "TIMESTAMP(" + Math.min(scale, 9) + ")"
                    + (isTimestampWithLocalTimeZone(normalized)
                    ? " WITH LOCAL TIME ZONE"
                    : " WITH TIME ZONE")
                    : raw;
        }
        if ("TIMESTAMP".equals(base) && scale > 0) {
            return raw + "(" + Math.min(scale, 9) + ")";
        }
        return raw;
    }

    private static boolean isStringLike(String type, String base) {
        return type.isEmpty()
                || isBoundedCharacter(base)
                || "TEXT".equals(base)
                || "TINYTEXT".equals(base)
                || "MEDIUMTEXT".equals(base)
                || "LONGTEXT".equals(base)
                || "CLOB".equals(base)
                || "NCLOB".equals(base)
                || "LONG".equals(base)
                || "ROWID".equals(base)
                || "UROWID".equals(base)
                || "JSON".equals(base)
                || "JSONB".equals(base)
                || "XMLTYPE".equals(base)
                || "SYS.XMLTYPE".equals(base)
                || "INTERVAL YEAR TO MONTH".equals(base)
                || "INTERVAL DAY TO SECOND".equals(base)
                || "INTERVAL".equals(base)
                || "ENUM".equals(base)
                || "SET".equals(base)
                || "UUID".equals(base)
                || "VECTOR".equals(base);
    }

    private static boolean isBoundedCharacter(String base) {
        return "CHAR".equals(base)
                || "CHARACTER".equals(base)
                || "VARCHAR".equals(base)
                || "VARCHAR2".equals(base)
                || "CHARACTER VARYING".equals(base)
                || "NCHAR".equals(base)
                || "NVARCHAR".equals(base)
                || "NVARCHAR2".equals(base);
    }

    private static boolean isNumberBase(String base) {
        return "NUMBER".equals(base)
                || "NUMERIC".equals(base)
                || "DECIMAL".equals(base)
                || "DEC".equals(base)
                || "FIXED".equals(base);
    }

    private static boolean canPreserve(String sourceType) {
        String normalized = normalize(sourceType);
        if (normalized.isEmpty()) {
            return false;
        }
        String base = baseType(normalized);
        return isNumberBase(base)
                || isStringLike(normalized, base)
                || isBoundedCharacter(base)
                || "BOOLEAN".equals(base)
                || "BOOL".equals(base)
                || "TINYINT".equals(base)
                || "SMALLINT".equals(base)
                || "INT".equals(base)
                || "INTEGER".equals(base)
                || "BIGINT".equals(base)
                || base.contains("UNSIGNED")
                || "FLOAT".equals(base)
                || "REAL".equals(base)
                || "DOUBLE".equals(base)
                || "DOUBLE PRECISION".equals(base)
                || "BINARY_FLOAT".equals(base)
                || "BINARY_DOUBLE".equals(base)
                || "BIT".equals(base)
                || "BIT VARYING".equals(base)
                || "VARBIT".equals(base)
                || "RAW".equals(base)
                || "LONG RAW".equals(base)
                || "BLOB".equals(base)
                || "BINARY".equals(base)
                || "VARBINARY".equals(base)
                || "DATE".equals(base)
                || "TIME".equals(base)
                || normalized.startsWith("TIMESTAMP")
                || normalized.startsWith("DATETIME");
    }

    private static boolean isTimeWithTimeZone(String type) {
        return type.startsWith("TIME")
                && !type.startsWith("TIMESTAMP")
                && type.contains("WITH TIME ZONE");
    }

    private static boolean isTimestampWithTimeZone(String type) {
        return type.startsWith("TIMESTAMP")
                && type.contains("WITH TIME ZONE")
                && !type.contains("WITH LOCAL TIME ZONE");
    }

    private static boolean isTimestampWithLocalTimeZone(String type) {
        return type.startsWith("TIMESTAMP")
                && type.contains("WITH LOCAL TIME ZONE");
    }

    private static String baseType(String type) {
        if (type == null || type.isEmpty()) {
            return "";
        }
        return type.replaceFirst("\\([^)]*\\)", "")
                .replaceAll("\\s+", " ")
                .trim();
    }

    private static Integer integer(ResultSet row, String name) throws SQLException {
        try {
            Object value = row.getObject(name);
            return value == null ? null : ((Number) value).intValue();
        } catch (SQLException e) {
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

    private static String firstText(String first, String second) {
        return first != null && !first.trim().isEmpty() ? first : second;
    }

    private static String normalize(String value) {
        return value == null
                ? ""
                : value.trim()
                .toUpperCase(Locale.ROOT)
                .replaceAll("\\s+", " ");
    }
}
