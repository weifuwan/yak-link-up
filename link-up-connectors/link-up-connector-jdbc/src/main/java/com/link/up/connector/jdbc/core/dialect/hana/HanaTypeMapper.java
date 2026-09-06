package com.link.up.connector.jdbc.core.dialect.hana;

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

/** SAP HANA JDBC type mapper for bounded/offline source and sink jobs. */
public final class HanaTypeMapper implements JdbcTypeMapper {

    static final int MAX_DECIMAL_PRECISION = 38;
    static final int DEFAULT_DECIMAL_PRECISION = 38;
    static final int DEFAULT_DECIMAL_SCALE = 0;
    static final int MAX_TIMESTAMP_PRECISION = 7;
    static final long MAX_NVARCHAR_LENGTH = 5000L;
    static final long MAX_VARBINARY_LENGTH = 5000L;
    public static final String NATIVE_ATTRIBUTE = "hana_native";

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
        Column.Builder builder = Column.builder(name, type)
                .nullable(nullable == null
                        || nullable != ResultSetMetaData.columnNoNulls)
                .defaultValue(safeObject(row, "COLUMN_DEF"))
                .comment(safeString(row, "REMARKS"))
                .sourceType(buildSourceType(typeName, precision, scale))
                .attribute(NATIVE_ATTRIBUTE, "true");

        String auto = safeString(row, "IS_AUTOINCREMENT");
        builder.autoIncrement("YES".equalsIgnoreCase(auto));
        applyProperties(builder, type, typeName, precision, scale);
        return builder.build();
    }

    @Override
    public String toDatabaseType(Column column) {
        return toDatabaseType(column, false);
    }

    /**
     * Converts Flux types to HANA target types.
     *
     * <p>When the source is also HANA, callers may preserve the original native
     * type to keep TINYINT/SMALLDECIMAL/SECONDDATE and exact LOB choices.</p>
     */
    public String toDatabaseType(Column column, boolean preserveSourceType) {
        if (column == null) {
            throw new IllegalArgumentException("column must not be null");
        }
        if (preserveSourceType && canPreserve(column.getSourceType())) {
            return column.getSourceType().trim();
        }

        SqlType type = column.getDataType().getSqlType();
        switch (type) {
            case BOOLEAN:
                return "BOOLEAN";
            case TINYINT:
            case SMALLINT:
                // Flux TINYINT is signed; HANA TINYINT is unsigned.
                return "SMALLINT";
            case INT:
                return "INTEGER";
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
                return "TIME";
            case TIMESTAMP:
                return "TIMESTAMP";
            case TIMESTAMP_TZ:
                throw new IllegalArgumentException(
                        "SAP HANA TIMESTAMP 不保存时区偏移，不能直接写入 TIMESTAMP_TZ，column="
                                + column.getName());
            default:
                throw new IllegalArgumentException(
                        "SAP HANA 不支持 Flux 类型：" + type
                                + "，column=" + column.getName());
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

        if (normalized.endsWith(" ARRAY") || "ARRAY".equals(base)) {
            throw unsupported(sourceType, jdbcType);
        }
        if ("ST_POINT".equals(base) || "ST_GEOMETRY".equals(base)) {
            throw unsupported(sourceType, jdbcType);
        }

        switch (base) {
            case "BOOLEAN":
                return BasicType.BOOLEAN_TYPE;
            case "TINYINT":
            case "SMALLINT":
                // HANA TINYINT is unsigned (0..255), so Byte would overflow.
                return BasicType.SHORT_TYPE;
            case "INTEGER":
            case "INT":
                return BasicType.INT_TYPE;
            case "BIGINT":
                return BasicType.LONG_TYPE;
            case "REAL":
                return BasicType.FLOAT_TYPE;
            case "DOUBLE":
            case "DOUBLE PRECISION":
                return BasicType.DOUBLE_TYPE;
            case "SMALLDECIMAL":
            case "DECIMAL":
                return decimal(precision, scale);
            case "DATE":
                return BasicType.DATE_TYPE;
            case "TIME":
                return BasicType.TIME_TYPE;
            case "SECONDDATE":
            case "TIMESTAMP":
                return BasicType.TIMESTAMP_TYPE;
            case "BINARY":
            case "VARBINARY":
            case "BLOB":
                return BasicType.BYTES_TYPE;
            case "VARCHAR":
            case "NVARCHAR":
            case "ALPHANUM":
            case "SHORTTEXT":
            case "CLOB":
            case "NCLOB":
            case "TEXT":
            case "BINTEXT":
                return BasicType.STRING_TYPE;
            default:
                break;
        }

        switch (jdbcType) {
            case Types.BOOLEAN:
            case Types.BIT:
                return BasicType.BOOLEAN_TYPE;
            case Types.TINYINT:
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
                return decimal(precision, scale);
            case Types.DATE:
                return BasicType.DATE_TYPE;
            case Types.TIME:
                return BasicType.TIME_TYPE;
            case Types.TIMESTAMP:
                return BasicType.TIMESTAMP_TYPE;
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
                return BasicType.STRING_TYPE;
            default:
                throw unsupported(sourceType, jdbcType);
        }
    }

    private static DecimalType decimal(int precision, int scale) {
        int safePrecision = precision <= 0
                ? DEFAULT_DECIMAL_PRECISION
                : precision;
        int safeScale = scale < 0 ? DEFAULT_DECIMAL_SCALE : scale;

        if (safePrecision > MAX_DECIMAL_PRECISION) {
            throw new IllegalArgumentException(
                    "SAP HANA DECIMAL precision 最大为 38，actual=" + safePrecision);
        }
        if (safeScale > safePrecision) {
            throw new IllegalArgumentException(
                    "SAP HANA DECIMAL scale 不能大于 precision，precision="
                            + safePrecision + "，scale=" + safeScale);
        }
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

        int precision = precisionValue == null
                ? DEFAULT_DECIMAL_PRECISION
                : precisionValue;
        int scale = scaleValue == null
                ? DEFAULT_DECIMAL_SCALE
                : scaleValue;

        if (precision <= 0) {
            precision = DEFAULT_DECIMAL_PRECISION;
        }
        if (precision > MAX_DECIMAL_PRECISION) {
            throw new IllegalArgumentException(
                    "SAP HANA DECIMAL precision 最大为 38，column="
                            + column.getName() + "，precision=" + precision);
        }
        if (scale < 0 || scale > precision) {
            throw new IllegalArgumentException(
                    "SAP HANA DECIMAL scale 非法，column=" + column.getName()
                            + "，precision=" + precision + "，scale=" + scale);
        }
        return "DECIMAL(" + precision + "," + scale + ")";
    }

    private static String stringType(Column column) {
        Long length = column.getLength();
        if (length == null || length <= 0 || length > MAX_NVARCHAR_LENGTH) {
            return "NCLOB";
        }
        return "NVARCHAR(" + Math.max(1L, length) + ")";
    }

    private static String binaryType(Column column) {
        Long length = column.getLength();
        if (length == null || length <= 0 || length > MAX_VARBINARY_LENGTH) {
            return "BLOB";
        }
        return "VARBINARY(" + Math.max(1L, length) + ")";
    }

    private static boolean canPreserve(String sourceType) {
        String normalized = normalizeType(sourceType);
        if (normalized.isEmpty() || normalized.endsWith(" ARRAY")) {
            return false;
        }
        switch (baseType(normalized)) {
            case "BOOLEAN":
            case "TINYINT":
            case "SMALLINT":
            case "INTEGER":
            case "INT":
            case "BIGINT":
            case "REAL":
            case "DOUBLE":
            case "DOUBLE PRECISION":
            case "SMALLDECIMAL":
            case "DECIMAL":
            case "DATE":
            case "TIME":
            case "SECONDDATE":
            case "TIMESTAMP":
            case "BINARY":
            case "VARBINARY":
            case "BLOB":
            case "VARCHAR":
            case "NVARCHAR":
            case "ALPHANUM":
            case "SHORTTEXT":
            case "CLOB":
            case "NCLOB":
            case "TEXT":
            case "BINTEXT":
                return true;
            default:
                return false;
        }
    }

    private static void applyProperties(
            Column.Builder builder,
            FluxDataType<?> type,
            String sourceType,
            int precision,
            int scale) {

        SqlType sqlType = type.getSqlType();
        String base = baseType(normalizeType(sourceType));

        if (sqlType == SqlType.STRING || sqlType == SqlType.BYTES) {
            if (precision > 0
                    && !"CLOB".equals(base)
                    && !"NCLOB".equals(base)
                    && !"TEXT".equals(base)
                    && !"BINTEXT".equals(base)
                    && !"BLOB".equals(base)) {
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
        if (sqlType == SqlType.TIMESTAMP && "TIMESTAMP".equals(base)) {
            int p = Math.max(0, Math.min(scale, MAX_TIMESTAMP_PRECISION));
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
        String base = baseType(normalizeType(raw));
        if ("DECIMAL".equals(base) || "SMALLDECIMAL".equals(base)) {
            if (precision > 0) {
                return raw + "(" + precision + "," + Math.max(0, scale) + ")";
            }
            return raw;
        }
        if (isLengthType(base) && precision > 0) {
            return raw + "(" + precision + ")";
        }
        return raw;
    }

    private static boolean isLengthType(String base) {
        return "BINARY".equals(base)
                || "VARBINARY".equals(base)
                || "VARCHAR".equals(base)
                || "NVARCHAR".equals(base)
                || "ALPHANUM".equals(base)
                || "SHORTTEXT".equals(base);
    }

    private static String normalizeType(String sourceType) {
        return sourceType == null
                ? ""
                : sourceType.trim().replaceAll("\\s+", " ").toUpperCase(Locale.ROOT);
    }

    private static String baseType(String sourceType) {
        if (sourceType == null || sourceType.isEmpty()) {
            return "";
        }
        String value = sourceType;
        int paren = value.indexOf('(');
        if (paren >= 0) {
            value = value.substring(0, paren).trim();
        }
        return value;
    }

    private static IllegalArgumentException unsupported(String sourceType, int jdbcType) {
        return new IllegalArgumentException(
                "暂不支持 SAP HANA 字段类型：" + sourceType + "，jdbcType=" + jdbcType);
    }

    private static String firstText(String first, String second) {
        if (first != null && !first.trim().isEmpty()) {
            return first.trim();
        }
        if (second != null && !second.trim().isEmpty()) {
            return second.trim();
        }
        throw new IllegalArgumentException("JDBC column name must not be empty");
    }

    private static Integer integer(ResultSet row, String column) {
        try {
            Object value = row.getObject(column);
            if (value == null) {
                return null;
            }
            if (value instanceof Number) {
                return ((Number) value).intValue();
            }
            return Integer.valueOf(value.toString());
        } catch (Exception ignored) {
            return null;
        }
    }

    private static String safeString(ResultSet row, String column) {
        try {
            return row.getString(column);
        } catch (SQLException ignored) {
            return null;
        }
    }

    private static Object safeObject(ResultSet row, String column) {
        try {
            return row.getObject(column);
        } catch (SQLException ignored) {
            return null;
        }
    }

    private static int value(Integer value) {
        return value == null ? 0 : value;
    }
}
