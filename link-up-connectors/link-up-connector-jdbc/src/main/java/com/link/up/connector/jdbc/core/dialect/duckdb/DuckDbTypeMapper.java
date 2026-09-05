package com.link.up.connector.jdbc.core.dialect.duckdb;

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

/** DuckDB JDBC type mapper for bounded/offline jobs. */
public final class DuckDbTypeMapper implements JdbcTypeMapper {

    static final int MAX_DECIMAL_PRECISION = 38;
    static final int DEFAULT_DECIMAL_PRECISION = 18;
    static final int DEFAULT_DECIMAL_SCALE = 3;
    static final int MAX_NANOSECOND_PRECISION = 9;

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
        Object defaultValue = safeObject(row, "COLUMN_DEF");

        Column.Builder builder = Column.builder(name, type)
                .nullable(nullable == null
                        || nullable != ResultSetMetaData.columnNoNulls)
                .defaultValue(defaultValue)
                .comment(safeString(row, "REMARKS"))
                .sourceType(buildSourceType(typeName, precision, safeScale));

        String auto = safeString(row, "IS_AUTOINCREMENT");
        boolean autoIncrement = "YES".equalsIgnoreCase(auto)
                || (defaultValue != null
                && String.valueOf(defaultValue).trim()
                .toLowerCase(Locale.ROOT).startsWith("nextval("));
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
                return "VARCHAR";
            case BYTES:
                return "BLOB";
            case DATE:
                return "DATE";
            case TIME:
                return timeType(column);
            case TIMESTAMP:
                return timestampType(column);
            case TIMESTAMP_TZ:
                if (column.getPrecision() != null && column.getPrecision() > 6) {
                    throw new IllegalArgumentException(
                            "DuckDB TIMESTAMPTZ 仅支持微秒精度，column="
                                    + column.getName() + "，precision="
                                    + column.getPrecision());
                }
                return "TIMESTAMPTZ";
            default:
                throw new IllegalArgumentException(
                        "DuckDB 不支持 Flux 类型：" + type
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

        if (isComplex(type, base)) {
            return BasicType.STRING_TYPE;
        }
        if ("TIME WITH TIME ZONE".equals(base) || "TIMETZ".equals(base)) {
            return BasicType.STRING_TYPE;
        }
        if ("TIMESTAMPTZ".equals(base)
                || "TIMESTAMP WITH TIME ZONE".equals(base)) {
            return BasicType.TIMESTAMP_TZ_TYPE;
        }
        if ("TIMESTAMP_NS".equals(base)) {
            return BasicType.TIMESTAMP_TYPE;
        }
        if ("TIMESTAMP_MS".equals(base)
                || "TIMESTAMP_S".equals(base)
                || "TIMESTAMP".equals(base)
                || "DATETIME".equals(base)
                || "TIMESTAMP WITHOUT TIME ZONE".equals(base)) {
            return BasicType.TIMESTAMP_TYPE;
        }
        if ("TIME_NS".equals(base) || "TIME".equals(base)
                || "TIME WITHOUT TIME ZONE".equals(base)) {
            return BasicType.TIME_TYPE;
        }

        switch (base) {
            case "BOOLEAN":
            case "BOOL":
            case "LOGICAL":
                return BasicType.BOOLEAN_TYPE;
            case "TINYINT":
            case "INT1":
                return BasicType.BYTE_TYPE;
            case "UTINYINT":
            case "UINT8":
                return BasicType.SHORT_TYPE;
            case "SMALLINT":
            case "INT2":
            case "INT16":
            case "SHORT":
                return BasicType.SHORT_TYPE;
            case "USMALLINT":
            case "UINT16":
                return BasicType.INT_TYPE;
            case "INTEGER":
            case "INT":
            case "INT4":
            case "INT32":
            case "SIGNED":
                return BasicType.INT_TYPE;
            case "UINTEGER":
            case "UINT32":
                return BasicType.LONG_TYPE;
            case "BIGINT":
            case "INT8":
            case "INT64":
            case "LONG":
                return BasicType.LONG_TYPE;
            case "UBIGINT":
            case "UINT64":
                return new DecimalType(20, 0);
            case "HUGEINT":
            case "INT128":
            case "UHUGEINT":
            case "UINT128":
            case "BIGNUM":
                return BasicType.STRING_TYPE;
            case "DECIMAL":
            case "NUMERIC":
                return decimalSourceType(precision, scale);
            case "FLOAT":
            case "FLOAT4":
            case "REAL":
                return BasicType.FLOAT_TYPE;
            case "DOUBLE":
            case "FLOAT8":
            case "DOUBLE PRECISION":
                return BasicType.DOUBLE_TYPE;
            case "DATE":
                return BasicType.DATE_TYPE;
            case "BLOB":
            case "BYTEA":
            case "BINARY":
            case "VARBINARY":
                return BasicType.BYTES_TYPE;
            case "BIT":
            case "BITSTRING":
            case "UUID":
            case "JSON":
            case "INTERVAL":
            case "ENUM":
                return BasicType.STRING_TYPE;
            default:
                break;
        }

        if (isStringType(base)) {
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
                return decimalSourceType(precision, scale);
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

    private static FluxDataType<?> decimalSourceType(int precision, int scale) {
        if (precision <= 0 || precision > MAX_DECIMAL_PRECISION
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
        int precision = precisionValue == null
                ? DEFAULT_DECIMAL_PRECISION : precisionValue;
        int scale = scaleValue == null ? DEFAULT_DECIMAL_SCALE : scaleValue;
        if (precision <= 0 || precision > MAX_DECIMAL_PRECISION) {
            throw new IllegalArgumentException(
                    "DuckDB DECIMAL precision 必须在 1..38，column="
                            + column.getName() + "，precision=" + precision);
        }
        if (scale < 0 || scale > precision) {
            throw new IllegalArgumentException(
                    "DuckDB DECIMAL scale 非法，column=" + column.getName()
                            + "，precision=" + precision + "，scale=" + scale);
        }
        return "DECIMAL(" + precision + "," + scale + ")";
    }

    private static String timeType(Column column) {
        Integer precision = column.getPrecision();
        if (precision != null && precision > MAX_NANOSECOND_PRECISION) {
            throw new IllegalArgumentException(
                    "DuckDB TIME 最大支持纳秒精度，column=" + column.getName()
                            + "，precision=" + precision);
        }
        return precision != null && precision > 6 ? "TIME_NS" : "TIME";
    }

    private static String timestampType(Column column) {
        Integer precision = column.getPrecision();
        if (precision != null && precision > MAX_NANOSECOND_PRECISION) {
            throw new IllegalArgumentException(
                    "DuckDB TIMESTAMP 最大支持纳秒精度，column=" + column.getName()
                            + "，precision=" + precision);
        }
        return precision != null && precision > 6 ? "TIMESTAMP_NS" : "TIMESTAMP";
    }

    private static void applyProperties(
            Column.Builder builder,
            FluxDataType<?> dataType,
            String sourceType,
            int precision,
            int scale) {

        SqlType sqlType = dataType.getSqlType();
        String base = baseType(normalize(sourceType));
        if (sqlType == SqlType.DECIMAL && dataType instanceof DecimalType) {
            DecimalType decimal = (DecimalType) dataType;
            builder.precision(decimal.getPrecision());
            builder.scale(decimal.getScale());
            return;
        }
        if (sqlType == SqlType.TIME) {
            builder.precision("TIME_NS".equals(base) ? 9 : 6);
            return;
        }
        if (sqlType == SqlType.TIMESTAMP) {
            if ("TIMESTAMP_NS".equals(base)) {
                builder.precision(9);
            } else if ("TIMESTAMP_MS".equals(base)) {
                builder.precision(3);
            } else if ("TIMESTAMP_S".equals(base)) {
                builder.precision(0);
            } else {
                builder.precision(6);
            }
            return;
        }
        if (sqlType == SqlType.TIMESTAMP_TZ) {
            builder.precision(6);
        }
    }

    private static String buildSourceType(String sourceType, int precision, int scale) {
        String raw = sourceType == null ? "" : sourceType.trim();
        String base = baseType(normalize(raw));
        if (("DECIMAL".equals(base) || "NUMERIC".equals(base))
                && raw.indexOf('(') < 0 && precision > 0) {
            return raw + "(" + precision + "," + scale + ")";
        }
        return raw;
    }

    private static boolean canPreserve(String sourceType) {
        String type = normalize(sourceType);
        String base = baseType(type);
        if (type.isEmpty() || isComplex(type, base)) {
            return false;
        }
        return isStringType(base)
                || "BOOLEAN".equals(base)
                || "BOOL".equals(base)
                || base.endsWith("INT")
                || base.startsWith("INT")
                || base.startsWith("UINT")
                || "HUGEINT".equals(base)
                || "UHUGEINT".equals(base)
                || "BIGNUM".equals(base)
                || "DECIMAL".equals(base)
                || "NUMERIC".equals(base)
                || "FLOAT".equals(base)
                || "REAL".equals(base)
                || "DOUBLE".equals(base)
                || "DATE".equals(base)
                || base.startsWith("TIME")
                || base.startsWith("TIMESTAMP")
                || "BLOB".equals(base)
                || "BYTEA".equals(base)
                || "BINARY".equals(base)
                || "VARBINARY".equals(base)
                || "BIT".equals(base)
                || "BITSTRING".equals(base)
                || "UUID".equals(base)
                || "JSON".equals(base)
                || "INTERVAL".equals(base)
                || "ENUM".equals(base);
    }

    private static boolean isStringType(String base) {
        return "VARCHAR".equals(base)
                || "CHAR".equals(base)
                || "BPCHAR".equals(base)
                || "TEXT".equals(base)
                || "STRING".equals(base);
    }

    private static boolean isComplex(String type, String base) {
        return type.contains("[]")
                || base.startsWith("LIST")
                || base.startsWith("ARRAY")
                || base.startsWith("STRUCT")
                || base.startsWith("MAP")
                || base.startsWith("UNION");
    }

    private static String baseType(String type) {
        int parenthesis = type.indexOf('(');
        return parenthesis >= 0 ? type.substring(0, parenthesis).trim() : type;
    }

    private static String normalize(String value) {
        return value == null ? ""
                : value.trim().toUpperCase(Locale.ROOT).replaceAll("\\s+", " ");
    }

    private static String firstText(String first, String second) {
        return first != null && !first.trim().isEmpty() ? first : second;
    }

    private static Integer integer(ResultSet row, String column) {
        try {
            Object value = row.getObject(column);
            return value == null ? null : ((Number) value).intValue();
        } catch (Exception ignored) {
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

    private static String safeString(ResultSet row, String column) {
        try {
            return row.getString(column);
        } catch (SQLException ignored) {
            return null;
        }
    }

    private static int value(Integer value) {
        return value == null ? 0 : value;
    }
}
