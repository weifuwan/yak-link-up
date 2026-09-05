package com.link.up.connector.jdbc.core.dialect.db2;

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

/** DB2 LUW JDBC type mapper for bounded jobs. */
public final class Db2TypeMapper implements JdbcTypeMapper {

    static final int MAX_DECIMAL_PRECISION = 31;
    static final int MAX_TIMESTAMP_PRECISION = 12;
    static final long MAX_VARCHAR_LENGTH = 32672L;
    static final long MAX_LOB_LENGTH = 2147483647L;

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

    /** Maps a JDBC DatabaseMetaData#getColumns row. */
    public Column toColumn(ResultSet row) throws SQLException {
        String name = row.getString("COLUMN_NAME");
        String typeName = row.getString("TYPE_NAME");
        Integer size = integer(row, "COLUMN_SIZE");
        Integer scale = integer(row, "DECIMAL_DIGITS");
        Integer nullableValue = integer(row, "NULLABLE");
        boolean nullable = nullableValue == null
                || nullableValue != ResultSetMetaData.columnNoNulls;

        int jdbcType = value(integer(row, "DATA_TYPE"));
        int precision = value(size);
        int safeScale = value(scale);
        FluxDataType<?> type = mapType(jdbcType, typeName, precision, safeScale);

        Column.Builder builder = Column.builder(name, type)
                .nullable(nullable)
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
                return "BOOLEAN";
            case TINYINT:
            case SMALLINT:
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
                return temporalType(column.getPrecision());
            case TIMESTAMP_TZ:
                throw new IllegalArgumentException(
                        "DB2 LUW 不支持 TIMESTAMP WITH TIME ZONE，column=" + column.getName());
            default:
                throw new IllegalArgumentException(
                        "DB2 不支持 Flux 类型：" + type + "，column=" + column.getName());
        }
    }

    /** Package-visible for focused type contract tests. */
    FluxDataType<?> mapType(
            int jdbcType,
            String sourceType,
            int precision,
            int scale) {

        String type = normalizeType(sourceType);

        if (type.contains("FOR BIT DATA")) {
            return BasicType.BYTES_TYPE;
        }

        if (type.startsWith("TIMESTAMP")) {
            return type.contains("TIME ZONE")
                    ? BasicType.TIMESTAMP_TZ_TYPE
                    : BasicType.TIMESTAMP_TYPE;
        }

        switch (baseType(type)) {
            case "BOOLEAN":
                return BasicType.BOOLEAN_TYPE;
            case "SMALLINT":
                return BasicType.SHORT_TYPE;
            case "INT":
            case "INTEGER":
                return BasicType.INT_TYPE;
            case "BIGINT":
                return BasicType.LONG_TYPE;
            case "REAL":
                return BasicType.FLOAT_TYPE;
            case "DOUBLE":
            case "DOUBLE PRECISION":
                return BasicType.DOUBLE_TYPE;
            case "DECFLOAT":
                // DECFLOAT(34) cannot be represented safely as IEEE DOUBLE.
                // Keep its exact textual form while preserving sourceType.
                return BasicType.STRING_TYPE;
            case "DECIMAL":
            case "DEC":
            case "NUMERIC":
            case "NUM":
                return decimal(precision, scale);
            case "DATE":
                return BasicType.DATE_TYPE;
            case "TIME":
                return BasicType.TIME_TYPE;
            case "CHAR":
            case "CHARACTER":
            case "VARCHAR":
            case "LONG VARCHAR":
            case "CLOB":
            case "GRAPHIC":
            case "VARGRAPHIC":
            case "DBCLOB":
            case "XML":
            case "ROWID":
                return BasicType.STRING_TYPE;
            case "BINARY":
            case "VARBINARY":
            case "BLOB":
                return BasicType.BYTES_TYPE;
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
            case Types.CLOB:
            case Types.NCHAR:
            case Types.NVARCHAR:
            case Types.LONGNVARCHAR:
            case Types.NCLOB:
            case Types.SQLXML:
                return BasicType.STRING_TYPE;
            default:
                throw new IllegalArgumentException(
                        "暂不支持 DB2 字段类型：" + sourceType + "，jdbcType=" + jdbcType);
        }
    }

    private static DecimalType decimal(int precision, int scale) {
        int safePrecision = precision <= 0 ? MAX_DECIMAL_PRECISION : precision;
        if (safePrecision > MAX_DECIMAL_PRECISION) {
            throw new IllegalArgumentException(
                    "DB2 DECIMAL precision 最大为 31，actual=" + safePrecision);
        }
        int safeScale = Math.max(0, scale);
        if (safeScale > safePrecision) {
            throw new IllegalArgumentException(
                    "DB2 DECIMAL scale 不能大于 precision，precision="
                            + safePrecision + "，scale=" + safeScale);
        }
        return new DecimalType(safePrecision, safeScale);
    }

    private static String decimalType(Column column) {
        int precision = column.getPrecision() == null
                ? MAX_DECIMAL_PRECISION
                : column.getPrecision();
        int scale = column.getScale() == null ? 0 : column.getScale();

        if (precision <= 0) {
            precision = MAX_DECIMAL_PRECISION;
        }
        if (precision > MAX_DECIMAL_PRECISION) {
            throw new IllegalArgumentException(
                    "DB2 DECIMAL precision 最大为 31，column=" + column.getName()
                            + "，precision=" + precision);
        }
        if (scale < 0 || scale > precision) {
            throw new IllegalArgumentException(
                    "DB2 DECIMAL scale 非法，column=" + column.getName()
                            + "，precision=" + precision + "，scale=" + scale);
        }
        return "DECIMAL(" + precision + "," + scale + ")";
    }

    private static String stringType(Column column) {
        Long length = column.getLength();
        if (length == null || length <= 0) {
            return "CLOB";
        }
        if (length <= MAX_VARCHAR_LENGTH) {
            return "VARCHAR(" + Math.max(1L, length) + ")";
        }
        return "CLOB(" + Math.min(length, MAX_LOB_LENGTH) + ")";
    }

    private static String binaryType(Column column) {
        Long length = column.getLength();
        if (length == null || length <= 0) {
            return "BLOB";
        }
        if (length <= MAX_VARCHAR_LENGTH) {
            return "VARBINARY(" + Math.max(1L, length) + ")";
        }
        return "BLOB(" + Math.min(length, MAX_LOB_LENGTH) + ")";
    }

    private static String temporalType(Integer precision) {
        if (precision == null || precision <= 0) {
            return "TIMESTAMP";
        }
        return "TIMESTAMP(" + Math.min(precision, MAX_TIMESTAMP_PRECISION) + ")";
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
            // DECFLOAT precision is significant digits, not character length.
            // Avoid narrowing the exact textual representation on cross-DB sinks.
            if (!"DECFLOAT".equals(base) && precision > 0) {
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
        if (sqlType == SqlType.TIMESTAMP) {
            int p = Math.max(0, Math.min(
                    scale > 0 ? scale : precision,
                    MAX_TIMESTAMP_PRECISION));
            if (p > 0) {
                builder.precision(p);
            }
        }
    }

    private static String buildSourceType(String sourceType, int precision, int scale) {
        String raw = sourceType == null ? "" : sourceType.trim();
        String type = baseType(normalizeType(sourceType));
        if (raw.isEmpty()) {
            return raw;
        }
        if (raw.contains("(") || raw.toUpperCase(Locale.ROOT).contains("FOR BIT DATA")) {
            return raw;
        }
        if ("DECFLOAT".equals(type)) {
            return precision == 16 || precision == 34
                    ? raw + "(" + precision + ")"
                    : raw;
        }
        if ("DECIMAL".equals(type) || "DEC".equals(type)
                || "NUMERIC".equals(type) || "NUM".equals(type)) {
            return precision > 0
                    ? raw + "(" + precision + "," + Math.max(0, scale) + ")"
                    : raw;
        }
        if ("CHAR".equals(type) || "CHARACTER".equals(type)
                || "VARCHAR".equals(type) || "BINARY".equals(type)
                || "VARBINARY".equals(type) || "CLOB".equals(type)
                || "BLOB".equals(type) || "GRAPHIC".equals(type)
                || "VARGRAPHIC".equals(type) || "DBCLOB".equals(type)) {
            return precision > 0 ? raw + "(" + precision + ")" : raw;
        }
        if ("TIMESTAMP".equals(type)) {
            return scale > 0 ? raw + "(" + Math.min(scale, MAX_TIMESTAMP_PRECISION) + ")" : raw;
        }
        return raw;
    }

    private static boolean canPreserve(String sourceType) {
        String type = normalizeType(sourceType);
        if (type.isEmpty()) {
            return false;
        }
        String base = baseType(type);
        return "BOOLEAN".equals(base)
                || "SMALLINT".equals(base)
                || "INT".equals(base)
                || "INTEGER".equals(base)
                || "BIGINT".equals(base)
                || "REAL".equals(base)
                || "DOUBLE".equals(base)
                || "DOUBLE PRECISION".equals(base)
                || "DECFLOAT".equals(base)
                || "DECIMAL".equals(base)
                || "DEC".equals(base)
                || "NUMERIC".equals(base)
                || "NUM".equals(base)
                || "CHAR".equals(base)
                || "CHARACTER".equals(base)
                || "VARCHAR".equals(base)
                || "LONG VARCHAR".equals(base)
                || "CLOB".equals(base)
                || "GRAPHIC".equals(base)
                || "VARGRAPHIC".equals(base)
                || "DBCLOB".equals(base)
                || "BINARY".equals(base)
                || "VARBINARY".equals(base)
                || "BLOB".equals(base)
                || "DATE".equals(base)
                || "TIME".equals(base)
                || "TIMESTAMP".equals(base)
                || "XML".equals(base)
                || type.contains("FOR BIT DATA");
    }

    private static String baseType(String type) {
        int parenthesis = type.indexOf('(');
        return parenthesis >= 0 ? type.substring(0, parenthesis).trim() : type;
    }

    private static String normalizeType(String value) {
        return value == null ? "" : value.trim().toUpperCase(Locale.ROOT).replaceAll("\\s+", " ");
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
