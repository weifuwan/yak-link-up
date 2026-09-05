package com.link.up.connector.jdbc.core.dialect.postgres;

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
 * PostgreSQL JDBC type mapper for bounded/offline jobs.
 *
 * <p>Common PostgreSQL types map to Flux primitives. UUID/JSON/JSONB and
 * unknown PostgreSQL extension types intentionally fall back to STRING so
 * schema discovery stays safe without introducing PostgreSQL-specific runtime
 * types into the connector API.</p>
 */
public final class PostgresTypeMapper implements JdbcTypeMapper {

    private static final int MAX_DECIMAL_PRECISION = 38;

    @Override
    public Column map(ResultSetMetaData metadata, int columnIndex) throws SQLException {
        String name = firstText(metadata.getColumnLabel(columnIndex), metadata.getColumnName(columnIndex));
        String sourceType = metadata.getColumnTypeName(columnIndex);
        int precision = metadata.getPrecision(columnIndex);
        int scale = metadata.getScale(columnIndex);
        FluxDataType<?> type = mapType(
                metadata.getColumnType(columnIndex), sourceType, precision, scale);

        Column.Builder builder = Column.builder(name, type)
                .nullable(metadata.isNullable(columnIndex) != ResultSetMetaData.columnNoNulls)
                .sourceType(sourceType);
        applyProperties(builder, type.getSqlType(), precision, scale);
        return builder.build();
    }

    /** Maps one information_schema.columns row. */
    public Column toColumn(ResultSet row) throws SQLException {
        String name = row.getString("COLUMN_NAME");
        String dataType = row.getString("DATA_TYPE");
        String udtName = row.getString("UDT_NAME");
        String sourceType = sourceType(dataType, udtName);
        Integer precision = integer(row, "NUMERIC_PRECISION");
        Integer scale = integer(row, "NUMERIC_SCALE");
        Integer timePrecision = integer(row, "DATETIME_PRECISION");
        Long length = longValue(row, "CHARACTER_MAXIMUM_LENGTH");

        FluxDataType<?> type = mapType(
                Types.OTHER,
                sourceType,
                value(precision),
                value(scale));

        Object defaultValue = row.getObject("COLUMN_DEFAULT");
        boolean identity = "YES".equalsIgnoreCase(row.getString("IS_IDENTITY"))
                || (defaultValue != null
                && String.valueOf(defaultValue).toLowerCase(Locale.ROOT).startsWith("nextval("));

        Column.Builder builder = Column.builder(name, type)
                .nullable("YES".equalsIgnoreCase(row.getString("IS_NULLABLE")))
                .defaultValue(defaultValue)
                .autoIncrement(identity)
                .comment(row.getString("COLUMN_COMMENT"))
                .sourceType(sourceType);
        applyProperties(
                builder,
                type.getSqlType(),
                length == null ? 0 : length.intValue(),
                type.getSqlType() == SqlType.DECIMAL ? value(precision) : value(timePrecision),
                value(scale));
        if (udtName != null) {
            builder.attribute("udt_name", udtName);
        }
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

        switch (column.getDataType().getSqlType()) {
            case STRING:
                return stringType(column);
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
                return "DOUBLE PRECISION";
            case DECIMAL:
                return decimalType(column);
            case BYTES:
                return "BYTEA";
            case DATE:
                return "DATE";
            case TIME:
                return temporal("TIME", column.getPrecision());
            case TIMESTAMP:
                return temporal("TIMESTAMP", column.getPrecision());
            case TIMESTAMP_TZ:
                return temporal("TIMESTAMP WITH TIME ZONE", column.getPrecision());
            default:
                throw new IllegalArgumentException(
                        "PostgreSQL unsupported Flux type: "
                                + column.getDataType().getSqlType()
                                + ", column=" + column.getName());
        }
    }

    private FluxDataType<?> mapType(int jdbcType, String sourceType, int precision, int scale) {
        String type = normalize(sourceType);
        if ("bool".equals(type) || "boolean".equals(type)) {
            return BasicType.BOOLEAN_TYPE;
        }
        if ("int2".equals(type) || "smallint".equals(type)
                || "smallserial".equals(type) || "serial2".equals(type)) {
            return BasicType.SHORT_TYPE;
        }
        if ("int4".equals(type) || "integer".equals(type)
                || "serial".equals(type) || "serial4".equals(type)) {
            return BasicType.INT_TYPE;
        }
        if ("int8".equals(type) || "bigint".equals(type)
                || "bigserial".equals(type) || "serial8".equals(type) || "oid".equals(type)) {
            return BasicType.LONG_TYPE;
        }
        if ("float4".equals(type) || "real".equals(type)) {
            return BasicType.FLOAT_TYPE;
        }
        if ("float8".equals(type) || "double precision".equals(type)) {
            return BasicType.DOUBLE_TYPE;
        }
        if ("numeric".equals(type) || "decimal".equals(type)) {
            return decimal(precision, scale);
        }
        if ("money".equals(type)) {
            return new DecimalType(19, 2);
        }
        if ("date".equals(type)) {
            return BasicType.DATE_TYPE;
        }
        if (type.startsWith("time") || "timetz".equals(type)) {
            return BasicType.TIME_TYPE;
        }
        if ("timestamptz".equals(type) || type.contains("timestamp with time zone")) {
            return BasicType.TIMESTAMP_TZ_TYPE;
        }
        if (type.startsWith("timestamp")) {
            return BasicType.TIMESTAMP_TYPE;
        }
        if ("bytea".equals(type)) {
            return BasicType.BYTES_TYPE;
        }
        if (isStringLike(type) || type.startsWith("_")) {
            return BasicType.STRING_TYPE;
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
            case Types.TIME_WITH_TIMEZONE:
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
            default:
                return BasicType.STRING_TYPE;
        }
    }

    private static boolean isStringLike(String type) {
        return type.isEmpty()
                || type.contains("char")
                || "text".equals(type)
                || "citext".equals(type)
                || "name".equals(type)
                || "uuid".equals(type)
                || "json".equals(type)
                || "jsonb".equals(type)
                || "xml".equals(type)
                || "inet".equals(type)
                || "cidr".equals(type)
                || type.startsWith("macaddr")
                || "interval".equals(type)
                || "array".equals(type)
                || "user-defined".equals(type);
    }

    private static boolean canPreserve(String sourceType) {
        String type = normalize(sourceType);
        return "boolean".equals(type) || "bool".equals(type)
                || "smallint".equals(type) || "int2".equals(type)
                || "integer".equals(type) || "int4".equals(type)
                || "bigint".equals(type) || "int8".equals(type)
                || "real".equals(type) || "float4".equals(type)
                || "double precision".equals(type) || "float8".equals(type)
                || "text".equals(type) || "bytea".equals(type)
                || "date".equals(type) || "uuid".equals(type)
                || "json".equals(type) || "jsonb".equals(type)
                || "xml".equals(type) || "inet".equals(type)
                || "cidr".equals(type) || type.startsWith("macaddr");
    }

    private static DecimalType decimal(int precision, int scale) {
        int p = precision <= 0 ? MAX_DECIMAL_PRECISION : Math.min(precision, MAX_DECIMAL_PRECISION);
        int s = Math.max(0, Math.min(scale, p));
        return new DecimalType(p, s);
    }

    private static String stringType(Column column) {
        Long length = column.getLength();
        return length != null && length > 0 && length <= 10_485_760L
                ? "VARCHAR(" + length + ")"
                : "TEXT";
    }

    private static String decimalType(Column column) {
        int p = column.getPrecision() == null ? MAX_DECIMAL_PRECISION
                : Math.max(1, Math.min(column.getPrecision(), MAX_DECIMAL_PRECISION));
        int s = column.getScale() == null ? 0 : Math.max(0, Math.min(column.getScale(), p));
        return "NUMERIC(" + p + "," + s + ")";
    }

    private static String temporal(String type, Integer precision) {
        return precision == null || precision <= 0
                ? type
                : type + "(" + Math.min(precision, 6) + ")";
    }

    private static void applyProperties(
            Column.Builder builder, SqlType sqlType, int precision, int scale) {
        applyProperties(builder, sqlType, precision, precision, scale);
    }

    private static void applyProperties(
            Column.Builder builder,
            SqlType sqlType,
            long length,
            int precision,
            int scale) {
        if (sqlType == SqlType.STRING || sqlType == SqlType.BYTES) {
            if (length > 0) {
                builder.length(length);
            }
        } else if (sqlType == SqlType.DECIMAL) {
            if (precision > 0) {
                builder.precision(Math.min(precision, MAX_DECIMAL_PRECISION));
            }
            builder.scale(Math.max(0, Math.min(scale, Math.max(precision, 0))));
        } else if (sqlType == SqlType.TIME
                || sqlType == SqlType.TIMESTAMP
                || sqlType == SqlType.TIMESTAMP_TZ) {
            if (precision > 0) {
                builder.precision(Math.min(precision, 6));
            }
        }
    }

    private static String sourceType(String dataType, String udtName) {
        if ("ARRAY".equalsIgnoreCase(dataType) || "USER-DEFINED".equalsIgnoreCase(dataType)) {
            return udtName == null ? dataType : udtName;
        }
        return dataType;
    }

    private static Integer integer(ResultSet row, String name) throws SQLException {
        Object value = row.getObject(name);
        return value == null ? null : ((Number) value).intValue();
    }

    private static Long longValue(ResultSet row, String name) throws SQLException {
        Object value = row.getObject(name);
        return value == null ? null : ((Number) value).longValue();
    }

    private static int value(Integer value) {
        return value == null ? 0 : value;
    }

    private static String firstText(String first, String second) {
        return first != null && !first.trim().isEmpty() ? first : second;
    }

    private static String normalize(String value) {
        return value == null ? "" : value.trim().toLowerCase(Locale.ROOT);
    }
}
