package com.link.up.connector.jdbc.core.dialect.oracle;

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
 * Oracle JDBC type mapper for bounded/offline jobs.
 *
 * <p>Oracle DATE is timestamp-like and therefore maps to Flux TIMESTAMP.
 * LOB/RAW types map to existing STRING/BYTES primitives. Complex or extension
 * types that can be represented textually fall back to STRING instead of
 * introducing Oracle-specific runtime types.</p>
 */
public final class OracleTypeMapper
        implements JdbcTypeMapper {

    private static final int MAX_DECIMAL_PRECISION = 38;
    private static final int DEFAULT_DECIMAL_SCALE = 18;
    private static final int MAX_TIMESTAMP_PRECISION = 9;
    private static final long MAX_VARCHAR_LENGTH = 4000L;
    private static final long MAX_RAW_LENGTH = 2000L;

    @Override
    public Column map(
            ResultSetMetaData metadata,
            int columnIndex)
            throws SQLException {

        String name =
                firstText(
                        metadata.getColumnLabel(
                                columnIndex),
                        metadata.getColumnName(
                                columnIndex));

        String sourceType =
                metadata.getColumnTypeName(
                        columnIndex);

        int precision =
                metadata.getPrecision(
                        columnIndex);

        int scale =
                metadata.getScale(
                        columnIndex);

        FluxDataType<?> type =
                mapType(
                        metadata.getColumnType(
                                columnIndex),
                        sourceType,
                        precision,
                        scale);

        Column.Builder builder =
                Column.builder(
                        name,
                        type)
                        .nullable(
                                metadata.isNullable(
                                        columnIndex)
                                        != ResultSetMetaData
                                        .columnNoNulls)
                        .sourceType(
                                sourceType);

        applyProperties(
                builder,
                type,
                sourceType,
                precision,
                scale);

        return builder.build();
    }

    /**
     * Maps one ALL_TAB_COLUMNS row returned by OracleCatalog.
     */
    public Column toColumn(
            ResultSet row)
            throws SQLException {

        String name =
                row.getString(
                        "COLUMN_NAME");

        String dataType =
                row.getString(
                        "DATA_TYPE");

        Long dataLength =
                longValue(
                        row,
                        "DATA_LENGTH");

        Long charLength =
                longValue(
                        row,
                        "CHAR_LENGTH");

        Integer precision =
                integer(
                        row,
                        "DATA_PRECISION");

        Integer scale =
                integer(
                        row,
                        "DATA_SCALE");

        String sourceType =
                buildSourceType(
                        dataType,
                        dataLength,
                        charLength,
                        precision,
                        scale);

        FluxDataType<?> type =
                mapType(
                        Types.OTHER,
                        dataType,
                        value(precision),
                        value(scale));

        Column.Builder builder =
                Column.builder(
                        name,
                        type)
                        .nullable(
                                !"N".equalsIgnoreCase(
                                        row.getString(
                                                "NULLABLE")))
                        .defaultValue(
                                row.getObject(
                                        "DATA_DEFAULT"))
                        .comment(
                                row.getString(
                                        "COLUMN_COMMENT"))
                        .sourceType(
                                sourceType);

        long logicalLength =
                charLength != null
                        && charLength > 0
                        ? charLength
                        : dataLength == null
                        ? 0L
                        : dataLength;

        applyProperties(
                builder,
                type,
                dataType,
                logicalLength,
                value(precision),
                value(scale));

        return builder.build();
    }

    @Override
    public String toDatabaseType(
            Column column) {

        return toDatabaseType(
                column,
                false);
    }

    public String toDatabaseType(
            Column column,
            boolean preserveSourceType) {

        if (preserveSourceType
                && canPreserve(
                column.getSourceType())) {

            return column.getSourceType()
                    .trim();
        }

        SqlType sqlType =
                column.getDataType()
                        .getSqlType();

        switch (sqlType) {
            case STRING:
                return stringType(
                        column);

            case BOOLEAN:
                /*
                 * NUMBER(1) keeps compatibility with Oracle releases
                 * before SQL BOOLEAN became generally available.
                 */
                return "NUMBER(1)";

            case TINYINT:
                return "NUMBER(3)";

            case SMALLINT:
                return "NUMBER(5)";

            case INT:
                return "NUMBER(10)";

            case BIGINT:
                return "NUMBER(19)";

            case FLOAT:
                return "BINARY_FLOAT";

            case DOUBLE:
                return "BINARY_DOUBLE";

            case DECIMAL:
                return decimalType(
                        column);

            case BYTES:
                return binaryType(
                        column);

            case DATE:
                return "DATE";

            case TIME:
                throw new IllegalArgumentException(
                        "Oracle 没有独立 TIME 列类型，"
                                + "请先转换字段，column="
                                + column.getName());

            case TIMESTAMP:
                return temporalType(
                        "TIMESTAMP",
                        column.getPrecision());

            case TIMESTAMP_TZ:
                return temporalType(
                        "TIMESTAMP",
                        column.getPrecision())
                        + " WITH TIME ZONE";

            default:
                throw new IllegalArgumentException(
                        "Oracle 不支持 Flux 类型："
                                + sqlType
                                + "，column="
                                + column.getName());
        }
    }

    private FluxDataType<?> mapType(
            int jdbcType,
            String sourceType,
            int precision,
            int scale) {

        String type =
                normalize(
                        sourceType);

        if (type.startsWith(
                "TIMESTAMP")) {

            return type.contains(
                    "WITH TIME ZONE")
                    || type.contains(
                    "WITH LOCAL TIME ZONE")
                    ? BasicType
                    .TIMESTAMP_TZ_TYPE
                    : BasicType
                    .TIMESTAMP_TYPE;
        }

        if ("DATE".equals(type)) {
            /*
             * Oracle DATE stores date + time to seconds.
             */
            return BasicType.TIMESTAMP_TYPE;
        }

        if ("NUMBER".equals(type)
                || type.startsWith(
                "NUMBER(")
                || "INTEGER".equals(type)) {

            return numberType(
                    precision,
                    scale);
        }

        if ("FLOAT".equals(type)
                || type.startsWith(
                "FLOAT(")) {

            return new DecimalType(
                    MAX_DECIMAL_PRECISION,
                    DEFAULT_DECIMAL_SCALE);
        }

        if ("BINARY_FLOAT".equals(type)
                || "REAL".equals(type)) {

            return BasicType.FLOAT_TYPE;
        }

        if ("BINARY_DOUBLE".equals(type)) {
            return BasicType.DOUBLE_TYPE;
        }

        if ("BOOLEAN".equals(type)) {
            return BasicType.BOOLEAN_TYPE;
        }

        if (isStringType(type)) {
            return BasicType.STRING_TYPE;
        }

        if (isBinaryType(type)) {
            return BasicType.BYTES_TYPE;
        }

        if (type.startsWith(
                "INTERVAL")) {

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
                return numberType(
                        precision,
                        scale);

            case Types.DATE:
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

    private static FluxDataType<?> numberType(
            int precision,
            int scale) {

        if (scale == -127
                || precision <= 0) {

            return new DecimalType(
                    MAX_DECIMAL_PRECISION,
                    DEFAULT_DECIMAL_SCALE);
        }

        if (scale <= 0) {
            int integerPrecision =
                    Math.min(
                            MAX_DECIMAL_PRECISION,
                            precision
                                    + Math.max(
                                    0,
                                    -scale));

            if (integerPrecision <= 9) {
                return BasicType.INT_TYPE;
            }

            if (integerPrecision <= 18) {
                return BasicType.LONG_TYPE;
            }

            return new DecimalType(
                    integerPrecision,
                    0);
        }

        int safePrecision =
                Math.max(
                        1,
                        Math.min(
                                precision,
                                MAX_DECIMAL_PRECISION));

        int safeScale =
                Math.max(
                        0,
                        Math.min(
                                scale,
                                safePrecision));

        return new DecimalType(
                safePrecision,
                safeScale);
    }

    private static boolean isStringType(
            String type) {

        return type.isEmpty()
                || type.startsWith("CHAR")
                || type.startsWith("NCHAR")
                || type.startsWith("VARCHAR")
                || type.startsWith("VARCHAR2")
                || type.startsWith("NVARCHAR2")
                || "LONG".equals(type)
                || "ROWID".equals(type)
                || "UROWID".equals(type)
                || "CLOB".equals(type)
                || "NCLOB".equals(type)
                || "XMLTYPE".equals(type)
                || "SYS.XMLTYPE".equals(type)
                || "JSON".equals(type);
    }

    private static boolean isBinaryType(
            String type) {

        return type.startsWith("RAW")
                || "LONG RAW".equals(type)
                || "BLOB".equals(type)
                || "BFILE".equals(type);
    }

    private static boolean canPreserve(
            String sourceType) {

        String type =
                normalize(
                        sourceType);

        return type.startsWith("NUMBER")
                || type.startsWith("FLOAT")
                || "INTEGER".equals(type)
                || "BINARY_FLOAT".equals(type)
                || "BINARY_DOUBLE".equals(type)
                || type.startsWith("CHAR")
                || type.startsWith("NCHAR")
                || type.startsWith("VARCHAR")
                || type.startsWith("VARCHAR2")
                || type.startsWith("NVARCHAR2")
                || "CLOB".equals(type)
                || "NCLOB".equals(type)
                || type.startsWith("RAW")
                || "BLOB".equals(type)
                || "DATE".equals(type)
                || type.startsWith("TIMESTAMP")
                || "ROWID".equals(type)
                || "UROWID".equals(type)
                || "XMLTYPE".equals(type)
                || "SYS.XMLTYPE".equals(type);
    }

    private static String stringType(
            Column column) {

        Long length =
                column.getLength();

        if (length == null
                || length <= 0
                || length > MAX_VARCHAR_LENGTH) {

            return "CLOB";
        }

        return "VARCHAR2("
                + length
                + ")";
    }

    private static String binaryType(
            Column column) {

        Long length =
                column.getLength();

        if (length == null
                || length <= 0
                || length > MAX_RAW_LENGTH) {

            return "BLOB";
        }

        return "RAW("
                + length
                + ")";
    }

    private static String decimalType(
            Column column) {

        int precision =
                column.getPrecision()
                        == null
                        ? MAX_DECIMAL_PRECISION
                        : column.getPrecision();

        int scale =
                column.getScale()
                        == null
                        ? 0
                        : column.getScale();

        precision =
                Math.max(
                        1,
                        Math.min(
                                precision,
                                MAX_DECIMAL_PRECISION));

        scale =
                Math.max(
                        0,
                        Math.min(
                                scale,
                                precision));

        return "NUMBER("
                + precision
                + ","
                + scale
                + ")";
    }

    private static String temporalType(
            String type,
            Integer precision) {

        if (precision == null
                || precision <= 0) {

            return type;
        }

        return type
                + "("
                + Math.min(
                precision,
                MAX_TIMESTAMP_PRECISION)
                + ")";
    }

    private static void applyProperties(
            Column.Builder builder,
            FluxDataType<?> dataType,
            String sourceType,
            int precision,
            int scale) {

        applyProperties(
                builder,
                dataType,
                sourceType,
                precision,
                precision,
                scale);
    }

    private static void applyProperties(
            Column.Builder builder,
            FluxDataType<?> dataType,
            String sourceType,
            long length,
            int precision,
            int scale) {

        SqlType sqlType =
                dataType.getSqlType();

        if (sqlType == SqlType.STRING
                || sqlType == SqlType.BYTES) {

            if (length > 0) {
                builder.length(
                        length);
            }

            return;
        }

        if (sqlType == SqlType.DECIMAL
                && dataType
                instanceof DecimalType) {

            DecimalType decimalType =
                    (DecimalType) dataType;

            builder.precision(
                    decimalType.getPrecision());

            builder.scale(
                    decimalType.getScale());

            return;
        }

        if (sqlType == SqlType.TIMESTAMP
                || sqlType
                == SqlType.TIMESTAMP_TZ) {

            int timestampPrecision =
                    Math.max(
                            0,
                            Math.min(
                                    scale > 0
                                            ? scale
                                            : precision,
                                    MAX_TIMESTAMP_PRECISION));

            if (timestampPrecision > 0) {
                builder.precision(
                        timestampPrecision);
            }
        }
    }

    private static String buildSourceType(
            String dataType,
            Long dataLength,
            Long charLength,
            Integer precision,
            Integer scale) {

        String type =
                dataType == null
                        ? ""
                        : dataType.trim();

        String normalized =
                normalize(type);

        if (normalized.startsWith("VARCHAR")
                || normalized.startsWith("VARCHAR2")
                || normalized.startsWith("CHAR")
                || normalized.startsWith("NCHAR")
                || normalized.startsWith("NVARCHAR2")) {

            Long length =
                    charLength != null
                            && charLength > 0
                            ? charLength
                            : dataLength;

            return length != null
                    && length > 0
                    ? type
                    + "("
                    + length
                    + ")"
                    : type;
        }

        if ("RAW".equals(normalized)) {
            return dataLength != null
                    && dataLength > 0
                    ? type
                    + "("
                    + dataLength
                    + ")"
                    : type;
        }

        if ("NUMBER".equals(normalized)) {
            if (precision == null
                    || precision <= 0) {

                return type;
            }

            if (scale == null) {
                return type
                        + "("
                        + precision
                        + ")";
            }

            return type
                    + "("
                    + precision
                    + ","
                    + scale
                    + ")";
        }

        if ("FLOAT".equals(normalized)
                && precision != null
                && precision > 0) {

            return type
                    + "("
                    + precision
                    + ")";
        }

        return type;
    }

    private static Integer integer(
            ResultSet row,
            String name)
            throws SQLException {

        Object value =
                row.getObject(
                        name);

        return value == null
                ? null
                : ((Number) value)
                .intValue();
    }

    private static Long longValue(
            ResultSet row,
            String name)
            throws SQLException {

        Object value =
                row.getObject(
                        name);

        return value == null
                ? null
                : ((Number) value)
                .longValue();
    }

    private static int value(
            Integer value) {

        return value == null
                ? 0
                : value;
    }

    private static String firstText(
            String first,
            String second) {

        return first != null
                && !first.trim().isEmpty()
                ? first
                : second;
    }

    private static String normalize(
            String value) {

        return value == null
                ? ""
                : value.trim()
                .toUpperCase(
                        Locale.ROOT)
                .replaceAll(
                        "\\s+",
                        " ");
    }
}
