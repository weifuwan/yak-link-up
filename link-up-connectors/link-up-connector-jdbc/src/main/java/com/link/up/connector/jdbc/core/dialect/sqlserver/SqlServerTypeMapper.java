package com.link.up.connector.jdbc.core.dialect.sqlserver;

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

/** SQL Server type mapping for bounded Source/Sink jobs. */
public final class SqlServerTypeMapper implements JdbcTypeMapper {

    private static final int MAX_DECIMAL_PRECISION = 38;
    private static final int MAX_TEMPORAL_PRECISION = 7;
    private static final long MAX_NVARCHAR_LENGTH = 4000L;
    private static final long MAX_VARBINARY_LENGTH = 8000L;

    @Override
    public Column map(ResultSetMetaData metadata, int columnIndex) throws SQLException {
        String name = firstText(
                metadata.getColumnLabel(columnIndex),
                metadata.getColumnName(columnIndex));
        String nativeType = metadata.getColumnTypeName(columnIndex);
        int precision = metadata.getPrecision(columnIndex);
        int scale = metadata.getScale(columnIndex);
        FluxDataType<?> type = mapNativeType(
                nativeType, metadata.getColumnType(columnIndex), precision, scale);

        Column.Builder builder = Column.builder(name, type)
                .nullable(metadata.isNullable(columnIndex)
                        != ResultSetMetaData.columnNoNulls)
                .sourceType(buildMetadataSourceType(nativeType, precision, scale));
        applyProperties(builder, type, nativeType, precision, precision, scale);
        return builder.build();
    }

    /** Maps one sys.columns row returned by SqlServerCatalog. */
    public Column toColumn(ResultSet row) throws SQLException {
        String name = row.getString("COLUMN_NAME");
        String nativeType = row.getString("DATA_TYPE");
        Integer maxLength = integer(row, "MAX_LENGTH");
        Integer precision = integer(row, "NUMERIC_PRECISION");
        Integer scale = integer(row, "NUMERIC_SCALE");
        int p = value(precision);
        int s = value(scale);
        FluxDataType<?> type = mapNativeType(nativeType, Types.OTHER, p, s);
        long logicalLength = logicalLength(nativeType, maxLength);
        String sourceType = buildCatalogSourceType(
                nativeType, maxLength, precision, scale);

        Column.Builder builder = Column.builder(name, type)
                .nullable(row.getBoolean("IS_NULLABLE"))
                .defaultValue(row.getObject("COLUMN_DEFAULT"))
                .autoIncrement(row.getBoolean("IS_IDENTITY"))
                .comment(row.getString("COLUMN_COMMENT"))
                .sourceType(sourceType);
        applyProperties(builder, type, nativeType, logicalLength, p, s);
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
                return "BIT";
            case TINYINT:
            case SMALLINT:
                return "SMALLINT";
            case INT:
                return "INT";
            case BIGINT:
                return "BIGINT";
            case FLOAT:
                return "REAL";
            case DOUBLE:
                return "FLOAT";
            case DECIMAL:
                return decimalType(column);
            case BYTES:
                return binaryType(column);
            case DATE:
                return "DATE";
            case TIME:
                return temporalType("TIME", column.getPrecision());
            case TIMESTAMP:
                return temporalType("DATETIME2", column.getPrecision());
            case TIMESTAMP_TZ:
                return temporalType("DATETIMEOFFSET", column.getPrecision());
            default:
                throw new IllegalArgumentException(
                        "SQL Server 不支持 Flux 类型："
                                + column.getDataType().getSqlType()
                                + "，column=" + column.getName());
        }
    }

    FluxDataType<?> mapNativeType(
            String sourceType, int jdbcType, int precision, int scale) {
        String type = baseType(sourceType);

        if ("bit".equals(type)) {
            return BasicType.BOOLEAN_TYPE;
        }
        if ("tinyint".equals(type)) {
            // SQL Server TINYINT is unsigned (0..255); Java Byte is not lossless.
            return BasicType.SHORT_TYPE;
        }
        if ("smallint".equals(type)) {
            return BasicType.SHORT_TYPE;
        }
        if ("int".equals(type)) {
            return BasicType.INT_TYPE;
        }
        if ("bigint".equals(type)) {
            return BasicType.LONG_TYPE;
        }
        if ("real".equals(type)) {
            return BasicType.FLOAT_TYPE;
        }
        if ("float".equals(type)) {
            return precision > 0 && precision <= 24
                    ? BasicType.FLOAT_TYPE
                    : BasicType.DOUBLE_TYPE;
        }
        if ("decimal".equals(type) || "numeric".equals(type)) {
            return decimal(precision, scale);
        }
        if ("money".equals(type)) {
            return new DecimalType(19, 4);
        }
        if ("smallmoney".equals(type)) {
            return new DecimalType(10, 4);
        }
        if ("date".equals(type)) {
            return BasicType.DATE_TYPE;
        }
        if ("time".equals(type)) {
            return BasicType.TIME_TYPE;
        }
        if ("datetimeoffset".equals(type)) {
            return BasicType.TIMESTAMP_TZ_TYPE;
        }
        if ("datetime".equals(type)
                || "datetime2".equals(type)
                || "smalldatetime".equals(type)) {
            return BasicType.TIMESTAMP_TYPE;
        }
        if ("timestamp".equals(type) || "rowversion".equals(type)) {
            // SQL Server timestamp is rowversion, not a temporal value.
            return BasicType.BYTES_TYPE;
        }
        if (isBinary(type)) {
            return BasicType.BYTES_TYPE;
        }
        if (isString(type)) {
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
                return BasicType.FLOAT_TYPE;
            case Types.FLOAT:
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
            default:
                return BasicType.STRING_TYPE;
        }
    }

    private static boolean isString(String type) {
        return type.isEmpty()
                || "char".equals(type)
                || "varchar".equals(type)
                || "nchar".equals(type)
                || "nvarchar".equals(type)
                || "text".equals(type)
                || "ntext".equals(type)
                || "xml".equals(type)
                || "uniqueidentifier".equals(type)
                || "sql_variant".equals(type)
                || "geometry".equals(type)
                || "geography".equals(type)
                || "hierarchyid".equals(type);
    }

    private static boolean isBinary(String type) {
        return "binary".equals(type)
                || "varbinary".equals(type)
                || "image".equals(type);
    }

    private static boolean canPreserve(String sourceType) {
        String type = baseType(sourceType);
        return "bit".equals(type)
                || "tinyint".equals(type)
                || "smallint".equals(type)
                || "int".equals(type)
                || "bigint".equals(type)
                || "real".equals(type)
                || "float".equals(type)
                || "decimal".equals(type)
                || "numeric".equals(type)
                || "money".equals(type)
                || "smallmoney".equals(type)
                || "char".equals(type)
                || "varchar".equals(type)
                || "nchar".equals(type)
                || "nvarchar".equals(type)
                || "binary".equals(type)
                || "varbinary".equals(type)
                || "date".equals(type)
                || "time".equals(type)
                || "datetime".equals(type)
                || "datetime2".equals(type)
                || "smalldatetime".equals(type)
                || "datetimeoffset".equals(type)
                || "uniqueidentifier".equals(type)
                || "xml".equals(type);
    }

    private static String stringType(Column column) {
        Long length = column.getLength();
        return length != null && length > 0 && length <= MAX_NVARCHAR_LENGTH
                ? "NVARCHAR(" + length + ")"
                : "NVARCHAR(MAX)";
    }

    private static String binaryType(Column column) {
        Long length = column.getLength();
        return length != null && length > 0 && length <= MAX_VARBINARY_LENGTH
                ? "VARBINARY(" + length + ")"
                : "VARBINARY(MAX)";
    }

    private static String decimalType(Column column) {
        int precision = column.getPrecision() == null
                ? MAX_DECIMAL_PRECISION : column.getPrecision();
        int scale = column.getScale() == null ? 0 : column.getScale();
        precision = Math.max(1, Math.min(precision, MAX_DECIMAL_PRECISION));
        scale = Math.max(0, Math.min(scale, precision));
        return "DECIMAL(" + precision + "," + scale + ")";
    }

    private static String temporalType(String type, Integer precision) {
        if (precision == null || precision <= 0) {
            return type;
        }
        return type + "(" + Math.min(precision, MAX_TEMPORAL_PRECISION) + ")";
    }

    private static DecimalType decimal(int precision, int scale) {
        int p = precision <= 0 ? MAX_DECIMAL_PRECISION
                : Math.min(precision, MAX_DECIMAL_PRECISION);
        int s = Math.max(0, Math.min(scale, p));
        return new DecimalType(p, s);
    }

    private static void applyProperties(
            Column.Builder builder,
            FluxDataType<?> dataType,
            String nativeType,
            long length,
            int precision,
            int scale) {
        SqlType sqlType = dataType.getSqlType();
        if (sqlType == SqlType.STRING || sqlType == SqlType.BYTES) {
            if (length > 0) {
                builder.length(length);
            }
        } else if (sqlType == SqlType.DECIMAL && dataType instanceof DecimalType) {
            DecimalType decimal = (DecimalType) dataType;
            builder.precision(decimal.getPrecision());
            builder.scale(decimal.getScale());
        } else if (sqlType == SqlType.TIME
                || sqlType == SqlType.TIMESTAMP
                || sqlType == SqlType.TIMESTAMP_TZ) {
            if (scale > 0) {
                builder.precision(Math.min(scale, MAX_TEMPORAL_PRECISION));
            }
        }
        if ("uniqueidentifier".equals(baseType(nativeType)) && length <= 0) {
            builder.length(36L);
        }
    }

    private static long logicalLength(String nativeType, Integer maxLength) {
        if (maxLength == null || maxLength <= 0) {
            return 0L;
        }
        String type = baseType(nativeType);
        if ("nchar".equals(type) || "nvarchar".equals(type)) {
            return maxLength / 2L;
        }
        return maxLength.longValue();
    }

    private static String buildMetadataSourceType(
            String nativeType, int precision, int scale) {
        String type = baseType(nativeType);
        if ("decimal".equals(type) || "numeric".equals(type)) {
            return nativeType + "(" + Math.max(1, precision) + "," + Math.max(0, scale) + ")";
        }
        if ("time".equals(type) || "datetime2".equals(type)
                || "datetimeoffset".equals(type)) {
            return scale > 0 ? nativeType + "(" + Math.min(scale, 7) + ")" : nativeType;
        }
        if ("char".equals(type) || "varchar".equals(type)
                || "nchar".equals(type) || "nvarchar".equals(type)
                || "binary".equals(type) || "varbinary".equals(type)) {
            return precision > 0 ? nativeType + "(" + precision + ")" : nativeType;
        }
        return nativeType;
    }

    private static String buildCatalogSourceType(
            String nativeType,
            Integer maxLength,
            Integer precision,
            Integer scale) {
        String type = baseType(nativeType);
        if ("decimal".equals(type) || "numeric".equals(type)) {
            return nativeType + "(" + Math.max(1, value(precision))
                    + "," + Math.max(0, value(scale)) + ")";
        }
        if ("time".equals(type) || "datetime2".equals(type)
                || "datetimeoffset".equals(type)) {
            return value(scale) > 0
                    ? nativeType + "(" + Math.min(value(scale), 7) + ")" : nativeType;
        }
        if ("char".equals(type) || "varchar".equals(type)
                || "nchar".equals(type) || "nvarchar".equals(type)
                || "binary".equals(type) || "varbinary".equals(type)) {
            if (maxLength != null && maxLength == -1) {
                return nativeType + "(max)";
            }
            long length = logicalLength(nativeType, maxLength);
            return length > 0 ? nativeType + "(" + length + ")" : nativeType;
        }
        return nativeType;
    }

    private static String baseType(String sourceType) {
        if (sourceType == null) {
            return "";
        }
        String value = sourceType.trim().toLowerCase(Locale.ROOT);
        int parenthesis = value.indexOf('(');
        return parenthesis < 0 ? value : value.substring(0, parenthesis).trim();
    }

    private static Integer integer(ResultSet row, String name) throws SQLException {
        Object value = row.getObject(name);
        return value == null ? null : ((Number) value).intValue();
    }

    private static int value(Integer value) {
        return value == null ? 0 : value;
    }

    private static String firstText(String first, String second) {
        return first != null && !first.trim().isEmpty() ? first : second;
    }
}
