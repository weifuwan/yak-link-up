package com.link.up.connector.file.config;

import com.link.up.api.table.type.BasicType;
import com.link.up.api.table.type.DecimalType;
import com.link.up.api.table.type.FluxDataType;
import com.link.up.api.table.type.SqlType;

import java.util.Locale;
import java.util.Map;

/**
 * One declared schema column of the file Source.
 *
 * <p>Files carry no catalog, so every type comes from the explicit schema
 * option or degrades to STRING; anything outside the Flux scalar vocabulary
 * fails here instead of at read time.
 */
public final class FileColumnSpec {

    private static final String SUPPORTED_TYPES =
            "string, boolean, tinyint, smallint, int, bigint, float, double, "
                    + "decimal(p,s), bytes, date, time, timestamp, timestamp_tz";

    private final String name;
    private final SqlType sqlType;
    private final FluxDataType<?> dataType;
    private final int precision;
    private final int scale;

    public FileColumnSpec(
            String name,
            SqlType sqlType,
            FluxDataType<?> dataType,
            int precision,
            int scale) {

        this.name = name;
        this.sqlType = sqlType;
        this.dataType = dataType;
        this.precision = precision;
        this.scale = scale;
    }

    public static FileColumnSpec parse(int index, Map<?, ?> raw) {
        Object rawName = raw.get("name");
        if (!(rawName instanceof String) || ((String) rawName).trim().isEmpty()) {
            throw new IllegalArgumentException(
                    "schema entry #" + index + " must declare a non-blank name");
        }
        String name = ((String) rawName).trim();

        Object rawType = raw.get("type");
        if (!(rawType instanceof String) || ((String) rawType).trim().isEmpty()) {
            throw new IllegalArgumentException(
                    "Column '" + name + "' must declare a type; supported: " + SUPPORTED_TYPES);
        }
        return parseType(name, ((String) rawType).trim());
    }

    private static FileColumnSpec parseType(String name, String type) {
        String normalized = type.toLowerCase(Locale.ROOT);
        if (normalized.startsWith("decimal")) {
            return decimalColumn(name, normalized);
        }

        switch (normalized) {
            case "string":
                return new FileColumnSpec(name, SqlType.STRING, BasicType.STRING_TYPE, 0, 0);
            case "boolean":
                return new FileColumnSpec(name, SqlType.BOOLEAN, BasicType.BOOLEAN_TYPE, 0, 0);
            case "tinyint":
                return new FileColumnSpec(name, SqlType.TINYINT, BasicType.BYTE_TYPE, 0, 0);
            case "smallint":
                return new FileColumnSpec(name, SqlType.SMALLINT, BasicType.SHORT_TYPE, 0, 0);
            case "int":
                return new FileColumnSpec(name, SqlType.INT, BasicType.INT_TYPE, 0, 0);
            case "bigint":
                return new FileColumnSpec(name, SqlType.BIGINT, BasicType.LONG_TYPE, 0, 0);
            case "float":
                return new FileColumnSpec(name, SqlType.FLOAT, BasicType.FLOAT_TYPE, 0, 0);
            case "double":
                return new FileColumnSpec(name, SqlType.DOUBLE, BasicType.DOUBLE_TYPE, 0, 0);
            case "bytes":
                return new FileColumnSpec(name, SqlType.BYTES, BasicType.BYTES_TYPE, 0, 0);
            case "date":
                return new FileColumnSpec(name, SqlType.DATE, BasicType.DATE_TYPE, 0, 0);
            case "time":
                return new FileColumnSpec(name, SqlType.TIME, BasicType.TIME_TYPE, 0, 0);
            case "timestamp":
                return new FileColumnSpec(name, SqlType.TIMESTAMP, BasicType.TIMESTAMP_TYPE, 0, 0);
            case "timestamp_tz":
                return new FileColumnSpec(name, SqlType.TIMESTAMP_TZ, BasicType.TIMESTAMP_TZ_TYPE, 0, 0);
            default:
                throw new IllegalArgumentException(
                        "Unsupported file column type '" + type + "' for column '" + name
                                + "'; supported: " + SUPPORTED_TYPES);
        }
    }

    private static FileColumnSpec decimalColumn(String name, String type) {
        int open = type.indexOf('(');
        int close = type.indexOf(')');
        if (open < 0 || close <= open) {
            throw new IllegalArgumentException(
                    "Column '" + name + "' decimal type requires precision and scale: decimal(p,s)");
        }

        String[] parts = type.substring(open + 1, close).split(",");
        if (parts.length != 2) {
            throw new IllegalArgumentException(
                    "Column '" + name + "' decimal type requires precision and scale: decimal(p,s)");
        }

        try {
            int precision = Integer.parseInt(parts[0].trim());
            int scale = Integer.parseInt(parts[1].trim());
            DecimalType decimalType = new DecimalType(precision, scale);
            return new FileColumnSpec(name, SqlType.DECIMAL, decimalType, precision, scale);
        } catch (RuntimeException failure) {
            throw new IllegalArgumentException(
                    "Column '" + name + "' has an invalid decimal type: " + type,
                    failure);
        }
    }

    public String getName() {
        return name;
    }

    public SqlType getSqlType() {
        return sqlType;
    }

    public FluxDataType<?> getDataType() {
        return dataType;
    }

    public int getPrecision() {
        return precision;
    }

    public int getScale() {
        return scale;
    }
}
