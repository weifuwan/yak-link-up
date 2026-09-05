package com.link.up.connector.starrocks.schema;

import com.link.up.api.table.catalog.Column;
import com.link.up.api.table.catalog.TableSchema;
import com.link.up.api.table.type.BasicType;
import com.link.up.api.table.type.DecimalType;
import com.link.up.api.table.type.FluxDataType;

import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Parses the explicit schema required by the StarRocks native scanner. */
public final class StarRocksSchemaParser {

    private static final Pattern DECIMAL_PATTERN =
            Pattern.compile(
                    "decimal(?:v2|v3|32|64|128)?\\s*\\(\\s*(\\d+)\\s*,\\s*(\\d+)\\s*\\)",
                    Pattern.CASE_INSENSITIVE);

    private static final Pattern CHAR_PATTERN =
            Pattern.compile("(?:char|varchar)\\s*\\(\\s*\\d+\\s*\\)", Pattern.CASE_INSENSITIVE);

    private StarRocksSchemaParser() {
    }

    public static TableSchema parse(Map<String, Object> schemaFields) {
        Objects.requireNonNull(schemaFields, "schemaFields must not be null");
        if (schemaFields.isEmpty()) {
            throw new IllegalArgumentException(
                    "StarRocks Native Source schema.fields must define at least one field");
        }

        TableSchema.Builder builder = TableSchema.builder();
        for (Map.Entry<String, Object> entry : schemaFields.entrySet()) {
            String fieldName = requireText(entry.getKey(), "field name");
            String sourceType = requireText(String.valueOf(entry.getValue()), "field type");
            builder.column(
                    Column.builder(fieldName, resolveType(sourceType))
                            .nullable(true)
                            .sourceType(sourceType)
                            .build());
        }
        return builder.build();
    }

    static FluxDataType<?> resolveType(String sourceType) {
        String normalized = requireText(sourceType, "sourceType")
                .trim()
                .toLowerCase(Locale.ROOT);

        Matcher decimal = DECIMAL_PATTERN.matcher(normalized);
        if (decimal.matches()) {
            int precision = Integer.parseInt(decimal.group(1));
            int scale = Integer.parseInt(decimal.group(2));
            if (precision < 1 || precision > 38) {
                throw new IllegalArgumentException(
                        "StarRocks DECIMAL precision must be between 1 and 38: " + sourceType);
            }
            if (scale < 0 || scale > precision) {
                throw new IllegalArgumentException(
                        "StarRocks DECIMAL scale must be between 0 and precision: " + sourceType);
            }
            return new DecimalType(precision, scale);
        }

        if (CHAR_PATTERN.matcher(normalized).matches()) {
            return BasicType.STRING_TYPE;
        }

        if (normalized.startsWith("array<") || normalized.startsWith("map<")) {
            throw new IllegalArgumentException(
                    "StarRocks Native Source Stage 1 does not support ARRAY/MAP yet; "
                            + "map the column to a supported scalar representation explicitly: "
                            + sourceType);
        }

        switch (normalized) {
            case "boolean":
            case "bool":
                return BasicType.BOOLEAN_TYPE;
            case "tinyint":
                return BasicType.BYTE_TYPE;
            case "smallint":
                return BasicType.SHORT_TYPE;
            case "int":
            case "integer":
                return BasicType.INT_TYPE;
            case "bigint":
                return BasicType.LONG_TYPE;
            case "largeint":
                // StarRocks LARGEINT is signed 128-bit. Flux DECIMAL(38, 0)
                // cannot represent its entire domain safely, so keep exact text.
                return BasicType.STRING_TYPE;
            case "float":
                return BasicType.FLOAT_TYPE;
            case "double":
                return BasicType.DOUBLE_TYPE;
            case "char":
            case "varchar":
            case "string":
            case "json":
                return BasicType.STRING_TYPE;
            case "date":
                return BasicType.DATE_TYPE;
            case "time":
                return BasicType.TIME_TYPE;
            case "datetime":
            case "timestamp":
                return BasicType.TIMESTAMP_TYPE;
            case "null":
                return BasicType.NULL_TYPE;
            default:
                throw new IllegalArgumentException(
                        "Unsupported StarRocks Native Source type: " + sourceType);
        }
    }

    private static String requireText(String value, String name) {
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalArgumentException(name + " must not be empty");
        }
        return value.trim();
    }
}
