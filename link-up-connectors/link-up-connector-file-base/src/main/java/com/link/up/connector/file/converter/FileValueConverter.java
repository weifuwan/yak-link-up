package com.link.up.connector.file.converter;

import com.link.up.api.table.type.SqlType;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Base64;

/**
 * Converts one raw text value to the declared Flux physical type.
 *
 * <p>Conversion is strict: values that cannot be represented fail with the
 * column identity attached, never silently truncated.
 */
public final class FileValueConverter {

    private static final DateTimeFormatter TIME_FORMAT = DateTimeFormatter.ofPattern("HH:mm:ss");
    private static final DateTimeFormatter TIMESTAMP_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss");
    private static final DateTimeFormatter TIMESTAMP_TZ_FORMAT = DateTimeFormatter.ISO_OFFSET_DATE_TIME;

    private FileValueConverter() {
    }

    public static Object convert(
            SqlType sqlType,
            int scale,
            String raw) {

        if (raw == null) {
            return null;
        }

        try {
            switch (sqlType) {
                case STRING:
                    return raw;
                case BOOLEAN:
                    if ("true".equals(raw) || "false".equals(raw)) {
                        return Boolean.valueOf(raw);
                    }
                    throw new IllegalArgumentException("expected true/false");
                case TINYINT:
                    return Byte.valueOf(raw);
                case SMALLINT:
                    return Short.valueOf(raw);
                case INT:
                    return Integer.valueOf(raw);
                case BIGINT:
                    return Long.valueOf(raw);
                case FLOAT:
                    return Float.valueOf(raw);
                case DOUBLE:
                    return Double.valueOf(raw);
                case DECIMAL:
                    // scale < 0 keeps the raw scale for schemas without a declared scale.
                    return scale < 0 ? new BigDecimal(raw) : new BigDecimal(raw).setScale(scale);
                case BYTES:
                    return Base64.getDecoder().decode(raw);
                case DATE:
                    return LocalDate.parse(raw);
                case TIME:
                    return LocalTime.parse(raw, TIME_FORMAT);
                case TIMESTAMP:
                    return LocalDateTime.parse(raw, TIMESTAMP_FORMAT);
                case TIMESTAMP_TZ:
                    return OffsetDateTime.parse(raw, TIMESTAMP_TZ_FORMAT);
                default:
                    throw new IllegalArgumentException("unsupported file column type " + sqlType);
            }
        } catch (RuntimeException failure) {
            throw new IllegalArgumentException(
                    "Cannot convert value '" + raw + "' to " + sqlType,
                    failure);
        }
    }
}
