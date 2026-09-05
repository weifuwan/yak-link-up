package com.link.up.connector.starrocks.converter;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.link.up.api.table.catalog.Column;
import com.link.up.api.table.catalog.TableSchema;
import com.link.up.api.table.type.FluxRow;
import com.link.up.api.table.type.SqlType;
import com.link.up.connector.starrocks.config.StarRocksLoadFormat;
import com.link.up.connector.starrocks.config.StarRocksSinkConfig;

import java.io.ByteArrayOutputStream;
import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Serializes Flux rows into deterministic StarRocks Stream Load payloads. */
public final class StarRocksRowSerializer {

    private static final ObjectMapper JSON_MAPPER = new ObjectMapper();
    private static final char[] HEX = "0123456789ABCDEF".toCharArray();

    private final StarRocksSinkConfig config;
    private final TableSchema schema;
    private final byte[] rowDelimiterBytes;

    public StarRocksRowSerializer(
            StarRocksSinkConfig config,
            TableSchema schema) {
        if (config == null) {
            throw new IllegalArgumentException("config must not be null");
        }
        if (schema == null) {
            throw new IllegalArgumentException("schema must not be null");
        }
        this.config = config;
        this.schema = schema;
        this.rowDelimiterBytes = config.getRowDelimiter().getBytes(StandardCharsets.UTF_8);
    }

    public byte[] serializeRow(FluxRow row) {
        validateArity(row);
        if (config.getLoadFormat() == StarRocksLoadFormat.CSV) {
            return serializeCsvRow(row).getBytes(StandardCharsets.UTF_8);
        }
        return serializeJsonRow(row).getBytes(StandardCharsets.UTF_8);
    }

    public byte[] joinRecords(List<byte[]> records, long rawRecordBytes) {
        if (records == null || records.isEmpty()) {
            return new byte[0];
        }
        long payloadSize = payloadSizeBytes(records.size(), rawRecordBytes);
        if (payloadSize > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("StarRocks Stream Load payload is too large");
        }

        ByteArrayOutputStream output = new ByteArrayOutputStream((int) payloadSize);
        if (config.getLoadFormat() == StarRocksLoadFormat.JSON) {
            output.write('[');
            for (int i = 0; i < records.size(); i++) {
                if (i > 0) {
                    output.write(',');
                }
                write(output, records.get(i));
            }
            output.write(']');
            return output.toByteArray();
        }

        for (int i = 0; i < records.size(); i++) {
            if (i > 0) {
                write(output, rowDelimiterBytes);
            }
            write(output, records.get(i));
        }
        return output.toByteArray();
    }

    public long payloadSizeBytes(int recordCount, long rawRecordBytes) {
        if (recordCount <= 0) {
            return 0L;
        }
        if (config.getLoadFormat() == StarRocksLoadFormat.JSON) {
            return rawRecordBytes + 2L + Math.max(0, recordCount - 1);
        }
        return rawRecordBytes
                + ((long) Math.max(0, recordCount - 1) * rowDelimiterBytes.length);
    }

    public String csvColumnsHeader() {
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < schema.getColumnCount(); i++) {
            if (i > 0) {
                builder.append(',');
            }
            String name = schema.getColumn(i).getName();
            builder.append('`').append(name.replace("`", "``")).append('`');
        }
        return builder.toString();
    }

    private String serializeJsonRow(FluxRow row) {
        Map<String, Object> values = new LinkedHashMap<String, Object>();
        for (int i = 0; i < schema.getColumnCount(); i++) {
            Column column = schema.getColumn(i);
            values.put(
                    column.getName(),
                    normalizeForJson(row.getField(i), column.getDataType().getSqlType()));
        }
        try {
            return JSON_MAPPER.writeValueAsString(values);
        } catch (JsonProcessingException failure) {
            throw new IllegalArgumentException(
                    "Failed to serialize FluxRow as StarRocks JSON",
                    failure);
        }
    }

    private String serializeCsvRow(FluxRow row) {
        String separator = config.getColumnSeparator();
        StringBuilder builder = new StringBuilder(schema.getColumnCount() * 24);
        for (int i = 0; i < schema.getColumnCount(); i++) {
            if (i > 0) {
                builder.append(separator);
            }
            Object value = row.getField(i);
            if (value == null) {
                builder.append("\\N");
                continue;
            }
            SqlType sqlType = schema.getColumn(i).getDataType().getSqlType();
            String text = csvText(value, sqlType);
            if (text.contains(separator)
                    || text.contains(config.getRowDelimiter())
                    || text.indexOf('\n') >= 0
                    || text.indexOf('\r') >= 0) {
                throw new IllegalArgumentException(
                        "CSV value contains the configured column/row delimiter; "
                                + "use JSON load_format or choose safe delimiters");
            }
            builder.append(text);
        }
        return builder.toString();
    }

    private Object normalizeForJson(Object value, SqlType sqlType) {
        if (value == null) {
            return null;
        }
        switch (sqlType) {
            case BOOLEAN:
                return value instanceof Boolean
                        ? value
                        : Boolean.valueOf(String.valueOf(value));
            case TINYINT:
            case SMALLINT:
            case INT:
            case BIGINT:
                if (value instanceof Number) {
                    return value;
                }
                return Long.valueOf(String.valueOf(value));
            case FLOAT:
            case DOUBLE:
                if (value instanceof Number) {
                    return value;
                }
                return Double.valueOf(String.valueOf(value));
            case DECIMAL:
                if (value instanceof BigDecimal) {
                    return value;
                }
                return new BigDecimal(String.valueOf(value));
            case DATE:
                return value instanceof LocalDate ? value.toString() : String.valueOf(value);
            case TIME:
                return value instanceof LocalTime ? value.toString() : String.valueOf(value);
            case TIMESTAMP:
                return timestampText(value);
            case BYTES:
                throw new IllegalArgumentException(
                        "StarRocks BINARY/VARBINARY Stream Load is not supported with JSON; "
                                + "use load_format=CSV so bytes can be encoded as hexadecimal text");
            case STRING:
            case TIMESTAMP_TZ:
                return String.valueOf(value);
            case ARRAY:
            case MAP:
                return normalizeAny(value);
            case ROW:
                throw new IllegalArgumentException(
                        "StarRocks ROW/STRUCT Sink values require explicit nested field metadata; "
                                + "Stage 2 intentionally does not infer STRUCT fields from FluxRow values");
            case NULL:
                return null;
            default:
                return normalizeAny(value);
        }
    }

    private static Object normalizeAny(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof LocalDate
                || value instanceof LocalTime) {
            return value.toString();
        }
        if (value instanceof LocalDateTime) {
            return timestampText(value);
        }
        if (value instanceof byte[]) {
            throw new IllegalArgumentException(
                    "Nested binary values are not supported by the bounded StarRocks Stage 2 serializer");
        }
        if (value instanceof Map) {
            Map<?, ?> source = (Map<?, ?>) value;
            Map<String, Object> result = new LinkedHashMap<String, Object>();
            for (Map.Entry<?, ?> entry : source.entrySet()) {
                result.put(String.valueOf(entry.getKey()), normalizeAny(entry.getValue()));
            }
            return result;
        }
        if (value instanceof Collection) {
            List<Object> result = new ArrayList<Object>();
            for (Object item : (Collection<?>) value) {
                result.add(normalizeAny(item));
            }
            return result;
        }
        Class<?> type = value.getClass();
        if (type.isArray()) {
            int length = Array.getLength(value);
            List<Object> result = new ArrayList<Object>(length);
            for (int i = 0; i < length; i++) {
                result.add(normalizeAny(Array.get(value, i)));
            }
            return result;
        }
        return value;
    }

    private static String csvText(Object value, SqlType sqlType) {
        if (sqlType == SqlType.BYTES) {
            if (!(value instanceof byte[])) {
                throw new IllegalArgumentException(
                        "StarRocks BYTES Sink field must contain byte[] values");
            }
            return toHex((byte[]) value);
        }
        if (sqlType == SqlType.ROW) {
            throw new IllegalArgumentException(
                    "StarRocks ROW/STRUCT Sink values require explicit nested field metadata; "
                            + "Stage 2 intentionally does not infer STRUCT fields from FluxRow values");
        }
        if (value instanceof LocalDateTime) {
            return timestampText(value);
        }
        if (value instanceof LocalDate || value instanceof LocalTime) {
            return value.toString();
        }
        if (value instanceof Map || value instanceof Collection || value.getClass().isArray()) {
            try {
                return JSON_MAPPER.writeValueAsString(normalizeAny(value));
            } catch (JsonProcessingException failure) {
                throw new IllegalArgumentException("Failed to serialize complex CSV value", failure);
            }
        }
        return String.valueOf(value);
    }

    private static String toHex(byte[] value) {
        char[] chars = new char[value.length * 2];
        for (int i = 0; i < value.length; i++) {
            int unsigned = value[i] & 0xFF;
            chars[i * 2] = HEX[unsigned >>> 4];
            chars[i * 2 + 1] = HEX[unsigned & 0x0F];
        }
        return new String(chars);
    }

    private static String timestampText(Object value) {
        String text = String.valueOf(value);
        return value instanceof LocalDateTime ? text.replace('T', ' ') : text;
    }

    private void validateArity(FluxRow row) {
        if (row == null) {
            throw new IllegalArgumentException("row must not be null");
        }
        if (row.getArity() != schema.getColumnCount()) {
            throw new IllegalArgumentException(
                    "StarRocks Sink row arity does not match table schema: row="
                            + row.getArity()
                            + ", schema="
                            + schema.getColumnCount());
        }
    }

    private static void write(ByteArrayOutputStream output, byte[] bytes) {
        output.write(bytes, 0, bytes.length);
    }
}
