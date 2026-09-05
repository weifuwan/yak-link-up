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
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Serializes Flux rows into deterministic StarRocks Stream Load payloads. */
public final class StarRocksRowSerializer {

    private static final ObjectMapper JSON_MAPPER = new ObjectMapper();
    private static final char[] HEX = "0123456789ABCDEF".toCharArray();
    private static final String CSV_NULL_MARKER = "\\N";

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
        validateSchema();
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
                builder.append(CSV_NULL_MARKER);
                continue;
            }
            SqlType sqlType = schema.getColumn(i).getDataType().getSqlType();
            String text = csvText(value, sqlType);
            if (CSV_NULL_MARKER.equals(text)) {
                throw new IllegalArgumentException(
                        "Non-null CSV value equals StarRocks null marker \\N; "
                                + "use JSON load_format or normalize the value upstream");
            }
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
                throw unsupportedBinaryJson();
            case TIMESTAMP_TZ:
                throw unsupportedTimezoneTimestamp();
            case ARRAY:
            case MAP:
            case ROW:
                throw unsupportedComplexType(sqlType);
            case STRING:
                return String.valueOf(value);
            case NULL:
                return null;
            default:
                return String.valueOf(value);
        }
    }

    private static String csvText(Object value, SqlType sqlType) {
        if (sqlType == SqlType.BYTES) {
            if (!(value instanceof byte[])) {
                throw new IllegalArgumentException(
                        "StarRocks BYTES Sink field must contain byte[] values");
            }
            return toHex((byte[]) value);
        }
        if (sqlType == SqlType.TIMESTAMP_TZ) {
            throw unsupportedTimezoneTimestamp();
        }
        if (sqlType == SqlType.ARRAY || sqlType == SqlType.MAP || sqlType == SqlType.ROW) {
            throw unsupportedComplexType(sqlType);
        }
        if (value instanceof LocalDateTime) {
            return timestampText(value);
        }
        if (value instanceof LocalDate || value instanceof LocalTime) {
            return value.toString();
        }
        return String.valueOf(value);
    }

    private void validateSchema() {
        for (Column column : schema.getColumns()) {
            SqlType sqlType = column.getDataType().getSqlType();
            if (sqlType == SqlType.BYTES && config.getLoadFormat() == StarRocksLoadFormat.JSON) {
                throw new IllegalArgumentException(
                        "StarRocks Sink column '"
                                + column.getName()
                                + "' is BYTES: BINARY/VARBINARY Stream Load requires load_format=CSV");
            }
            if (sqlType == SqlType.TIMESTAMP_TZ) {
                throw new IllegalArgumentException(
                        "StarRocks Sink column '"
                                + column.getName()
                                + "' is TIMESTAMP_TZ: Stage 2 does not implicitly discard timezone offsets; "
                                + "convert it explicitly upstream before loading into DATETIME");
            }
            if (sqlType == SqlType.ARRAY || sqlType == SqlType.MAP || sqlType == SqlType.ROW) {
                throw new IllegalArgumentException(
                        "StarRocks Sink column '"
                                + column.getName()
                                + "' uses unsupported Stage 2 complex type "
                                + sqlType
                                + "; map it to a validated scalar representation explicitly");
            }
        }
    }

    private static IllegalArgumentException unsupportedBinaryJson() {
        return new IllegalArgumentException(
                "StarRocks BINARY/VARBINARY Stream Load is not supported with JSON; "
                        + "use load_format=CSV so bytes can be encoded as hexadecimal text");
    }

    private static IllegalArgumentException unsupportedTimezoneTimestamp() {
        return new IllegalArgumentException(
                "StarRocks Stage 2 does not implicitly discard TIMESTAMP_TZ offsets; "
                        + "convert the value explicitly upstream before loading into DATETIME");
    }

    private static IllegalArgumentException unsupportedComplexType(SqlType sqlType) {
        return new IllegalArgumentException(
                "StarRocks Stage 2 does not support complex Sink type "
                        + sqlType
                        + " yet; map it to a validated scalar representation explicitly");
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
