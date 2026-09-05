package com.link.up.connector.starrocks.converter;

import com.link.up.api.table.type.FluxDataType;
import com.link.up.api.table.type.FluxRow;
import com.link.up.api.table.type.FluxRowType;
import com.link.up.api.table.type.SqlType;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.util.Text;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/** Decodes one StarRocks BE Arrow payload into Link-Up Flux rows. */
public final class StarRocksArrowRowReader {

    private StarRocksArrowRowReader() {
    }

    public static List<FluxRow> read(byte[] payload, FluxRowType rowType) throws IOException {
        if (payload == null || payload.length == 0) {
            return java.util.Collections.emptyList();
        }
        if (rowType == null) {
            throw new IllegalArgumentException("rowType must not be null");
        }

        List<FluxRow> rows = new ArrayList<FluxRow>();
        try (RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
             ArrowStreamReader reader =
                     new ArrowStreamReader(new ByteArrayInputStream(payload), allocator)) {

            VectorSchemaRoot root = reader.getVectorSchemaRoot();
            while (reader.loadNextBatch()) {
                Map<String, FieldVector> vectors = indexVectors(root.getFieldVectors());
                for (int rowIndex = 0; rowIndex < root.getRowCount(); rowIndex++) {
                    FluxRow row = new FluxRow(rowType.getFieldCount());
                    for (int fieldIndex = 0; fieldIndex < rowType.getFieldCount(); fieldIndex++) {
                        String fieldName = rowType.getFieldName(fieldIndex);
                        FieldVector vector = findVector(vectors, fieldName);
                        if (vector == null) {
                            throw new IOException(
                                    "Arrow payload does not contain projected field: " + fieldName);
                        }
                        Object raw = vector.isNull(rowIndex) ? null : vector.getObject(rowIndex);
                        row.setField(
                                fieldIndex,
                                convertValue(
                                        raw,
                                        rowType.getFieldType(fieldIndex),
                                        vector.getMinorType()));
                    }
                    rows.add(row);
                }
            }
        }
        return rows;
    }

    private static Map<String, FieldVector> indexVectors(List<FieldVector> vectors) {
        Map<String, FieldVector> result = new HashMap<String, FieldVector>();
        for (FieldVector vector : vectors) {
            String name = vector.getField().getName();
            result.put(name, vector);
            result.put(name.toLowerCase(Locale.ROOT), vector);
        }
        return result;
    }

    private static FieldVector findVector(Map<String, FieldVector> vectors, String fieldName) {
        FieldVector exact = vectors.get(fieldName);
        return exact != null ? exact : vectors.get(fieldName.toLowerCase(Locale.ROOT));
    }

    private static Object convertValue(
            Object value,
            FluxDataType<?> targetType,
            Types.MinorType minorType) {
        if (value == null) {
            return null;
        }

        SqlType sqlType = targetType.getSqlType();
        switch (sqlType) {
            case STRING:
                if (value instanceof Text) {
                    return value.toString();
                }
                if (value instanceof byte[]) {
                    return new String((byte[]) value, StandardCharsets.UTF_8);
                }
                return String.valueOf(value);
            case BOOLEAN:
                if (value instanceof Boolean) {
                    return value;
                }
                if (value instanceof Number) {
                    return ((Number) value).longValue() != 0L;
                }
                return Boolean.parseBoolean(value.toString());
            case TINYINT:
                return number(value).byteValue();
            case SMALLINT:
                return number(value).shortValue();
            case INT:
                return number(value).intValue();
            case BIGINT:
                return number(value).longValue();
            case FLOAT:
                return number(value).floatValue();
            case DOUBLE:
                return number(value).doubleValue();
            case DECIMAL:
                if (value instanceof BigDecimal) {
                    return value;
                }
                if (value instanceof Number) {
                    return new BigDecimal(value.toString());
                }
                return new BigDecimal(value instanceof Text ? value.toString() : String.valueOf(value));
            case DATE:
                return dateValue(value);
            case TIME:
                return timeValue(value);
            case TIMESTAMP:
                return timestampValue(value, minorType);
            case BYTES:
                if (value instanceof byte[]) {
                    return value;
                }
                if (value instanceof Text) {
                    return value.toString().getBytes(StandardCharsets.UTF_8);
                }
                return String.valueOf(value).getBytes(StandardCharsets.UTF_8);
            case NULL:
                return null;
            case ARRAY:
            case MAP:
            case ROW:
            case TIMESTAMP_TZ:
            default:
                throw new IllegalArgumentException(
                        "Unsupported StarRocks Arrow conversion target: " + sqlType);
        }
    }

    private static Number number(Object value) {
        if (value instanceof Number) {
            return (Number) value;
        }
        return new BigDecimal(value instanceof Text ? value.toString() : String.valueOf(value));
    }

    private static LocalDate dateValue(Object value) {
        if (value instanceof LocalDate) {
            return (LocalDate) value;
        }
        if (value instanceof LocalDateTime) {
            return ((LocalDateTime) value).toLocalDate();
        }
        if (value instanceof Number) {
            return LocalDate.ofEpochDay(((Number) value).longValue());
        }
        return LocalDate.parse(value.toString());
    }

    private static LocalTime timeValue(Object value) {
        if (value instanceof LocalTime) {
            return (LocalTime) value;
        }
        if (value instanceof LocalDateTime) {
            return ((LocalDateTime) value).toLocalTime();
        }
        if (value instanceof Integer) {
            return LocalTime.ofSecondOfDay(((Integer) value).longValue());
        }
        if (value instanceof Long) {
            return Instant.ofEpochMilli((Long) value)
                    .atZone(ZoneId.systemDefault())
                    .toLocalDateTime()
                    .toLocalTime();
        }
        return LocalTime.parse(value.toString());
    }

    private static LocalDateTime timestampValue(Object value, Types.MinorType minorType) {
        if (value instanceof LocalDateTime) {
            return (LocalDateTime) value;
        }
        if (value instanceof Number) {
            long raw = ((Number) value).longValue();
            String minorName = minorType == null ? "" : minorType.name();
            Instant instant;
            if (minorName.contains("NANO")) {
                instant = Instant.ofEpochSecond(
                        raw / 1_000_000_000L,
                        raw % 1_000_000_000L);
            } else if (minorName.contains("MICRO")) {
                instant = Instant.ofEpochSecond(
                        raw / 1_000_000L,
                        (raw % 1_000_000L) * 1_000L);
            } else if (minorName.contains("MILLI")) {
                instant = Instant.ofEpochMilli(raw);
            } else if (minorName.contains("SEC")) {
                instant = Instant.ofEpochSecond(raw);
            } else {
                instant = Instant.ofEpochMilli(raw);
            }
            return instant.atZone(ZoneId.systemDefault()).toLocalDateTime();
        }
        String text = value.toString();
        if (text.indexOf('T') >= 0) {
            return LocalDateTime.parse(text);
        }
        return LocalDateTime.parse(text.replace(' ', 'T'));
    }
}
