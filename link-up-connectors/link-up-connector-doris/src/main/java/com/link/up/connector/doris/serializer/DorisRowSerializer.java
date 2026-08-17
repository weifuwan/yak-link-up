package com.link.up.connector.doris.serializer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.link.up.api.table.catalog.Column;
import com.link.up.api.table.catalog.TableSchema;
import com.link.up.api.table.type.FluxRow;
import com.link.up.api.table.type.SqlType;
import com.link.up.connector.doris.config.DorisLoadFormat;
import com.link.up.connector.doris.config.DorisSinkConfig;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 将 FluxRow 序列化为 Doris Stream Load 可接受的格式。
 *
 * <p>支持 JSON（每行一个 JSON 对象）和 CSV 两种格式。
 */
public final class DorisRowSerializer {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final DorisSinkConfig config;
    private final TableSchema schema;
    private final String enclose;
    private final String escape;
    private final String lineDelimiter;

    public DorisRowSerializer(DorisSinkConfig config, TableSchema schema) {
        this.config = config;
        this.schema = schema;
        this.enclose = config.getEnclose();
        this.escape = config.getEscape();
        this.lineDelimiter = config.getLineDelimiter();
    }

    /**
     * 将一批 FluxRow 序列化为 Stream Load 数据字符串。
     */
    public String serialize(List<FluxRow> rows) {
        if (config.getLoadFormat() == DorisLoadFormat.CSV) {
            return serializeCsv(rows);
        }
        return serializeJson(rows);
    }

    /**
     * JSON 格式：每行一个 JSON 对象，以换行符分隔。
     */
    private String serializeJson(List<FluxRow> rows) {
        StringBuilder sb = new StringBuilder(rows.size() * 128);

        for (int i = 0; i < rows.size(); i++) {
            if (i > 0) {
                sb.append('\n');
            }
            FluxRow row = rows.get(i);
            Map<String, Object> map = rowToMap(row);
            try {
                sb.append(MAPPER.writeValueAsString(map));
            } catch (JsonProcessingException e) {
                throw new RuntimeException("Failed to serialize row to JSON", e);
            }
        }

        return sb.toString();
    }

    /**
     * CSV 格式：每行用分隔符分隔的字段值。
     *
     * <p>当配置了 enclose 时，字段值中包含分隔符、换行符或包围符本身时，
     * 会自动用包围符包裹字段，并用转义符处理内部的包围符字符。
     */
    private String serializeCsv(List<FluxRow> rows) {
        String separator = config.getCsvColumnSeparator();
        StringBuilder sb = new StringBuilder(rows.size() * 64);

        for (int i = 0; i < rows.size(); i++) {
            if (i > 0) {
                sb.append(lineDelimiter != null ? lineDelimiter : "\n");
            }
            FluxRow row = rows.get(i);
            for (int f = 0; f < schema.getColumnCount(); f++) {
                if (f > 0) {
                    sb.append(separator);
                }
                Object value = row.getField(f);
                sb.append(formatCsvValue(value));
            }
        }

        return sb.toString();
    }

    private Map<String, Object> rowToMap(FluxRow row) {
        Map<String, Object> map = new LinkedHashMap<>(schema.getColumnCount());

        for (int i = 0; i < schema.getColumnCount(); i++) {
            Column column = schema.getColumn(i);
            Object value = row.getField(i);
            map.put(column.getName(), convertForJson(value, column));
        }

        return map;
    }

    private Object convertForJson(Object value, Column column) {
        if (value == null) {
            return null;
        }

        SqlType sqlType = column.getDataType().getSqlType();
        switch (sqlType) {
            case BOOLEAN:
                if (value instanceof Boolean) return value;
                return Boolean.parseBoolean(String.valueOf(value));
            case TINYINT:
            case SMALLINT:
            case INT:
            case BIGINT:
                if (value instanceof Number) return value;
                return Long.parseLong(String.valueOf(value));
            case FLOAT:
            case DOUBLE:
                if (value instanceof Number) return value;
                return Double.parseDouble(String.valueOf(value));
            case DATE:
                if (value instanceof LocalDate) return value.toString();
                return String.valueOf(value);
            case TIME:
                if (value instanceof LocalTime) return value.toString();
                return String.valueOf(value);
            case TIMESTAMP:
                if (value instanceof LocalDateTime) return value.toString();
                return String.valueOf(value);
            case BYTES:
                if (value instanceof byte[]) {
                    return java.util.Base64.getEncoder().encodeToString((byte[]) value);
                }
                return String.valueOf(value);
            default:
                return String.valueOf(value);
        }
    }

    private String formatCsvValue(Object value) {
        if (value == null) {
            return "\\N";
        }

        String text;
        if (value instanceof byte[]) {
            text = java.util.Base64.getEncoder().encodeToString((byte[]) value);
        } else if (value instanceof LocalDate || value instanceof LocalTime || value instanceof LocalDateTime) {
            text = value.toString();
        } else {
            text = String.valueOf(value);
        }

        // 当配置了 enclose 时，检查字段值是否包含需要包围的字符
        if (enclose != null && !enclose.isEmpty()) {
            String separator = config.getCsvColumnSeparator();
            String ld = lineDelimiter != null ? lineDelimiter : "\n";
            boolean needsEnclose = text.contains(separator)
                    || text.contains(ld)
                    || text.contains(enclose);

            if (needsEnclose) {
                // 先用 escape 转义字段中出现的 enclose 字符
                if (escape != null && !escape.isEmpty()) {
                    text = text.replace(enclose, escape + enclose);
                }
                return enclose + text + enclose;
            }
        }

        return text;
    }
}
