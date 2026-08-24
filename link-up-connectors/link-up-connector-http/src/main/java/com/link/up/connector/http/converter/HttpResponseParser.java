package com.link.up.connector.http.converter;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;
import com.jayway.jsonpath.spi.json.JacksonJsonNodeJsonProvider;
import com.jayway.jsonpath.spi.mapper.JacksonMappingProvider;
import com.link.up.api.table.catalog.Column;
import com.link.up.api.table.catalog.TableSchema;
import com.link.up.api.table.type.SqlType;
import com.link.up.api.table.type.FluxRow;
import com.link.up.connector.http.config.HttpFormat;
import com.link.up.connector.http.config.HttpSourceConfig;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Converts HTTP response payloads into Link-Up rows and pagination values.
 *
 * <p>The class is kept under the connector {@code converter} role because it
 * translates external representations into Link-Up row/schema values; HTTP I/O
 * remains in the client/reader roles.</p>
 */
public final class HttpResponseParser {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static final Configuration JSON_PATH_CONFIG =
            Configuration.builder()
                    .jsonProvider(new JacksonJsonNodeJsonProvider())
                    .mappingProvider(new JacksonMappingProvider())
                    .options(Option.SUPPRESS_EXCEPTIONS)
                    .build();

    private HttpResponseParser() {
    }

    public static List<FluxRow> parseResponse(
            String responseBody,
            HttpSourceConfig config,
            TableSchema schema) throws Exception {

        if (config.getFormat() == HttpFormat.TEXT) {
            return parseTextResponse(responseBody, config);
        }

        if (!config.getJsonField().isEmpty()) {
            return parseByJsonField(responseBody, config, schema);
        }

        return parseByContentField(responseBody, config, schema);
    }

    private static List<FluxRow> parseTextResponse(
            String responseBody,
            HttpSourceConfig config) {

        if (responseBody == null || responseBody.isEmpty()) {
            return Collections.emptyList();
        }

        if (config.isEnableMultiLines()) {
            String[] lines = responseBody.split("\\r?\\n");
            List<FluxRow> rows = new ArrayList<>(lines.length);
            for (String line : lines) {
                String trimmed = line.trim();
                if (!trimmed.isEmpty()) {
                    rows.add(FluxRow.of(trimmed));
                }
            }
            return rows;
        }

        return Collections.singletonList(FluxRow.of(responseBody));
    }

    private static List<FluxRow> parseByContentField(
            String responseBody,
            HttpSourceConfig config,
            TableSchema schema) throws Exception {

        JsonNode root = MAPPER.readTree(responseBody);
        JsonNode dataNode;

        if (config.getContentField() != null && !config.getContentField().isEmpty()) {
            dataNode = evaluateJsonPath(root, config.getContentField());
        } else {
            dataNode = root;
        }

        if (dataNode == null || dataNode.isMissingNode()) {
            return Collections.emptyList();
        }

        if (dataNode.isArray()) {
            List<FluxRow> rows = new ArrayList<>(dataNode.size());
            for (JsonNode element : dataNode) {
                if (element.isObject()) {
                    rows.add(mapJsonNodeToRow(element, schema, config.isJsonFieldMissedReturnNull()));
                }
            }
            return rows;
        }

        if (dataNode.isObject()) {
            return Collections.singletonList(
                    mapJsonNodeToRow(dataNode, schema, config.isJsonFieldMissedReturnNull()));
        }

        return Collections.singletonList(FluxRow.of(jsonNodeToJavaValue(dataNode)));
    }

    private static List<FluxRow> parseByJsonField(
            String responseBody,
            HttpSourceConfig config,
            TableSchema schema) throws Exception {

        JsonNode root = MAPPER.readTree(responseBody);
        Map<String, Object> jsonFieldMapping = config.getJsonField();

        List<String> fieldNames = schema.getColumns().stream()
                .map(Column::getName)
                .collect(Collectors.toList());

        List<List<Object>> fieldValues = new ArrayList<>(fieldNames.size());
        int maxLen = 0;

        for (String fieldName : fieldNames) {
            String jsonPath = (String) jsonFieldMapping.get(fieldName);
            List<Object> values;

            if (jsonPath != null && !jsonPath.isEmpty()) {
                values = extractJsonPathValues(root, jsonPath);
            } else {
                values = extractDirectField(root, fieldName);
            }

            fieldValues.add(values);
            maxLen = Math.max(maxLen, values.size());
        }

        if (maxLen == 0) {
            return Collections.emptyList();
        }

        List<FluxRow> rows = new ArrayList<>(maxLen);
        for (int i = 0; i < maxLen; i++) {
            FluxRow row = new FluxRow(fieldNames.size());
            for (int f = 0; f < fieldNames.size(); f++) {
                List<Object> values = fieldValues.get(f);
                Object value = i < values.size() ? values.get(i) : null;
                Column column = schema.getColumn(f);
                row.setField(f, convertValue(value, column));
            }
            rows.add(row);
        }

        return rows;
    }

    public static String extractSingleStringValue(
            String responseBody,
            String jsonPath) throws Exception {

        if (responseBody == null || jsonPath == null) {
            return null;
        }

        JsonNode root = MAPPER.readTree(responseBody);
        String normalized = normalizeJsonPath(jsonPath);
        Object result = JsonPath.using(JSON_PATH_CONFIG).parse(root).read(normalized);

        if (result == null) {
            return null;
        }

        if (result instanceof JsonNode) {
            JsonNode node = (JsonNode) result;
            if (node.isNull() || node.isMissingNode()) {
                return null;
            }
            return node.isTextual() ? node.asText() : node.toString();
        }

        return String.valueOf(result);
    }

    private static JsonNode evaluateJsonPath(JsonNode root, String jsonPath) {
        String normalized = normalizeJsonPath(jsonPath);
        Object result = JsonPath.using(JSON_PATH_CONFIG).parse(root).read(normalized);
        if (result instanceof JsonNode) {
            return (JsonNode) result;
        }
        return null;
    }

    private static List<Object> extractJsonPathValues(JsonNode root, String jsonPath) {
        String normalized = normalizeJsonPath(jsonPath);
        Object result = JsonPath.using(JSON_PATH_CONFIG).parse(root).read(normalized);

        if (result == null) {
            return Collections.emptyList();
        }

        if (result instanceof ArrayNode) {
            ArrayNode array = (ArrayNode) result;
            List<Object> values = new ArrayList<>(array.size());
            for (JsonNode node : array) {
                values.add(jsonNodeToJavaValue(node));
            }
            return values;
        }

        if (result instanceof JsonNode) {
            JsonNode node = (JsonNode) result;
            if (node.isArray()) {
                List<Object> values = new ArrayList<>(node.size());
                for (JsonNode child : node) {
                    values.add(jsonNodeToJavaValue(child));
                }
                return values;
            }
            return Collections.singletonList(jsonNodeToJavaValue(node));
        }

        return Collections.singletonList(result);
    }

    private static List<Object> extractDirectField(JsonNode root, String fieldName) {
        if (root.isObject() && root.has(fieldName)) {
            JsonNode value = root.get(fieldName);
            if (value.isArray()) {
                List<Object> values = new ArrayList<>(value.size());
                for (JsonNode element : value) {
                    values.add(jsonNodeToJavaValue(element));
                }
                return values;
            }
            return Collections.singletonList(jsonNodeToJavaValue(value));
        }
        return Collections.emptyList();
    }

    private static String normalizeJsonPath(String path) {
        if (path == null) {
            return "$";
        }
        return path.replaceAll("\\.\\*", "[*]");
    }

    private static FluxRow mapJsonNodeToRow(
            JsonNode node,
            TableSchema schema,
            boolean missingReturnNull) {

        FluxRow row = new FluxRow(schema.getColumnCount());

        for (int i = 0; i < schema.getColumnCount(); i++) {
            Column column = schema.getColumn(i);
            JsonNode fieldNode = node.get(column.getName());

            if (fieldNode == null || fieldNode.isNull() || fieldNode.isMissingNode()) {
                if (!missingReturnNull && fieldNode == null) {
                    throw new IllegalArgumentException(
                            "JSON field '" + column.getName() + "' is missing. "
                                    + "Set json_field_missed_return_null=true to allow null values.");
                }
                row.setField(i, null);
            } else {
                row.setField(i, convertValue(jsonNodeToJavaValue(fieldNode), column));
            }
        }

        return row;
    }

    private static Object jsonNodeToJavaValue(JsonNode node) {
        if (node == null || node.isNull() || node.isMissingNode()) {
            return null;
        }
        if (node.isTextual()) return node.asText();
        if (node.isBoolean()) return node.asBoolean();
        if (node.isInt()) return node.asInt();
        if (node.isLong()) return node.asLong();
        if (node.isFloat() || node.isDouble()) return node.asDouble();
        if (node.isBigDecimal()) return node.decimalValue();
        if (node.isBinary()) {
            try {
                return node.binaryValue();
            } catch (Exception e) {
                return node.asText();
            }
        }
        return node.toString();
    }

    private static Object convertValue(Object value, Column column) {
        if (value == null) {
            return null;
        }

        SqlType sqlType = column.getDataType().getSqlType();

        switch (sqlType) {
            case STRING:
                return String.valueOf(value);
            case BOOLEAN:
                if (value instanceof Boolean) return value;
                return Boolean.parseBoolean(String.valueOf(value));
            case TINYINT:
                if (value instanceof Number) return ((Number) value).byteValue();
                return Byte.parseByte(String.valueOf(value));
            case SMALLINT:
                if (value instanceof Number) return ((Number) value).shortValue();
                return Short.parseShort(String.valueOf(value));
            case INT:
                if (value instanceof Number) return ((Number) value).intValue();
                return Integer.parseInt(String.valueOf(value));
            case BIGINT:
                if (value instanceof Number) return ((Number) value).longValue();
                return Long.parseLong(String.valueOf(value));
            case FLOAT:
                if (value instanceof Number) return ((Number) value).floatValue();
                return Float.parseFloat(String.valueOf(value));
            case DOUBLE:
                if (value instanceof Number) return ((Number) value).doubleValue();
                return Double.parseDouble(String.valueOf(value));
            case DECIMAL:
                if (value instanceof BigDecimal) return value;
                return new BigDecimal(String.valueOf(value));
            case DATE:
                if (value instanceof LocalDate) return value;
                return LocalDate.parse(String.valueOf(value));
            case TIME:
                if (value instanceof LocalTime) return value;
                return LocalTime.parse(String.valueOf(value));
            case TIMESTAMP:
                if (value instanceof LocalDateTime) return value;
                return LocalDateTime.parse(String.valueOf(value));
            case BYTES:
                if (value instanceof byte[]) return value;
                return String.valueOf(value).getBytes(java.nio.charset.StandardCharsets.UTF_8);
            default:
                return String.valueOf(value);
        }
    }
}
