package com.link.up.connector.elasticsearch8.schema;

import co.elastic.clients.elasticsearch._types.mapping.TypeMapping;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.link.up.api.table.catalog.Column;
import com.link.up.api.table.catalog.TableSchema;
import com.link.up.api.table.type.BasicType;
import com.link.up.api.table.type.DecimalType;
import com.link.up.api.table.type.FluxDataType;
import jakarta.json.stream.JsonGenerator;

import java.io.IOException;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Restrained ES8 mapping conversion matching the Stage 1 product type boundary. */
public final class Elasticsearch8TypeMapper {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private Elasticsearch8TypeMapper() {
    }

    public static TableSchema toTableSchema(
            TypeMapping mapping,
            List<String> projectedFields) {
        if (mapping == null) {
            throw new IllegalArgumentException("Elasticsearch index mapping must not be null");
        }
        return toTableSchema(toRawMapping(mapping), projectedFields);
    }

    private static Map<String, Object> toRawMapping(TypeMapping mapping) {
        try {
            JacksonJsonpMapper jsonpMapper = new JacksonJsonpMapper(OBJECT_MAPPER);
            StringWriter json = new StringWriter();
            JsonGenerator generator = jsonpMapper.jsonProvider().createGenerator(json);
            try {
                mapping.serialize(generator, jsonpMapper);
            } finally {
                generator.close();
            }
            return OBJECT_MAPPER.readValue(
                    json.toString(),
                    new TypeReference<Map<String, Object>>() { });
        } catch (IOException | RuntimeException failure) {
            throw new IllegalArgumentException("Failed to read Elasticsearch 8 mapping JSON", failure);
        }
    }

    static TableSchema toTableSchema(
            Map<String, Object> mapping,
            List<String> projectedFields) {
        Map<String, Object> properties = mapValue(mapping == null ? null : mapping.get("properties"));
        if (properties.isEmpty()) {
            throw new IllegalArgumentException("Elasticsearch index mapping does not contain any properties");
        }

        List<String> fields = projectedFields == null || projectedFields.isEmpty()
                ? new ArrayList<String>(properties.keySet())
                : new ArrayList<String>(projectedFields);

        TableSchema.Builder builder = TableSchema.builder();
        for (String field : fields) {
            Map<String, Object> fieldMapping = findFieldMapping(properties, field);
            if (fieldMapping == null) {
                throw new IllegalArgumentException("Cannot find Elasticsearch mapping for source field: " + field);
            }
            String sourceType = stringValue(fieldMapping.get("type"));
            if (sourceType == null && fieldMapping.containsKey("properties")) {
                sourceType = "object";
            }
            if (sourceType == null) {
                sourceType = "unknown";
            }
            Column.Builder column = Column.builder(field, toFluxType(sourceType))
                    .nullable(true)
                    .sourceType(sourceType);
            String format = stringValue(fieldMapping.get("format"));
            if (format != null) {
                column.attribute("format", format);
            }
            builder.column(column.build());
        }
        return builder.build();
    }

    private static FluxDataType<?> toFluxType(String sourceType) {
        String type = sourceType == null ? "" : sourceType;
        switch (type) {
            case "boolean":
                return BasicType.BOOLEAN_TYPE;
            case "byte":
                return BasicType.BYTE_TYPE;
            case "short":
                return BasicType.SHORT_TYPE;
            case "integer":
            case "token_count":
                return BasicType.INT_TYPE;
            case "long":
                return BasicType.LONG_TYPE;
            case "unsigned_long":
                return new DecimalType(20, 0);
            case "half_float":
            case "float":
                return BasicType.FLOAT_TYPE;
            case "double":
            case "scaled_float":
            case "rank_feature":
                return BasicType.DOUBLE_TYPE;
            default:
                return BasicType.STRING_TYPE;
        }
    }

    private static Map<String, Object> findFieldMapping(
            Map<String, Object> rootProperties,
            String fieldPath) {
        String[] parts = fieldPath.split("\\.");
        Map<String, Object> properties = rootProperties;
        Map<String, Object> current = null;
        for (int index = 0; index < parts.length; index++) {
            current = mapValue(properties.get(parts[index]));
            if (current.isEmpty()) {
                return null;
            }
            if (index < parts.length - 1) {
                properties = mapValue(current.get("properties"));
                if (properties.isEmpty()) {
                    return null;
                }
            }
        }
        return current;
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> mapValue(Object value) {
        if (!(value instanceof Map)) {
            return Collections.emptyMap();
        }
        Map<String, Object> result = new LinkedHashMap<String, Object>();
        for (Map.Entry<?, ?> entry : ((Map<?, ?>) value).entrySet()) {
            if (entry.getKey() != null) {
                result.put(String.valueOf(entry.getKey()), entry.getValue());
            }
        }
        return result;
    }

    private static String stringValue(Object value) {
        if (value == null) {
            return null;
        }
        String text = String.valueOf(value).trim();
        return text.isEmpty() ? null : text;
    }
}
