package com.link.up.connector.file.converter;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.link.up.api.table.catalog.TableSchema;
import com.link.up.api.table.type.FluxRow;
import com.link.up.api.table.type.SqlType;

import java.util.List;
import java.util.Map;

/**
 * Converts one JSON Lines record to a FluxRow over the projected columns.
 *
 * <p>Scalar fields only; missing fields become null and extra fields are
 * ignored. Complex JSON values fail fast — MAP/ARRAY/ROW generation has no
 * Flux semantics in this stage.
 */
public final class JsonLineRowConverter implements FileRowConverter {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final TableSchema schema;
    private final List<Integer> outputIndexes;

    public JsonLineRowConverter(
            TableSchema schema,
            List<Integer> outputIndexes) {

        this.schema = schema;
        this.outputIndexes = outputIndexes;
    }

    @Override
    @SuppressWarnings("unchecked")
    public FluxRow convert(String line, String rowContext) {
        Map<String, Object> record;
        try {
            record = MAPPER.readValue(line, Map.class);
        } catch (Exception failure) {
            throw new IllegalArgumentException(
                    "Malformed JSON line " + rowContext + ": " + failure.getMessage(),
                    failure);
        }

        FluxRow row = new FluxRow(outputIndexes.size());
        for (int i = 0; i < outputIndexes.size(); i++) {
            int columnIndex = outputIndexes.get(i);
            String name = schema.getColumn(columnIndex).getName();
            Object value = record.get(name);
            row.setField(i, convertField(value, columnIndex, name, rowContext));
        }
        return row;
    }

    private Object convertField(
            Object value,
            int columnIndex,
            String name,
            String rowContext) {

        if (value == null) {
            return null;
        }
        if (value instanceof Map || value instanceof Iterable) {
            throw new IllegalArgumentException(
                    "Column '" + name + "' " + rowContext
                            + " carries a complex JSON value; nested structures are not supported in this stage");
        }

        String raw = value instanceof String ? (String) value : String.valueOf(value);
        return FileValueConverter.convert(
                schema.getColumn(columnIndex).getDataType().getSqlType(),
                schema.getColumn(columnIndex).getScale() == null
                        ? -1
                        : schema.getColumn(columnIndex).getScale(),
                raw);
    }
}
