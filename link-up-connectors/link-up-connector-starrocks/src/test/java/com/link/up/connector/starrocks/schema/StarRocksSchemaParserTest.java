package com.link.up.connector.starrocks.schema;

import com.link.up.api.table.catalog.Column;
import com.link.up.api.table.catalog.TableSchema;
import com.link.up.api.table.type.DecimalType;
import com.link.up.api.table.type.SqlType;
import org.junit.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class StarRocksSchemaParserTest {

    @Test
    public void mapsCoreStarRocksScalarTypes() {
        Map<String, Object> fields = new LinkedHashMap<String, Object>();
        fields.put("flag", "BOOLEAN");
        fields.put("id", "BIGINT");
        fields.put("huge", "LARGEINT");
        fields.put("amount", "DECIMAL(20, 4)");
        fields.put("name", "VARCHAR(128)");
        fields.put("payload", "JSON");
        fields.put("created_on", "DATE");
        fields.put("created_at", "DATETIME");

        TableSchema schema = StarRocksSchemaParser.parse(fields);

        assertEquals(8, schema.getColumns().size());
        assertEquals(SqlType.BOOLEAN, schema.getColumn(0).getDataType().getSqlType());
        assertEquals(SqlType.BIGINT, schema.getColumn(1).getDataType().getSqlType());
        assertEquals(SqlType.STRING, schema.getColumn(2).getDataType().getSqlType());
        assertEquals("LARGEINT", schema.getColumn(2).getSourceType());
        assertEquals(new DecimalType(20, 4), schema.getColumn(3).getDataType());
        assertEquals(SqlType.STRING, schema.getColumn(4).getDataType().getSqlType());
        assertEquals(SqlType.STRING, schema.getColumn(5).getDataType().getSqlType());
        assertEquals(SqlType.DATE, schema.getColumn(6).getDataType().getSqlType());
        assertEquals(SqlType.TIMESTAMP, schema.getColumn(7).getDataType().getSqlType());

        Column amount = schema.getColumn(3);
        assertEquals("DECIMAL(20, 4)", amount.getSourceType());
    }

    @Test
    public void rejectsComplexTypesDuringStageOne() {
        boolean failed = false;
        try {
            StarRocksSchemaParser.resolveType("ARRAY<INT>");
        } catch (IllegalArgumentException expected) {
            failed = true;
            assertTrue(expected.getMessage().contains("ARRAY/MAP"));
        }
        assertTrue(failed);
    }

    @Test(expected = IllegalArgumentException.class)
    public void rejectsDecimalBeyondStarRocksPrecision() {
        StarRocksSchemaParser.resolveType("DECIMAL(39, 2)");
    }
}
