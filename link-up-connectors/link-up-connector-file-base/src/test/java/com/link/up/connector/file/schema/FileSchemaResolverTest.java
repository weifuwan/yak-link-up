package com.link.up.connector.file.schema;

import com.link.up.api.table.catalog.Column;
import com.link.up.api.table.catalog.TableSchema;
import com.link.up.api.table.type.BasicType;
import com.link.up.api.table.type.SqlType;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class FileSchemaResolverTest {

    @Test
    public void shouldBuildDeclaredSchemaWithDecimalScale() {
        TableSchema schema = FileSchemaResolver.fromDeclared(Arrays.asList(
                new com.link.up.connector.file.config.FileColumnSpec(
                        "id", SqlType.BIGINT, BasicType.LONG_TYPE, 0, 0),
                new com.link.up.connector.file.config.FileColumnSpec(
                        "price", SqlType.DECIMAL, new com.link.up.api.table.type.DecimalType(10, 2), 10, 2)));

        assertEquals(SqlType.BIGINT, schema.getColumn("id").getDataType().getSqlType());
        assertEquals(Integer.valueOf(2), schema.getColumn("price").getScale());
    }

    @Test
    public void shouldMapHeaderRowToStringColumns() {
        TableSchema schema = FileSchemaResolver.fromHeader(Arrays.asList("id", " name "));

        assertEquals(2, schema.getColumnCount());
        assertEquals("name", schema.getColumn(1).getName());
        assertEquals(SqlType.STRING, schema.getColumn("id").getDataType().getSqlType());
    }

    @Test
    public void shouldRejectDuplicateHeaderNames() {
        try {
            FileSchemaResolver.fromHeader(Arrays.asList("id", "id"));
            fail("Expected duplicate header names to be rejected");
        } catch (IllegalArgumentException failure) {
            assertTrue(failure.getMessage().contains("Duplicate"));
        }
    }

    @Test
    public void shouldRejectBlankHeaderNames() {
        try {
            FileSchemaResolver.fromHeader(Arrays.asList("id", "  "));
            fail("Expected blank header names to be rejected");
        } catch (IllegalArgumentException failure) {
            assertTrue(failure.getMessage().contains("blank"));
        }
    }

    @Test
    public void shouldProjectInDeclaredOrder() {
        TableSchema schema = TableSchema.builder()
                .column(Column.builder("a", BasicType.STRING_TYPE).build())
                .column(Column.builder("b", BasicType.STRING_TYPE).build())
                .column(Column.builder("c", BasicType.STRING_TYPE).build())
                .build();

        assertEquals(
                Arrays.asList("c", "a"),
                FileSchemaResolver.projectedNames(schema, Arrays.asList("c", "a")));
    }
}
