package com.link.up.connector.file.converter;

import com.link.up.api.table.catalog.Column;
import com.link.up.api.table.catalog.TableSchema;
import com.link.up.api.table.type.BasicType;
import com.link.up.api.table.type.DecimalType;
import com.link.up.api.table.type.FluxRow;
import org.apache.commons.csv.CSVFormat;
import org.junit.Test;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class DelimitedRowConverterTest {

    @Test
    public void shouldConvertDeclaredTypes() {
        DelimitedRowConverter converter = converter(emptyList(), null);

        FluxRow row = convert(converter, "1,3.14,widget,true", "test:1");

        assertEquals(1L, row.getField(0));
        assertEquals(new BigDecimal("3.14"), row.getField(1));
        assertEquals("widget", row.getField(2));
        assertEquals(Boolean.TRUE, row.getField(3));
    }

    @Test
    public void shouldFailFastOnUnsafeNumericText() {
        DelimitedRowConverter converter = converter(emptyList(), null);

        try {
            convert(converter, "x,3.14,widget,true", "in rows.csv at split offset 0, split-local line 3");
            fail("Expected a non-numeric id to fail fast");
        } catch (IllegalArgumentException failure) {
            assertTrue(failure.getMessage().contains("Column 'id'"));
            assertTrue(failure.getMessage().contains("split-local line 3"));
        }
    }

    @Test
    public void shouldRejectWrongFieldCount() {
        DelimitedRowConverter converter = converter(emptyList(), null);

        try {
            convert(converter, "1,3.14,widget", "test:1");
            fail("Expected a short row to be rejected");
        } catch (IllegalArgumentException failure) {
            assertTrue(failure.getMessage().contains("fields"));
        }
    }

    @Test
    public void shouldMapNullValueTextToNull() {
        DelimitedRowConverter converter = converter(emptyList(), "\\N");

        FluxRow row = convert(converter, "1,\\N,widget,\\N", "test:1");

        assertEquals(1L, row.getField(0));
        assertEquals(null, row.getField(1));
    }

    @Test
    public void shouldProjectSelectedFields() {
        DelimitedRowConverter converter = converter(Arrays.asList("price", "name"), null);

        FluxRow row = convert(converter, "1,3.14,widget,true", "test:1");

        assertEquals(2, row.getArity());
        assertEquals(new BigDecimal("3.14"), row.getField(0));
        assertEquals("widget", row.getField(1));
    }

    @Test
    public void shouldKeepQuotedCommaInsideCsvField() {
        DelimitedRowConverter converter = converter(emptyList(), null);

        FluxRow row = convert(converter, "1,3.14,\"widget, inc\",true", "test:1");

        assertEquals("widget, inc", row.getField(2));
    }

    @Test
    public void shouldKeepQuotedNewlineInsideRecord() {
        DelimitedRowConverter converter = converter(emptyList(), null);

        FluxRow row = convert(converter, "1,3.14,\"multi\nline\",true", "test:1");

        assertEquals("multi\nline", row.getField(2));
    }

    private FluxRow convert(DelimitedRowConverter converter, String line, String rowContext) {
        CSVFormat format = DelimitedRowConverter.csvFormat('"', '"');
        return converter.convert(DelimitedRowConverter.parseSingleRecord(format, line), rowContext);
    }

    private static List<String> emptyList() {
        return new ArrayList<String>();
    }

    private DelimitedRowConverter converter(List<String> fields, String nullValue) {
        TableSchema schema = TableSchema.builder()
                .column(Column.builder("id", BasicType.LONG_TYPE).nullable(true).build())
                .column(Column.builder("price", new DecimalType(10, 2)).nullable(true).build())
                .column(Column.builder("name", BasicType.STRING_TYPE).nullable(true).build())
                .column(Column.builder("flag", BasicType.BOOLEAN_TYPE).nullable(true).build())
                .build();
        List<String> projected = fields == null || fields.isEmpty()
                ? Arrays.asList("id", "price", "name", "flag")
                : fields;
        return DelimitedRowConverter.csv(
                schema,
                DelimitedRowConverter.outputIndexes(schema, projected),
                nullValue,
                '"',
                '"');
    }
}
