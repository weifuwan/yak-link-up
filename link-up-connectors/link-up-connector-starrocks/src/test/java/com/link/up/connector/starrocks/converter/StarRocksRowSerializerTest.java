package com.link.up.connector.starrocks.converter;

import com.link.up.api.configuration.ReadonlyConfig;
import com.link.up.api.table.catalog.Column;
import com.link.up.api.table.catalog.TableSchema;
import com.link.up.api.table.type.BasicType;
import com.link.up.api.table.type.DecimalType;
import com.link.up.api.table.type.FluxRow;
import com.link.up.connector.starrocks.config.StarRocksSinkConfig;
import org.junit.Test;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class StarRocksRowSerializerTest {

    @Test
    public void serializesJsonArrayWithTemporalAndDecimalValues() {
        StarRocksSinkConfig config = config("JSON", "\t");
        TableSchema schema = TableSchema.builder()
                .column(Column.builder("id", BasicType.LONG_TYPE).build())
                .column(Column.builder("amount", new DecimalType(18, 2)).build())
                .column(Column.builder("created_on", BasicType.DATE_TYPE).build())
                .column(Column.builder("created_at", BasicType.TIMESTAMP_TYPE).build())
                .column(Column.builder("note", BasicType.STRING_TYPE).build())
                .build();
        StarRocksRowSerializer serializer = new StarRocksRowSerializer(config, schema);

        byte[] first = serializer.serializeRow(
                FluxRow.of(
                        1L,
                        new BigDecimal("12.30"),
                        LocalDate.of(2026, 9, 5),
                        LocalDateTime.of(2026, 9, 5, 12, 34, 56),
                        null));
        byte[] second = serializer.serializeRow(
                FluxRow.of(
                        2L,
                        new BigDecimal("9.99"),
                        LocalDate.of(2026, 9, 6),
                        LocalDateTime.of(2026, 9, 6, 8, 0, 1),
                        "ok"));

        List<byte[]> rows = new ArrayList<byte[]>();
        rows.add(first);
        rows.add(second);
        byte[] payload = serializer.joinRecords(rows, first.length + second.length);
        String json = new String(payload, StandardCharsets.UTF_8);

        assertTrue(json.startsWith("["));
        assertTrue(json.endsWith("]"));
        assertTrue(json.contains("\"id\":1"));
        assertTrue(json.contains("\"amount\":12.30"));
        assertTrue(json.contains("\"created_on\":\"2026-09-05\""));
        assertTrue(json.contains("\"created_at\":\"2026-09-05 12:34:56\""));
        assertTrue(json.contains("\"note\":null"));
    }

    @Test
    public void serializesCsvUsingConfiguredDelimiterAndNullMarker() {
        StarRocksSinkConfig config = config("CSV", "|");
        TableSchema schema = TableSchema.builder()
                .column(Column.builder("id", BasicType.LONG_TYPE).build())
                .column(Column.builder("name", BasicType.STRING_TYPE).build())
                .column(Column.builder("note", BasicType.STRING_TYPE).build())
                .build();
        StarRocksRowSerializer serializer = new StarRocksRowSerializer(config, schema);

        String record = new String(
                serializer.serializeRow(FluxRow.of(7L, "Alice", null)),
                StandardCharsets.UTF_8);

        assertEquals("7|Alice|\\N", record);
        assertEquals("`id`,`name`,`note`", serializer.csvColumnsHeader());
    }

    @Test
    public void serializesBinaryAsHexForCsvStreamLoad() {
        StarRocksSinkConfig config = config("CSV", "|");
        TableSchema schema = TableSchema.builder()
                .column(Column.builder("payload", BasicType.BYTES_TYPE).build())
                .build();
        StarRocksRowSerializer serializer = new StarRocksRowSerializer(config, schema);

        String record = new String(
                serializer.serializeRow(FluxRow.of(new byte[] {0x00, 0x0F, (byte) 0xFF})),
                StandardCharsets.UTF_8);

        assertEquals("000FFF", record);
    }

    @Test(expected = IllegalArgumentException.class)
    public void jsonRejectsBinaryBecauseStarRocksJsonLoadDoesNotSupportIt() {
        StarRocksSinkConfig config = config("JSON", "\t");
        TableSchema schema = TableSchema.builder()
                .column(Column.builder("payload", BasicType.BYTES_TYPE).build())
                .build();

        new StarRocksRowSerializer(config, schema)
                .serializeRow(FluxRow.of(new byte[] {0x01, 0x02}));
    }

    @Test(expected = IllegalArgumentException.class)
    public void csvFailsFastWhenValueContainsDelimiter() {
        StarRocksSinkConfig config = config("CSV", "|");
        TableSchema schema = TableSchema.builder()
                .column(Column.builder("name", BasicType.STRING_TYPE).build())
                .build();
        new StarRocksRowSerializer(config, schema)
                .serializeRow(FluxRow.of("A|B"));
    }

    private static StarRocksSinkConfig config(String format, String separator) {
        Map<String, Object> values = new LinkedHashMap<String, Object>();
        values.put("node_urls", Arrays.asList("127.0.0.1:8030"));
        values.put("username", "root");
        values.put("database", "demo");
        values.put("table", "orders");
        values.put("load_format", format);
        values.put("column_separator", separator);
        return StarRocksSinkConfig.of(ReadonlyConfig.fromMap(values));
    }
}
