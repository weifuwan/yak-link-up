package com.link.up.connector.starrocks.client.sink;

import com.link.up.api.configuration.ReadonlyConfig;
import com.link.up.api.table.catalog.Column;
import com.link.up.api.table.catalog.TableSchema;
import com.link.up.api.table.type.BasicType;
import com.link.up.connector.starrocks.config.StarRocksSinkConfig;
import org.junit.Test;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class StarRocksStreamLoadClientTest {

    @Test
    public void jsonHeadersOwnIdempotencyAndPayloadSemantics() {
        Map<String, Object> values = minimalConfig();
        Map<String, String> extra = new LinkedHashMap<String, String>();
        extra.put("strict_mode", "true");
        values.put("stream_load.params", extra);
        StarRocksSinkConfig config =
                StarRocksSinkConfig.of(ReadonlyConfig.fromMap(values));
        TableSchema schema = TableSchema.builder()
                .column(Column.builder("id", BasicType.LONG_TYPE).build())
                .build();

        StarRocksStreamLoadClient client = new StarRocksStreamLoadClient(config);
        try {
            Map<String, String> headers = client.buildHeaders("batch_1", schema);
            assertEquals("batch_1", headers.get("label"));
            assertEquals("json", headers.get("format"));
            assertEquals("true", headers.get("strip_outer_array"));
            assertEquals("true", headers.get("strict_mode"));
            assertEquals("100-continue", headers.get("Expect"));
            assertTrue(headers.get("Authorization").startsWith("Basic "));
        } finally {
            client.close();
        }
    }

    @Test
    public void csvHeadersCarryExactSchemaOrder() {
        Map<String, Object> values = minimalConfig();
        values.put("load_format", "CSV");
        values.put("column_separator", "|");
        StarRocksSinkConfig config =
                StarRocksSinkConfig.of(ReadonlyConfig.fromMap(values));
        TableSchema schema = TableSchema.builder()
                .column(Column.builder("id", BasicType.LONG_TYPE).build())
                .column(Column.builder("name", BasicType.STRING_TYPE).build())
                .build();

        StarRocksStreamLoadClient client = new StarRocksStreamLoadClient(config);
        try {
            Map<String, String> headers = client.buildHeaders("batch_2", schema);
            assertEquals("csv", headers.get("format"));
            assertEquals("|", headers.get("column_separator"));
            assertEquals("`id`,`name`", headers.get("columns"));
        } finally {
            client.close();
        }
    }

    private static Map<String, Object> minimalConfig() {
        Map<String, Object> values = new LinkedHashMap<String, Object>();
        values.put("node_urls", Arrays.asList("127.0.0.1:8030"));
        values.put("username", "root");
        values.put("password", "secret");
        values.put("database", "demo");
        values.put("table", "orders");
        return values;
    }
}
