package com.link.up.connector.starrocks.config;

import com.link.up.api.configuration.ReadonlyConfig;
import org.junit.Test;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class StarRocksSinkConfigTest {

    @Test
    public void parsesMinimalBoundedSinkConfig() {
        StarRocksSinkConfig config = StarRocksSinkConfig.of(
                ReadonlyConfig.fromMap(minimalConfig()));

        assertEquals(Arrays.asList("fe-1:8030", "fe-2:8030"), config.getNodeUrls());
        assertEquals("root", config.getUsername());
        assertEquals("demo", config.getDatabase());
        assertEquals("orders", config.getTable());
        assertEquals(StarRocksLoadFormat.JSON, config.getLoadFormat());
        assertEquals(1024, config.getBatchMaxRows());
        assertEquals(5L * 1024L * 1024L, config.getBatchMaxBytes());
        assertTrue(config.getLabelPrefix().startsWith("link_up_demo_orders_"));
        assertTrue(config.getLabelPrefix().length() <= 96);
    }

    @Test
    public void parsesCsvAndSafeStreamLoadParams() {
        Map<String, Object> values = minimalConfig();
        values.put("load_format", "CSV");
        values.put("column_separator", "|");
        Map<String, String> params = new LinkedHashMap<String, String>();
        params.put("strict_mode", "true");
        params.put("timeout", "300");
        values.put("stream_load.params", params);

        StarRocksSinkConfig config =
                StarRocksSinkConfig.of(ReadonlyConfig.fromMap(values));

        assertEquals(StarRocksLoadFormat.CSV, config.getLoadFormat());
        assertEquals("|", config.getColumnSeparator());
        assertEquals("true", config.getStreamLoadParams().get("strict_mode"));
    }

    @Test
    public void longDefaultLabelPrefixIsTrimmedForGeneratedUuid() {
        Map<String, Object> values = minimalConfig();
        values.put("database", repeat('d', 100));
        values.put("table", repeat('t', 100));

        StarRocksSinkConfig config =
                StarRocksSinkConfig.of(ReadonlyConfig.fromMap(values));

        assertTrue(config.getLabelPrefix().startsWith("link_up_"));
        assertEquals(96, config.getLabelPrefix().length());
    }

    @Test(expected = IllegalArgumentException.class)
    public void rejectsReservedStreamLoadHeaderOverride() {
        Map<String, Object> values = minimalConfig();
        Map<String, String> params = new LinkedHashMap<String, String>();
        params.put("label", "unsafe");
        values.put("stream_load.params", params);
        StarRocksSinkConfig.of(ReadonlyConfig.fromMap(values));
    }

    @Test(expected = IllegalArgumentException.class)
    public void rejectsMergeCommitBecauseItOwnsLabels() {
        Map<String, Object> values = minimalConfig();
        Map<String, String> params = new LinkedHashMap<String, String>();
        params.put("enable_merge_commit", "true");
        values.put("stream_load.params", params);
        StarRocksSinkConfig.of(ReadonlyConfig.fromMap(values));
    }

    @Test(expected = IllegalArgumentException.class)
    public void rejectsPartialUpdateSemanticsInBoundedStage2() {
        Map<String, Object> values = minimalConfig();
        Map<String, String> params = new LinkedHashMap<String, String>();
        params.put("partial_update", "true");
        values.put("stream_load.params", params);
        StarRocksSinkConfig.of(ReadonlyConfig.fromMap(values));
    }

    @Test(expected = IllegalArgumentException.class)
    public void rejectsCompressionHeaderWithoutCompressedPayload() {
        Map<String, Object> values = minimalConfig();
        Map<String, String> params = new LinkedHashMap<String, String>();
        params.put("compression", "gzip");
        values.put("stream_load.params", params);
        StarRocksSinkConfig.of(ReadonlyConfig.fromMap(values));
    }

    @Test(expected = IllegalArgumentException.class)
    public void rejectsLabelPrefixWithHyphen() {
        Map<String, Object> values = minimalConfig();
        values.put("label_prefix", "batch-run");
        StarRocksSinkConfig.of(ReadonlyConfig.fromMap(values));
    }

    @Test(expected = IllegalArgumentException.class)
    public void rejectsLabelPrefixStartingWithDigit() {
        Map<String, Object> values = minimalConfig();
        values.put("label_prefix", "1batch");
        StarRocksSinkConfig.of(ReadonlyConfig.fromMap(values));
    }

    @Test(expected = IllegalArgumentException.class)
    public void rejectsLabelPrefixThatLeavesNoRoomForUuid() {
        Map<String, Object> values = minimalConfig();
        values.put("label_prefix", repeat('a', 97));
        StarRocksSinkConfig.of(ReadonlyConfig.fromMap(values));
    }

    @Test(expected = IllegalArgumentException.class)
    public void rejectsDelimiterBeyondStarRocksLimit() {
        Map<String, Object> values = minimalConfig();
        values.put("column_separator", repeat('x', 51));
        StarRocksSinkConfig.of(ReadonlyConfig.fromMap(values));
    }

    @Test(expected = IllegalArgumentException.class)
    public void rejectsNonPositiveBatchSize() {
        Map<String, Object> values = minimalConfig();
        values.put("batch_max_rows", 0);
        StarRocksSinkConfig.of(ReadonlyConfig.fromMap(values));
    }

    private static Map<String, Object> minimalConfig() {
        Map<String, Object> values = new LinkedHashMap<String, Object>();
        values.put("node_urls", Arrays.asList("fe-1:8030", "fe-2:8030"));
        values.put("username", "root");
        values.put("password", "");
        values.put("database", "demo");
        values.put("table", "orders");
        return values;
    }

    private static String repeat(char value, int count) {
        char[] chars = new char[count];
        Arrays.fill(chars, value);
        return new String(chars);
    }
}
