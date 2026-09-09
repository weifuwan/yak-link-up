package com.link.up.connector.elasticsearch7.config;

import com.link.up.api.configuration.ReadonlyConfig;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

public class Elasticsearch7SinkConfigTest {

    @Test
    public void parsesBoundedSinkDefaults() {
        Elasticsearch7SinkConfig config =
                Elasticsearch7SinkConfig.of(ReadonlyConfig.fromMap(config("orders")));
        assertEquals(1000, config.getBatchSize());
        assertEquals(3, config.getMaxRetries());
        assertEquals(200L, config.getRetryBackoffMs());
        assertEquals(5000L, config.getMaxRetryBackoffMs());
        assertNull(config.getDocumentIdField());
        assertEquals("http://localhost:9200", config.getHosts().get(0));
    }

    @Test
    public void rejectsWildcardTargetIndex() {
        expectFailure("orders-*", "wildcards");
    }

    @Test
    public void rejectsNegativeRetries() {
        Map<String, Object> values = config("orders");
        values.put("max_retries", -1);
        try {
            Elasticsearch7SinkConfig.of(ReadonlyConfig.fromMap(values));
            fail("Expected max_retries validation failure");
        } catch (IllegalArgumentException expected) {
            org.junit.Assert.assertTrue(expected.getMessage().contains("max_retries"));
        }
    }

    @Test
    public void trimsDocumentIdField() {
        Map<String, Object> values = config("orders");
        values.put("document_id_field", " order_id ");
        Elasticsearch7SinkConfig config = Elasticsearch7SinkConfig.of(ReadonlyConfig.fromMap(values));
        assertEquals("order_id", config.getDocumentIdField());
    }

    private static void expectFailure(String index, String message) {
        try {
            Elasticsearch7SinkConfig.of(ReadonlyConfig.fromMap(config(index)));
            fail("Expected validation failure");
        } catch (IllegalArgumentException expected) {
            org.junit.Assert.assertTrue(expected.getMessage().contains(message));
        }
    }

    private static Map<String, Object> config(String index) {
        Map<String, Object> values = new HashMap<String, Object>();
        values.put("hosts", Arrays.asList("http://localhost:9200/"));
        values.put("index", index);
        return values;
    }
}
