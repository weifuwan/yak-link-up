package com.link.up.connector.starrocks.config;

import com.link.up.api.configuration.ReadonlyConfig;
import com.link.up.api.table.type.SqlType;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class StarRocksSourceConfigTest {

    @Test
    public void parsesSingleTableNativeSource() {
        Map<String, Object> root = baseConfig();
        root.put("table", "orders");
        root.put("scan_filter", "id > 100");
        root.put("schema", Collections.<String, Object>singletonMap(
                "fields", fields("id", "BIGINT", "name", "VARCHAR(64)")));

        StarRocksSourceConfig config =
                StarRocksSourceConfig.of(ReadonlyConfig.fromMap(root));

        assertEquals(Arrays.asList("fe-1:8030", "fe-2:8030"), config.getNodeUrls());
        assertEquals("demo", config.getDatabase());
        assertEquals(1, config.getTableConfigs().size());
        StarRocksSourceTableConfig table = config.getTableConfigs().get(0);
        assertEquals("orders", table.getTable());
        assertEquals("id > 100", table.getScanFilter());
        assertEquals(SqlType.BIGINT,
                table.getCatalogTable().getTableSchema().getColumn(0).getDataType().getSqlType());
        assertEquals(2, config.getCatalogTables().get(0).getTableSchema().getColumns().size());
    }

    @Test
    public void parsesMultiTableSchemaAndPerTableFilter() {
        Map<String, Object> root = baseConfig();
        root.put("scan_filter", "tenant_id = 7");

        Map<String, Object> orders = new LinkedHashMap<String, Object>();
        orders.put("table", "orders");
        orders.put("scan_filter", "status = 'PAID'");
        orders.put("schema", Collections.<String, Object>singletonMap(
                "fields", fields("id", "BIGINT", "amount", "DECIMAL(18,2)")));

        Map<String, Object> customers = new LinkedHashMap<String, Object>();
        customers.put("table", "customers");
        customers.put("schema", Collections.<String, Object>singletonMap(
                "fields", fields("id", "BIGINT", "name", "STRING")));

        root.put("table_list", Arrays.<Map<String, Object>>asList(orders, customers));

        StarRocksSourceConfig config =
                StarRocksSourceConfig.of(ReadonlyConfig.fromMap(root));

        assertEquals(2, config.getTableConfigs().size());
        assertEquals("status = 'PAID'", config.getTableConfigs().get(0).getScanFilter());
        assertEquals("tenant_id = 7", config.getTableConfigs().get(1).getScanFilter());
    }

    @Test
    public void rejectsTableAndTableListTogether() {
        Map<String, Object> root = baseConfig();
        root.put("table", "orders");
        root.put("schema", Collections.<String, Object>singletonMap(
                "fields", fields("id", "BIGINT")));

        Map<String, Object> item = new LinkedHashMap<String, Object>();
        item.put("table", "customers");
        item.put("schema", Collections.<String, Object>singletonMap(
                "fields", fields("id", "BIGINT")));
        root.put("table_list", Collections.<Map<String, Object>>singletonList(item));

        boolean failed = false;
        try {
            StarRocksSourceConfig.of(ReadonlyConfig.fromMap(root));
        } catch (IllegalArgumentException expected) {
            failed = true;
            assertTrue(expected.getMessage().contains("Exactly one"));
        }
        assertTrue(failed);
    }

    private static Map<String, Object> baseConfig() {
        Map<String, Object> root = new LinkedHashMap<String, Object>();
        root.put("node_urls", Arrays.asList("fe-1:8030", "fe-2:8030"));
        root.put("username", "root");
        root.put("password", "secret");
        root.put("database", "demo");
        return root;
    }

    private static Map<String, Object> fields(String... values) {
        if (values.length % 2 != 0) {
            throw new IllegalArgumentException("fields must contain name/type pairs");
        }
        Map<String, Object> result = new LinkedHashMap<String, Object>();
        for (int i = 0; i < values.length; i += 2) {
            result.put(values[i], values[i + 1]);
        }
        return result;
    }
}
