package com.link.up.connector.starrocks.client.source;

import com.link.up.connector.starrocks.config.StarRocksSourceTableConfig;
import com.link.up.connector.starrocks.schema.StarRocksSchemaParser;
import org.junit.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class StarRocksQueryPlanClientTest {

    @Test
    public void buildsProjectedNativeQueryWithFilter() {
        Map<String, Object> fields = new LinkedHashMap<String, Object>();
        fields.put("id", "BIGINT");
        fields.put("order`name", "STRING");

        StarRocksSourceTableConfig table =
                new StarRocksSourceTableConfig(
                        "analytics",
                        "orders",
                        "id >= 100",
                        StarRocksSchemaParser.parse(fields));

        assertEquals(
                "SELECT `id`, `order``name` FROM `analytics`.`orders` WHERE id >= 100",
                StarRocksQueryPlanClient.buildQuerySql(table));
    }
}
