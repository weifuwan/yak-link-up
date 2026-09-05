package com.link.up.connector.starrocks.sink;

import com.link.up.api.configuration.ReadonlyConfig;
import com.link.up.api.sink.PreparedSinkMetadata;
import com.link.up.api.sink.SinkPrepareContext;
import com.link.up.api.table.catalog.CatalogTable;
import com.link.up.api.table.catalog.Column;
import com.link.up.api.table.catalog.TablePath;
import com.link.up.api.table.catalog.TableSchema;
import com.link.up.api.table.type.BasicType;
import com.link.up.connector.starrocks.config.StarRocksSinkConfig;
import org.junit.Test;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class StarRocksSinkPreparerTest {

    @Test
    public void mapsSingleSourceSchemaToConfiguredTarget() throws Exception {
        Map<String, Object> configValues = new LinkedHashMap<String, Object>();
        configValues.put("node_urls", Arrays.asList("127.0.0.1:8030"));
        configValues.put("username", "root");
        configValues.put("database", "warehouse");
        configValues.put("table", "orders_sink");
        StarRocksSinkConfig config =
                StarRocksSinkConfig.of(ReadonlyConfig.fromMap(configValues));

        TablePath sourcePath = TablePath.of("source_db", "orders");
        TableSchema schema = TableSchema.builder()
                .column(Column.builder("id", BasicType.LONG_TYPE).build())
                .column(Column.builder("name", BasicType.STRING_TYPE).build())
                .build();
        CatalogTable sourceTable = CatalogTable.builder(sourcePath, schema).build();
        Map<TablePath, CatalogTable> sourceTables =
                new LinkedHashMap<TablePath, CatalogTable>();
        sourceTables.put(sourcePath, sourceTable);

        PreparedSinkMetadata metadata =
                new StarRocksSinkPreparer(config)
                        .prepare(
                                new SinkPrepareContext(
                                        ReadonlyConfig.fromMap(configValues),
                                        sourceTables));

        CatalogTable target = metadata.getTargetTable(sourcePath);
        assertEquals(TablePath.of("warehouse", "orders_sink"), target.getTablePath());
        assertEquals(schema, target.getTableSchema());
    }

    @Test(expected = IllegalArgumentException.class)
    public void rejectsMultiSourceTopologyInStageTwo() throws Exception {
        Map<String, Object> configValues = new LinkedHashMap<String, Object>();
        configValues.put("node_urls", Arrays.asList("127.0.0.1:8030"));
        configValues.put("username", "root");
        configValues.put("database", "warehouse");
        configValues.put("table", "orders_sink");
        StarRocksSinkConfig config =
                StarRocksSinkConfig.of(ReadonlyConfig.fromMap(configValues));

        TableSchema schema = TableSchema.builder()
                .column(Column.builder("id", BasicType.LONG_TYPE).build())
                .build();
        Map<TablePath, CatalogTable> sourceTables =
                new LinkedHashMap<TablePath, CatalogTable>();
        TablePath first = TablePath.of("source_db", "a");
        TablePath second = TablePath.of("source_db", "b");
        sourceTables.put(first, CatalogTable.builder(first, schema).build());
        sourceTables.put(second, CatalogTable.builder(second, schema).build());

        new StarRocksSinkPreparer(config)
                .prepare(
                        new SinkPrepareContext(
                                ReadonlyConfig.fromMap(configValues),
                                sourceTables));
    }
}
