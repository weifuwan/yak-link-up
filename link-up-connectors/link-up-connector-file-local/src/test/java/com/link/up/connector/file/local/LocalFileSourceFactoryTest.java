package com.link.up.connector.file.local;

import com.link.up.api.configuration.ReadonlyConfig;
import com.link.up.api.connector.schema.ConnectorCapability;
import com.link.up.api.source.Source;
import com.link.up.api.table.catalog.CatalogTable;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class LocalFileSourceFactoryTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private final LocalFileSourceFactory factory = new LocalFileSourceFactory();

    @Test
    public void shouldExposeIdentifierLocalfile() {
        assertEquals("localfile", factory.factoryIdentifier());
    }

    @Test
    public void shouldDeclareSplitAndSchemaCapabilities() {
        assertTrue(factory.capabilities().contains(ConnectorCapability.PARTITION_SPLIT));
        assertTrue(factory.capabilities().contains(ConnectorCapability.TABLE_SCHEMA_DISCOVERY));
    }

    @Test
    public void shouldRejectRemoteSchemes() throws Exception {
        Map<String, Object> values = new LinkedHashMap<String, Object>();
        values.put("path", "s3://bucket/prefix/");
        values.put("format", "csv");
        values.put("schema", Collections.singletonList(column("id", "bigint")));

        try {
            factory.createSource(new com.link.up.api.source.SourceFactoryContext(
                    ReadonlyConfig.fromMap(values)));
            org.junit.Assert.fail("Expected a remote scheme to be rejected by localfile");
        } catch (IllegalArgumentException failure) {
            assertTrue(failure.getMessage().contains("localfile"));
        }
    }

    @Test
    public void shouldDiscoverSchemaFromHeader() throws Exception {
        File dir = folder.newFolder("data");
        File csv = new File(dir, "rows.csv");
        try (OutputStream output = new FileOutputStream(csv)) {
            output.write("id,city\n1,hz\n".getBytes(StandardCharsets.UTF_8));
        }

        Map<String, Object> values = new LinkedHashMap<String, Object>();
        values.put("path", dir.getAbsolutePath());
        values.put("format", "csv");
        values.put("header", true);

        List<CatalogTable> tables = factory.discoverTableSchemas(
                new com.link.up.api.source.SourceFactoryContext(ReadonlyConfig.fromMap(values)));

        assertEquals(1, tables.size());
        assertEquals(2, tables.get(0).getTableSchema().getColumnCount());
    }

    private static Map<String, Object> column(String name, String type) {
        Map<String, Object> column = new LinkedHashMap<String, Object>();
        column.put("name", name);
        column.put("type", type);
        return column;
    }
}
