package com.link.up.connector.file.config;

import com.link.up.api.configuration.ReadonlyConfig;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class FileSourceBaseConfigTest {

                @Test
    public void shouldInferFormatFromExtension() {
        FileSourceBaseConfig config = FileSourceBaseConfig.of(ReadonlyConfig.fromMap(baseLocalCsv()));

        assertEquals(FileFormat.CSV, config.getFormat());
        assertEquals("rows", config.getTableName());
        assertFalse(config.isGzipFile("rows.csv"));
    }

    @Test
    public void shouldRequireFormatWhenExtensionUnknown() {
        Map<String, Object> values = baseLocalCsv();
        values.put("path", "some/dir/without/extension");

        try {
            FileSourceBaseConfig.of(ReadonlyConfig.fromMap(values));
            fail("Expected format to be required without a known extension");
        } catch (IllegalArgumentException failure) {
            assertTrue(failure.getMessage().contains("format"));
        }
    }

    @Test
    public void shouldRejectSchemaWhenHeaderEnabled() {
        Map<String, Object> values = baseLocalCsv();
        values.put("header", true);

        try {
            FileSourceBaseConfig.of(ReadonlyConfig.fromMap(values));
            fail("Expected schema and header together to be rejected");
        } catch (IllegalArgumentException failure) {
            assertTrue(failure.getMessage().contains("mutually exclusive"));
        }
    }

    @Test
    public void shouldRejectExplicitNoneCompressionForGzExtension() {
        Map<String, Object> values = baseLocalCsv();
        values.put("path", "data/rows.csv.gz");
        values.put("compression", "none");

        try {
            FileSourceBaseConfig.of(ReadonlyConfig.fromMap(values));
            fail("Expected compression=none with a .gz path to be rejected");
        } catch (IllegalArgumentException failure) {
            assertTrue(failure.getMessage().contains("conflicts"));
        }
    }

    @Test
    public void shouldAutoDetectGzCompression() {
        Map<String, Object> values = baseLocalCsv();
        values.put("path", "data/rows.csv.gz");

        FileSourceBaseConfig config = FileSourceBaseConfig.of(ReadonlyConfig.fromMap(values));

        assertTrue(config.isGzipFile("rows.csv.gz"));
        assertFalse(config.isGzipFile("rows.csv"));
        assertEquals("rows", config.getTableName());
    }

    @Test
    public void shouldRejectSplitSizeBelowMinimum() {
        Map<String, Object> values = baseLocalCsv();
        values.put("split_size", 1024L);

        try {
            FileSourceBaseConfig.of(ReadonlyConfig.fromMap(values));
            fail("Expected split_size below the minimum to be rejected");
        } catch (IllegalArgumentException failure) {
            assertTrue(failure.getMessage().contains("split_size"));
        }
    }

            @Test
    public void shouldRejectDelimiterForCsv() {
        Map<String, Object> values = baseLocalCsv();
        values.put("delimiter", "|");

        try {
            FileSourceBaseConfig.of(ReadonlyConfig.fromMap(values));
            fail("Expected a custom delimiter on csv to be rejected");
        } catch (IllegalArgumentException failure) {
            assertTrue(failure.getMessage().contains("delimiter"));
        }
    }

    @Test
    public void shouldRejectUnknownEncoding() {
        Map<String, Object> values = baseLocalCsv();
        values.put("encoding", "NOT_A_CHARSET");

        try {
            FileSourceBaseConfig.of(ReadonlyConfig.fromMap(values));
            fail("Expected an unknown encoding to be rejected");
        } catch (IllegalArgumentException failure) {
            assertTrue(failure.getMessage().contains("encoding"));
        }
    }

    @Test
    public void shouldRejectSplitUnsafeEncoding() {
        Map<String, Object> values = baseLocalCsv();
        values.put("encoding", "UTF-16");

        try {
            FileSourceBaseConfig.of(ReadonlyConfig.fromMap(values));
            fail("Expected UTF-16 to be rejected for byte-level split alignment");
        } catch (IllegalArgumentException failure) {
            assertTrue(failure.getMessage().contains("ASCII-compatible"));
        }
    }

    @Test
    public void shouldResolveTextSchemaAsSingleContentColumn() {
        Map<String, Object> values = new LinkedHashMap<String, Object>();
        values.put("path", "data/notes.txt");

        FileSourceBaseConfig config = FileSourceBaseConfig.of(ReadonlyConfig.fromMap(values));

        assertEquals(1, config.getDeclaredColumns().size());
        assertEquals("content", config.getDeclaredColumns().get(0).getName());
    }

                    private static Map<String, Object> baseSftpWithoutAuth() {
        Map<String, Object> values = new LinkedHashMap<String, Object>();
        values.put("path", "/data/orders");
        values.put("storage_type", "sftp");
        values.put("format", "text");
        values.put("host", "10.0.0.1");
        values.put("user", "sync");
        return values;
    }

    private static Map<String, Object> baseLocalCsv() {
        Map<String, Object> values = new LinkedHashMap<String, Object>();
        values.put("path", "data/rows.csv");
        values.put("schema", schema());
        return values;
    }

    private static List<Map<String, Object>> schema() {
        Map<String, Object> column = new LinkedHashMap<String, Object>();
        column.put("name", "id");
        column.put("type", "bigint");
        return Collections.singletonList(column);
    }
}
