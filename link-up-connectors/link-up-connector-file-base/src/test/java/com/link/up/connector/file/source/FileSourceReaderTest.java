package com.link.up.connector.file.source;

import com.link.up.api.configuration.ReadonlyConfig;
import com.link.up.api.source.RecordBatch;
import com.link.up.api.table.catalog.CatalogTable;
import com.link.up.api.table.catalog.TablePath;
import com.link.up.api.table.type.FluxRow;
import com.link.up.connector.file.config.FileSourceBaseConfig;
import com.link.up.connector.file.internal.LocalFileStorage;
import com.link.up.connector.file.schema.FileSchemaResolver;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPOutputStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class FileSourceReaderTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void shouldSkipHeaderOnlyInFirstSplitAndReadAllRows() throws Exception {
        File dir = folder.newFolder("csv");
        StringBuilder content = new StringBuilder("id,name\n");
        // Build well beyond the 1MB split floor so the file plans 2+ splits.
        for (int i = 1; content.length() < 3L * 1024 * 1024; i++) {
            content.append(i).append(",name-").append(i).append('\n');
        }
        writeText(new File(dir, "rows.csv"), content.toString());

        FileSourceBaseConfig config = config(dir, null);
        List<FileSourceSplit> splits = new FileSourceSplitEnumerator(config, LocalFileStorage::new).enumerateSplits();
        assertTrue("Expected the large csv to plan multiple splits", splits.size() >= 2);

        FileSourceReader reader = new FileSourceReader(config, LocalFileStorage::new, tables(config), 4);
        reader.open(splits);

        String contentText = content.toString();
        long dataRows = contentText.length() - contentText.replace("\n", "").length() - 1L;

        long rows = 0L;
        RecordBatch<FluxRow> batch;
        while (!(batch = reader.readBatch()).isEndOfInput()) {
            for (FluxRow row : batch.getRecords()) {
                rows++;
                long id = (Long) row.getField(0);
                assertTrue("header row leaked into data", id >= 1);
            }
        }
        assertEquals("continuation splits must not skip rows", dataRows, rows);
        reader.close();
    }

    @Test
    public void shouldReadMultilineQuotedCsvRecord() throws Exception {
        File dir = folder.newFolder("multiline");
        writeText(new File(dir, "rows.csv"),
                "id,note\n1,\"multi\nline\"\n2,plain\n");

        FileSourceBaseConfig config = config(dir, null);
        FileSourceReader reader = new FileSourceReader(config, LocalFileStorage::new, tables(config), 10);
        reader.open(new FileSourceSplitEnumerator(config, LocalFileStorage::new).enumerateSplits());

        RecordBatch<FluxRow> first = reader.readBatch();
        assertEquals(2, first.size());
        assertEquals("multi\nline", first.getRecords().get(0).getField(1));
        assertEquals(2L, first.getRecords().get(1).getField(0));
        assertEquals(RecordBatch.<FluxRow>endOfInput().isEndOfInput(), reader.readBatch().isEndOfInput());
        reader.close();
    }

    @Test
    public void shouldValidateHeaderPerFile() throws Exception {
        File dir = folder.newFolder("header");
        writeText(new File(dir, "a.csv"), "id,name\n1,one\n");
        writeText(new File(dir, "b.csv"), "name,id\ntwo,2\n");

        Map<String, Object> values = new LinkedHashMap<String, Object>();
        values.put("path", dir.getAbsolutePath());
        values.put("format", "csv");
        values.put("header", true);
        values.put("split_size", 1048576L);
        FileSourceBaseConfig config = FileSourceBaseConfig.of(ReadonlyConfig.fromMap(values));
        CatalogTable table = FileSchemaResolver.toCatalogTable(
                config, FileSchemaResolver.fromHeader(Arrays.asList("id", "name")));
        Map<TablePath, CatalogTable> tables =
                Collections.singletonMap(config.getTablePath(), table);
        FileSourceReader reader = new FileSourceReader(config, LocalFileStorage::new, tables, 10);
        reader.open(new FileSourceSplitEnumerator(config, LocalFileStorage::new).enumerateSplits());

        // The first file matches the discovered schema; the second one fails.
        RecordBatch<FluxRow> first = reader.readBatch();
        assertEquals(1, first.size());

        try {
            reader.readBatch();
            fail("Expected a mismatched header in a later file to fail");
        } catch (IllegalArgumentException failure) {
            assertTrue(failure.getMessage().contains("does not match the discovered schema"));
        } finally {
            reader.close();
        }
    }

    @Test
    public void shouldReadGzipFileEndToEnd() throws Exception {
        File dir = folder.newFolder("gz");
        File gz = new File(dir, "rows.csv.gz");
        StringBuilder content = new StringBuilder("id,name\n");
        for (int i = 1; i <= 3; i++) {
            content.append(i).append(",name-").append(i).append('\n');
        }
        try (GZIPOutputStream gzip = new GZIPOutputStream(new FileOutputStream(gz))) {
            gzip.write(content.toString().getBytes(StandardCharsets.UTF_8));
        }

        FileSourceBaseConfig config = config(dir, null);
        List<FileSourceSplit> splits = new FileSourceSplitEnumerator(config, LocalFileStorage::new).enumerateSplits();
        assertEquals("A gz file must stay one whole-file split", 1, splits.size());

        FileSourceReader reader = new FileSourceReader(config, LocalFileStorage::new, tables(config), 10);
        reader.open(splits);

        long rows = 0L;
        RecordBatch<FluxRow> batch;
        while (!(batch = reader.readBatch()).isEndOfInput()) {
            rows += batch.size();
        }
        assertEquals(3L, rows);
        reader.close();
    }

    @Test
    public void shouldReadTextAndJsonl() throws Exception {
        File textDir = folder.newFolder("text");
        writeText(new File(textDir, "notes.txt"), "first\n\nsecond\n");
        FileSourceBaseConfig textConfig = textConfig(textDir);
        FileSourceReader textReader = new FileSourceReader(
                textConfig, LocalFileStorage::new, tables(textConfig), 10);
        textReader.open(new FileSourceSplitEnumerator(textConfig, LocalFileStorage::new).enumerateSplits());
        assertEquals(3L, countRows(textReader));
        textReader.close();

        File jsonlDir = folder.newFolder("jsonl");
        writeText(new File(jsonlDir, "rows.jsonl"),
                "{\"id\": 1, \"name\": \"a\"}\n{\"id\": 2, \"name\": \"b\"}\n");
        FileSourceBaseConfig jsonlConfig = jsonlConfig(jsonlDir);
        FileSourceReader jsonlReader = new FileSourceReader(
                jsonlConfig, LocalFileStorage::new, tables(jsonlConfig), 10);
        jsonlReader.open(new FileSourceSplitEnumerator(jsonlConfig, LocalFileStorage::new).enumerateSplits());

        RecordBatch<FluxRow> first = jsonlReader.readBatch();
        assertEquals(1L, first.getRecords().get(0).getField(0));
        assertEquals("a", first.getRecords().get(0).getField(1));
        assertEquals("b", first.getRecords().get(1).getField(1));
        assertEquals(2L, first.size() + countRows(jsonlReader));
        jsonlReader.close();
    }

    @Test
    public void shouldRejectReadBeforeOpen() throws Exception {
        File dir = folder.newFolder("closed");
        writeText(new File(dir, "rows.csv"), "id\n1\n");

        FileSourceReader reader = new FileSourceReader(config(dir, null), LocalFileStorage::new, tables(config(dir, null)), 10);

        try {
            reader.readBatch();
            fail("Expected readBatch before open to be rejected");
        } catch (IllegalStateException failure) {
            assertTrue(failure.getMessage().contains("not open"));
        }
    }

    private static long rowsBytes(StringBuilder content) {
        return content.length();
    }

    private long countRows(FileSourceReader reader) {
        long rows = 0L;
        RecordBatch<FluxRow> batch;
        while (!(batch = reader.readBatch()).isEndOfInput()) {
            rows += batch.size();
        }
        return rows;
    }

    private FileSourceBaseConfig config(File dir, String filePattern) {
        Map<String, Object> values = new LinkedHashMap<String, Object>();
        values.put("path", dir.getAbsolutePath());
        values.put("format", "csv");
        List<Map<String, Object>> schema = new java.util.ArrayList<Map<String, Object>>();
        schema.add(column("id", "bigint"));
        schema.add(column("name", "string"));
        values.put("schema", schema);
        values.put("split_size", 1048576L);
        values.put("skip_header_rows", 1L);
        if (filePattern != null) {
            values.put("file_pattern", filePattern);
        }
        return FileSourceBaseConfig.of(ReadonlyConfig.fromMap(values));
    }

    private FileSourceBaseConfig textConfig(File dir) {
        Map<String, Object> values = new LinkedHashMap<String, Object>();
        values.put("path", dir.getAbsolutePath());
        values.put("format", "text");
        return FileSourceBaseConfig.of(ReadonlyConfig.fromMap(values));
    }

    private FileSourceBaseConfig jsonlConfig(File dir) {
        Map<String, Object> values = new LinkedHashMap<String, Object>();
        values.put("path", dir.getAbsolutePath());
        List<Map<String, Object>> schema = new java.util.ArrayList<Map<String, Object>>();
        schema.add(column("id", "bigint"));
        schema.add(column("name", "string"));
        values.put("schema", schema);
        values.put("format", "jsonl");
        return FileSourceBaseConfig.of(ReadonlyConfig.fromMap(values));
    }

    private Map<TablePath, CatalogTable> tables(FileSourceBaseConfig config) {
        CatalogTable table = FileSchemaResolver.toCatalogTable(
                config,
                FileSchemaResolver.fromDeclared(config.getDeclaredColumns()));
        return Collections.singletonMap(config.getTablePath(), table);
    }

    private static Map<String, Object> column(String name, String type) {
        Map<String, Object> column = new LinkedHashMap<String, Object>();
        column.put("name", name);
        column.put("type", type);
        return column;
    }

    private static void writeText(File file, String content) throws Exception {
        try (OutputStream output = new FileOutputStream(file)) {
            output.write(content.getBytes(StandardCharsets.UTF_8));
        }
    }
}
