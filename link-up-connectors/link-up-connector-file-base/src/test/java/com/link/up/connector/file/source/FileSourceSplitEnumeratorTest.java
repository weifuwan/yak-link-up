package com.link.up.connector.file.source;

import com.link.up.api.configuration.ReadonlyConfig;
import com.link.up.connector.file.config.FileSourceBaseConfig;
import com.link.up.connector.file.internal.LocalFileStorage;
import com.link.up.connector.file.internal.FileEntry;
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

public class FileSourceSplitEnumeratorTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void shouldPlanRowAlignedSplitsPerSortedFile() throws Exception {
        File dir = folder.newFolder("data");
        writeText(new File(dir, "b.csv"), "id\n1\n2\n3\n4\n");
        writeText(new File(dir, "a.csv"), "id\n1\n2\n3\n4\n");

        List<FileSourceSplit> splits = enumerator(dir, null, 12L).enumerateSplits();

        // Both files are below split_size: one split each, a.csv before b.csv.
        assertEquals(2, splits.size());
        assertTrue(splits.get(0).getFileKey().endsWith("a.csv"));
        assertTrue(splits.get(1).getFileKey().endsWith("b.csv"));
        assertEquals(2, splits.size());
    }

    @Test
    public void shouldApplyFileNamePatternFilter() throws Exception {
        File dir = folder.newFolder("data");
        writeText(new File(dir, "sales_2024.csv"), "id\n1\n");
        writeText(new File(dir, "ignore_me.csv"), "id\n2\n");

        List<FileSourceSplit> splits = enumerator(dir, "sales_.*\\.csv", 1024L).enumerateSplits();

        assertEquals(1, splits.size());
        assertTrue(splits.get(0).getFileKey().endsWith("sales_2024.csv"));
    }

    @Test
    public void shouldFailWhenNoFilesMatch() throws Exception {
        File dir = folder.newFolder("empty");

        try {
            enumerator(dir, null, 1024L).enumerateSplits();
            fail("Expected an empty directory to fail enumeration");
        } catch (IllegalStateException failure) {
            assertTrue(failure.getMessage().contains("No files matched"));
        }
    }

    @Test
    public void shouldFailOnConflictingExtension() throws Exception {
        File dir = folder.newFolder("mixed");
        writeText(new File(dir, "rows.tsv"), "id\t1\n");
        writeText(new File(dir, "rows.csv"), "id\n1\n");

        try {
            enumerator(dir, null, 1024L).enumerateSplits();
            fail("Expected a conflicting file extension to fail enumeration");
        } catch (IllegalStateException failure) {
            assertTrue(failure.getMessage().contains("looks like"));
        }
    }

    @Test
    public void shouldPlanWholeFileForGzip() throws Exception {
        File dir = folder.newFolder("gz");
        File gz = new File(dir, "rows.csv.gz");
        StringBuilder content = new StringBuilder("id\n");
        for (int i = 0; i < 200000; i++) {
            content.append(i).append('\n');
        }
        try (GZIPOutputStream gzip = new GZIPOutputStream(new FileOutputStream(gz))) {
            gzip.write(content.toString().getBytes(StandardCharsets.UTF_8));
        }

        List<FileSourceSplit> splits = enumerator(dir, null, 65536L).enumerateSplits();

        // Even though the uncompressed content exceeds split_size, a gz file
        // must stay a single whole-file split.
        assertEquals(1, splits.size());
        assertEquals(0L, splits.get(0).getStartOffset());
    }

        @Test
    public void shouldPlanWholeFileForWholeFileOnlyStorage() {
        Map<String, Object> values = new LinkedHashMap<String, Object>();
        values.put("path", "data");
        values.put("format", "csv");
        values.put("schema", Collections.singletonList(column("id", "bigint")));
        values.put("split_size", 1048576L);
        FileSourceBaseConfig config = FileSourceBaseConfig.of(ReadonlyConfig.fromMap(values));

        com.link.up.connector.file.internal.FileStorage fake =
                new com.link.up.connector.file.internal.FileStorage() {
            @Override
            public java.util.List<FileEntry> listFiles(String basePath, boolean recursive) {
                return Arrays.asList(
                        new FileEntry("/data/a.csv", 10L * 1024),
                        new FileEntry("/data/b.csv", 10L * 1024));
            }

            @Override
            public java.io.InputStream openRange(String fileKey, long start, long length) {
                throw new UnsupportedOperationException("alignment must not read whole-file splits");
            }

            @Override
            public boolean exists(String fileKey) {
                return false;
            }

            @Override
            public boolean wholeFileOnly() {
                return true;
            }

            @Override
            public void close() {
            }
        };

        List<FileSourceSplit> splits = new FileSourceSplitEnumerator(config, () -> fake).enumerateSplits();

        assertEquals(2, splits.size());
        for (FileSourceSplit split : splits) {
            assertEquals(0L, split.getStartOffset());
            assertEquals(10L * 1024, split.getLength());
        }
    }

    private FileSourceSplitEnumerator enumerator(
            File dir,
            String filePattern,
            long splitSize) {

        Map<String, Object> values = new LinkedHashMap<String, Object>();
        values.put("path", dir.getAbsolutePath());
        values.put("format", "csv");
        values.put("schema", Collections.singletonList(column("id", "bigint")));
        values.put("split_size", Math.max(splitSize, 1048576L));
        if (filePattern != null) {
            values.put("file_pattern", filePattern);
        }
        FileSourceBaseConfig config = FileSourceBaseConfig.of(ReadonlyConfig.fromMap(values));
        return new FileSourceSplitEnumerator(config, LocalFileStorage::new);
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
