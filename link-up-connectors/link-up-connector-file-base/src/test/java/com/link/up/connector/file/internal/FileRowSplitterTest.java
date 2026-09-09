package com.link.up.connector.file.internal;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class FileRowSplitterTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private final LocalFileStorage storage = new LocalFileStorage();

    @Test
    public void shouldUseWholeFileWhenRequested() throws Exception {
        File file = writeText("rows.csv", "a\nb\nc\nd\n");

        List<FileRowSplitter.FileSplitRange> ranges = FileRowSplitter.plan(
                storage, file.getAbsolutePath(), file.length(), 4L, true, true, '"');

        assertEquals(1, ranges.size());
        assertEquals(0L, ranges.get(0).getStart());
        assertEquals(file.length(), ranges.get(0).getLength());
    }

    @Test
    public void shouldAlignCutBackwardToRowBoundary() throws Exception {
        File file = writeText("rows.txt", "row-1abcd\nrow-2efgh\n");

        List<FileRowSplitter.FileSplitRange> ranges = FileRowSplitter.plan(
                storage, file.getAbsolutePath(), file.length(), 12L, false, false, '"');

        // The cut lands inside "row-2efgh", so the boundary moves back to the
        // end of "row-1abcd"; splits are <= split_size and always row-aligned.
        assertEquals(2, ranges.size());
        assertEquals(0L, ranges.get(0).getStart());
        assertEquals("row-1abcd\n".length(), ranges.get(0).getLength());
        assertEquals("row-1abcd\n".length(), ranges.get(1).getStart());
        assertEquals("row-2efgh\n".length(), ranges.get(1).getLength());
    }

    @Test
    public void shouldPreserveQuotedNewlineInsideCsvField() throws Exception {
        String content = "id,note\n1,\"multi\nline\"\n2,ok\n";
        File file = writeText("rows.csv", content);
        int embeddedNewlineEnd = content.indexOf("multi\n") + "multi\n".length();

        List<FileRowSplitter.FileSplitRange> aware = FileRowSplitter.plan(
                storage, file.getAbsolutePath(), file.length(), 20L, false, true, '"');
        List<FileRowSplitter.FileSplitRange> blind = FileRowSplitter.plan(
                storage, file.getAbsolutePath(), file.length(), 20L, false, false, '"');

        assertEquals(content, concatRanges(file, aware));
        assertEquals(content, concatRanges(file, blind));
        // Quote-blind planning cuts right after the embedded newline; the
        // quote-aware planner never starts a split inside the quoted field.
        assertEquals(embeddedNewlineEnd, blind.get(blind.size() - 1).getStart());
        for (FileRowSplitter.FileSplitRange range : aware) {
            org.junit.Assert.assertNotEquals(embeddedNewlineEnd, range.getStart());
        }
    }

    @Test
    public void shouldMergeTrailingPartialRowIntoLastSplit() throws Exception {
        File file = writeText("rows.txt", "aaaaaa\nbbbbbb\ncc");

        List<FileRowSplitter.FileSplitRange> ranges = FileRowSplitter.plan(
                storage, file.getAbsolutePath(), file.length(), 8L, false, false, '"');

        FileRowSplitter.FileSplitRange last = ranges.get(ranges.size() - 1);
        assertEquals("cc".length(), last.getLength());
        assertEquals(file.length() - "cc".length(), last.getStart());

        String joined = slice(file, ranges.get(0))
                + slice(file, ranges.get(1)) + slice(file, last);
        assertEquals("aaaaaa\nbbbbbb\ncc", joined);
    }

    @Test
    public void shouldFailFastWhenAlignmentWindowExceeded() throws Exception {
        StringBuilder giant = new StringBuilder("g");
        for (int i = 0; i < 2048; i++) {
            giant.append("0123456789");
        }
        giant.append("tail-without-newline");
        File file = writeText("giant.txt", giant.toString());

        try {
            FileRowSplitter.plan(
                    storage, file.getAbsolutePath(), file.length(), 1024L, false, false, '"');
            fail("Expected the alignment window exceed to fail fast");
        } catch (IllegalStateException failure) {
            org.junit.Assert.assertTrue(failure.getMessage().contains("row boundary"));
        }
    }

    private String concatRanges(File file, List<FileRowSplitter.FileSplitRange> ranges)
            throws Exception {
        StringBuilder joined = new StringBuilder();
        for (FileRowSplitter.FileSplitRange range : ranges) {
            joined.append(slice(file, range));
        }
        return joined.toString();
    }

    private String slice(File file, FileRowSplitter.FileSplitRange range) throws Exception {
        byte[] bytes = new byte[(int) range.getLength()];
        try (java.io.InputStream input = storage.openRange(
                file.getAbsolutePath(), range.getStart(), range.getLength())) {
            int offset = 0;
            while (offset < bytes.length) {
                int read = input.read(bytes, offset, bytes.length - offset);
                if (read < 0) {
                    break;
                }
                offset += read;
            }
        }
        return new String(bytes, StandardCharsets.UTF_8);
    }

    private File writeText(String name, String content) throws Exception {
        File file = folder.newFile(name);
        try (OutputStream output = new FileOutputStream(file)) {
            output.write(content.getBytes(StandardCharsets.UTF_8));
        }
        return file;
    }
}
