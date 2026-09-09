package com.link.up.connector.file.source;

import com.link.up.api.source.RecordBatch;
import com.link.up.api.source.SourceReader;
import com.link.up.api.table.catalog.CatalogTable;
import com.link.up.api.table.catalog.TablePath;
import com.link.up.api.table.catalog.TableSchema;
import com.link.up.api.table.type.FluxRow;
import com.link.up.connector.file.config.FileFormat;
import com.link.up.connector.file.config.FileSourceBaseConfig;
import com.link.up.connector.file.internal.FileStorageFactory;
import com.link.up.connector.file.converter.DelimitedRowConverter;
import com.link.up.connector.file.converter.FileRowConverter;
import com.link.up.connector.file.converter.FileRowConverters;
import com.link.up.connector.file.internal.FileStorage;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.zip.GZIPInputStream;

/**
 * Reads assigned splits one at a time; a split is a bounded byte range that
 * always starts on a row boundary.
 *
 * <p>Leading lines are skipped only in the split starting at offset zero:
 * continuation splits start exactly after a row terminator, so skipping again
 * would silently drop data rows. With {@code header=true} every file's header
 * row is validated against the discovered schema, because column order in one
 * file must never silently re-map another file's columns.
 *
 * <p>Delimited formats are parsed by the CSV layer, which owns record
 * boundaries and therefore reads quoted multi-line records correctly; text
 * and jsonl convert line by line.
 */
public final class FileSourceReader
        implements SourceReader<FluxRow, FileSourceSplit> {

    private final FileSourceBaseConfig config;
    private final FileStorageFactory storageFactory;
    private final Map<TablePath, CatalogTable> tables;
    private final int batchSize;
    private final boolean delimitedFormat;
    private final boolean skipBlankLines;

    private FileStorage storage;
    private TableSchema schema;
    private BufferedReader lineReader;
    private DelimitedRowConverter delimitedConverter;
    private Iterator<CSVRecord> recordIterator;
    private FileRowConverter lineConverter;
    private List<FileSourceSplit> assignedSplits = Collections.emptyList();
    private int nextSplitIndex;
    private FileSourceSplit currentSplit;
    private long splitLocalLine;
    private boolean opened;
    private boolean closed;

    public FileSourceReader(
            FileSourceBaseConfig config,
            FileStorageFactory storageFactory,
            Map<TablePath, CatalogTable> tables,
            int batchSize) {

        this.config = Objects.requireNonNull(config, "config must not be null");
        this.storageFactory = Objects.requireNonNull(storageFactory, "storageFactory must not be null");
        Objects.requireNonNull(tables, "tables must not be null");
        if (batchSize <= 0) {
            throw new IllegalArgumentException("batchSize must be greater than 0");
        }
        this.tables = Collections.unmodifiableMap(
                new LinkedHashMap<TablePath, CatalogTable>(tables));
        this.batchSize = batchSize;
        this.delimitedFormat = config.getFormat() == FileFormat.CSV
                || config.getFormat() == FileFormat.TSV;
        this.skipBlankLines = config.getFormat() != FileFormat.TEXT;
    }

    @Override
    public void open(List<FileSourceSplit> splits) {
        ensureNotOpened();
        this.assignedSplits = Collections.unmodifiableList(
                new ArrayList<FileSourceSplit>(
                        splits == null
                                ? Collections.<FileSourceSplit>emptyList()
                                : splits));
        this.storage = storageFactory.create();
        CatalogTable table = tables.get(config.getTablePath());
        if (table == null) {
            throw new IllegalArgumentException(
                    "No prepared schema found for the file dataset: " + config.getTableName());
        }
        this.schema = table.getTableSchema();
        if (delimitedFormat) {
            this.delimitedConverter = FileRowConverters.createDelimited(config, schema);
        } else {
            this.lineConverter = FileRowConverters.createLine(config, schema);
        }
        this.opened = true;
    }

    @Override
    public void open() {
        open(Collections.<FileSourceSplit>emptyList());
    }

    @Override
    public void openSplit(FileSourceSplit split) {
        Objects.requireNonNull(split, "split must not be null");
        if (!opened) {
            open();
        }
        ensureUsable();
        if (currentSplit != null) {
            throw new IllegalStateException("A file split is already open: " + currentSplit.splitId());
        }
        openCurrentSplit(split);
    }

    @Override
    public RecordBatch<FluxRow> readBatch() {
        ensureUsable();

        while (true) {
            if (currentSplit == null && !openNextAssignedSplit()) {
                return RecordBatch.endOfInput();
            }
            FileSourceSplit batchSplit = currentSplit;

            List<FluxRow> rows = new ArrayList<FluxRow>(batchSize);
            while (rows.size() < batchSize) {
                if (delimitedFormat) {
                    CSVRecord record = nextRecord(batchSplit);
                    if (record == null) {
                        closeSplit();
                        break;
                    }
                    if (isBlankRecord(record)) {
                        continue;
                    }
                    rows.add(delimitedConverter.convert(record.toList(), rowContext(batchSplit)));
                } else {
                    String line = readLineSafe(batchSplit);
                    if (line == null) {
                        closeSplit();
                        break;
                    }
                    splitLocalLine++;
                    if (line.isEmpty() && skipBlankLines) {
                        continue;
                    }
                    rows.add(lineConverter.convert(line, rowContext(batchSplit)));
                }
            }

            if (!rows.isEmpty()) {
                return RecordBatch.of(batchSplit, rows);
            }
        }
    }

    @Override
    public void closeSplit() {
        try {
            if (lineReader != null) {
                lineReader.close();
            }
        } catch (IOException failure) {
            throw new IllegalStateException(
                    "Could not close the split reader of "
                            + (currentSplit == null ? "unknown" : currentSplit.getFileKey()),
                    failure);
        } finally {
            lineReader = null;
            recordIterator = null;
            currentSplit = null;
            splitLocalLine = 0;
        }
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        try {
            closeSplit();
        } finally {
            if (storage != null) {
                storage.close();
            }
            storage = null;
            closed = true;
        }
    }

    private boolean openNextAssignedSplit() {
        if (nextSplitIndex >= assignedSplits.size()) {
            return false;
        }
        FileSourceSplit split = assignedSplits.get(nextSplitIndex++);
        openCurrentSplit(split);
        return true;
    }

    private void openCurrentSplit(FileSourceSplit split) {
        if (!config.getTableName().equals(split.dataSetId())) {
            throw new IllegalArgumentException(
                    "File split does not belong to the configured dataset: " + split.dataSetId());
        }
        boolean wholeFile = config.isGzipFile(fileNameOf(split.getFileKey()))
                || storage.wholeFileOnly();
        if (wholeFile && split.getStartOffset() != 0) {
            throw new IllegalStateException(
                    "A whole-file split must start at offset 0, but offset is "
                            + split.getStartOffset() + " for " + split.getFileKey());
        }

        try {
            InputStream range = storage.openRange(
                    split.getFileKey(),
                    split.getStartOffset(),
                    split.getLength());
            InputStream input = config.isGzipFile(fileNameOf(split.getFileKey()))
                    ? new GZIPInputStream(range)
                    : range;
            this.lineReader = new BufferedReader(
                    new InputStreamReader(input, config.getEncoding()));
            this.currentSplit = split;
            this.splitLocalLine = 0;

            if (split.getStartOffset() == 0) {
                if (config.isHeader()) {
                    String headerLine = lineReader.readLine();
                    splitLocalLine++;
                    if (headerLine == null) {
                        throw new IllegalStateException(
                                "Header row is missing in " + split.getFileKey());
                    }
                    validateHeader(split.getFileKey(), headerLine);
                } else {
                    for (long i = 0; i < config.getSkipHeaderRows(); i++) {
                        if (lineReader.readLine() == null) {
                            break;
                        }
                        splitLocalLine++;
                    }
                }
            }

            if (delimitedFormat) {
                CSVParser parser = FileRowConverters.delimitedFormat(config).parse(lineReader);
                this.recordIterator = parser.iterator();
            }
        } catch (IOException failure) {
            throw new IllegalStateException(
                    "Could not open split of " + split.getFileKey()
                            + " [" + split.getStartOffset() + ", " + split.getLength() + ")",
                    failure);
        }
    }

    /** Column order in every file must match the discovered schema exactly. */
    private void validateHeader(String fileKey, String headerLine) {
        List<String> actual = DelimitedRowConverter.parseSingleRecord(
                FileRowConverters.delimitedFormat(config), headerLine);
        List<String> trimmed = new ArrayList<String>(actual.size());
        for (String name : actual) {
            trimmed.add(name == null ? "" : name.trim());
        }

        List<String> expected = new ArrayList<String>(schema.getColumnCount());
        for (int i = 0; i < schema.getColumnCount(); i++) {
            expected.add(schema.getColumn(i).getName());
        }

        if (!trimmed.equals(expected)) {
            throw new IllegalArgumentException(
                    "Header of " + fileKey + " does not match the discovered schema: expected "
                            + expected + " but found " + trimmed
                            + "; align the file or read it as a separate dataset");
        }
    }

    private CSVRecord nextRecord(FileSourceSplit split) {
        try {
            return recordIterator.hasNext() ? recordIterator.next() : null;
        } catch (java.io.UncheckedIOException failure) {
            throw new IllegalStateException(
                    "Could not read from " + split.getFileKey()
                            + " at split offset " + split.getStartOffset(),
                    failure);
        }
    }

    private String readLineSafe(FileSourceSplit split) {
        try {
            return lineReader.readLine();
        } catch (IOException failure) {
            throw new IllegalStateException(
                    "Could not read from " + split.getFileKey()
                            + " at split offset " + split.getStartOffset(),
                    failure);
        }
    }

    private static boolean isBlankRecord(CSVRecord record) {
        for (String value : record) {
            if (value != null && !value.isEmpty()) {
                return false;
            }
        }
        return true;
    }

    private static String fileNameOf(String fileKey) {
        int nameStart = Math.max(fileKey.lastIndexOf('/'), fileKey.lastIndexOf('\\'));
        return nameStart < 0 ? fileKey : fileKey.substring(nameStart + 1);
    }

    private String rowContext(FileSourceSplit split) {
        return "in " + split.getFileKey()
                + " at split offset " + split.getStartOffset()
                + ", split-local line " + splitLocalLine;
    }

    private void ensureNotOpened() {
        if (opened || closed) {
            throw new IllegalStateException("FileSourceReader has already been opened or closed");
        }
    }

    private void ensureUsable() {
        if (!opened || closed) {
            throw new IllegalStateException("FileSourceReader is not open");
        }
    }
}
