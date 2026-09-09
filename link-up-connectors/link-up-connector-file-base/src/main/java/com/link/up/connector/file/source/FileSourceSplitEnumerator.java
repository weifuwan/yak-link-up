package com.link.up.connector.file.source;

import com.link.up.api.source.SourceSplitEnumerator;
import com.link.up.connector.file.config.FileFormat;
import com.link.up.connector.file.config.FileSourceBaseConfig;
import com.link.up.connector.file.internal.FileStorageFactory;
import com.link.up.connector.file.internal.FileEntry;
import com.link.up.connector.file.internal.FileRowSplitter;
import com.link.up.connector.file.internal.FileStorage;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Lists files once, validates extensions against the declared format, and
 * plans row-aligned bounded splits. Split order follows the sorted file keys
 * so enumeration is deterministic and repeatable.
 */
public final class FileSourceSplitEnumerator
        implements SourceSplitEnumerator<FileSourceSplit> {

    private final FileSourceBaseConfig config;
    private final FileStorage storage;

    public FileSourceSplitEnumerator(
            FileSourceBaseConfig config,
            FileStorageFactory storageFactory) {

        this.config = Objects.requireNonNull(config, "config must not be null");
        this.storage = Objects.requireNonNull(storageFactory, "storageFactory must not be null").create();
    }

    FileSourceSplitEnumerator(
            FileSourceBaseConfig config,
            FileStorage storage) {

        this.config = Objects.requireNonNull(config, "config must not be null");
        this.storage = Objects.requireNonNull(storage, "storage must not be null");
    }

    @Override
    public List<FileSourceSplit> enumerateSplits() {
        List<FileEntry> files = storage.listFiles(config.getPath(), config.isRecursive());
        List<FileEntry> selected = filter(files);

        if (selected.isEmpty()) {
            throw new IllegalStateException(
                    "No files matched under '" + config.getPath() + "'"
                            + (config.getFilePattern() == null
                            ? ""
                            : " with file_pattern " + config.getFilePattern().pattern()));
        }

        List<FileSourceSplit> splits = new ArrayList<FileSourceSplit>();
        int sequence = 0;
        for (FileEntry file : selected) {
            if (file.getSize() == 0) {
                continue;
            }
            List<FileRowSplitter.FileSplitRange> ranges = FileRowSplitter.plan(
                    storage,
                    file.getFileKey(),
                    file.getSize(),
                    config.getSplitSize(),
                    config.isGzipFile(fileNameOf(file.getFileKey()))
                            || storage.wholeFileOnly(),
                    config.getFormat() == FileFormat.CSV,
                    config.getQuoteChar());
            for (FileRowSplitter.FileSplitRange range : ranges) {
                splits.add(new FileSourceSplit(
                        sequence++,
                        config.getTableName(),
                        file.getFileKey(),
                        range.getStart(),
                        range.getLength()));
            }
        }
        return splits;
    }

    private List<FileEntry> filter(List<FileEntry> files) {
        Pattern pattern = config.getFilePattern();
        List<FileEntry> selected = new ArrayList<FileEntry>();
        for (FileEntry file : files) {
            String fileName = fileNameOf(file.getFileKey());
            if (pattern != null && !pattern.matcher(fileName).matches()) {
                continue;
            }
            validateExtension(fileName, file.getFileKey());
            validateCompression(fileName, file.getFileKey());
            selected.add(file);
        }
        return selected;
    }

    private void validateCompression(String fileName, String fileKey) {
        if (!config.isGzipFile(fileName) && FileFormat.isGzipByName(fileName)) {
            throw new IllegalStateException(
                    "File '" + fileKey + "' is gz compressed but the job declares compression=none"
                            + "; exclude it with file_pattern or fix compression");
        }
    }

    private void validateExtension(String fileName, String fileKey) {
        FileFormat fileFormat = FileFormat.fromExtension(
                FileFormat.stripCompressionSuffix(fileName));
        if (fileFormat != null && fileFormat != config.getFormat()) {
            throw new IllegalStateException(
                    "File '" + fileKey + "' looks like " + fileFormat
                            + " but the job declares format " + config.getFormat()
                            + "; exclude it with file_pattern or align the format");
        }
    }

    private static String fileNameOf(String fileKey) {
        int nameStart = Math.max(fileKey.lastIndexOf('/'), fileKey.lastIndexOf('\\'));
        return nameStart < 0 ? fileKey : fileKey.substring(nameStart + 1);
    }

    @Override
    public void close() {
        storage.close();
    }
}
