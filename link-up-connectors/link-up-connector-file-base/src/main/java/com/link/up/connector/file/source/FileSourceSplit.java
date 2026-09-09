package com.link.up.connector.file.source;

import com.link.up.api.source.SourceSplit;

import java.util.Objects;

/** One row-aligned byte-range read unit of a single file. */
public final class FileSourceSplit implements SourceSplit {

    private static final long serialVersionUID = 1L;

    private final String splitId;
    private final String dataSetId;
    private final String fileKey;
    private final long startOffset;
    private final long length;

    public FileSourceSplit(
            int sequence,
            String dataSetId,
            String fileKey,
            long startOffset,
            long length) {

        Objects.requireNonNull(dataSetId, "dataSetId must not be null");
        Objects.requireNonNull(fileKey, "fileKey must not be null");
        if (startOffset < 0) {
            throw new IllegalArgumentException("startOffset must not be negative");
        }
        if (length <= 0) {
            throw new IllegalArgumentException("length must be greater than 0");
        }

        this.splitId = dataSetId + "#" + fileKey + "#" + sequence;
        this.dataSetId = dataSetId;
        this.fileKey = fileKey;
        this.startOffset = startOffset;
        this.length = length;
    }

    @Override
    public String splitId() {
        return splitId;
    }

    @Override
    public String dataSetId() {
        return dataSetId;
    }

    public String getFileKey() {
        return fileKey;
    }

    public long getStartOffset() {
        return startOffset;
    }

    public long getLength() {
        return length;
    }

    @Override
    public String toString() {
        return "FileSourceSplit{"
                + "splitId='" + splitId + '\''
                + ", startOffset=" + startOffset
                + ", length=" + length
                + '}';
    }
}
