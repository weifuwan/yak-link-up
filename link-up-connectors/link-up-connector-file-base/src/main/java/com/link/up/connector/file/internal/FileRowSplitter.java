package com.link.up.connector.file.internal;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Plans row-aligned byte-range splits for one file.
 *
 * <p>Cut points land after a row terminator so every reader starts on a row
 * boundary. For CSV the boundary must sit outside quoted fields; because the
 * quote state at a cut is unknown, the planner scans backward from the cut to
 * the nearest newline whose quote parity up to the cut is even, re-counting
 * only the bounded window between candidate and cut. Doubling escape semantics
 * (RFC4180 "") keep byte parity correct; the parity trick assumes the quote
 * byte never appears inside multi-byte encoded characters, which holds for
 * UTF-8/GBK but not for UTF-16.
 */
public final class FileRowSplitter {

    private static final int ALIGNMENT_WINDOW = 8 * 1024 * 1024;

    private FileRowSplitter() {
    }

    /** One planned byte range [start, start + length). */
    public static final class FileSplitRange {

        private final long start;
        private final long length;

        private FileSplitRange(
                long start,
                long length) {

            this.start = start;
            this.length = length;
        }

        public long getStart() {
            return start;
        }

        public long getLength() {
            return length;
        }
    }

    public static List<FileSplitRange> plan(
            FileStorage storage,
            String fileKey,
            long fileSize,
            long splitSize,
            boolean wholeFile,
            boolean csvQuoteAware,
            char quoteChar) {

        Objects.requireNonNull(storage, "storage must not be null");
        Objects.requireNonNull(fileKey, "fileKey must not be null");
        if (fileSize <= 0) {
            return new ArrayList<FileSplitRange>();
        }

        List<FileSplitRange> ranges = new ArrayList<FileSplitRange>();
        if (wholeFile || fileSize <= splitSize) {
            ranges.add(new FileSplitRange(0L, fileSize));
            return ranges;
        }

        long start = 0L;
        while (fileSize - start > splitSize) {
            long cut = start + splitSize;
            long boundary = alignToRowBoundary(
                    storage, fileKey, cut, start, csvQuoteAware, quoteChar);
            ranges.add(new FileSplitRange(start, boundary - start));
            start = boundary;
        }
        ranges.add(new FileSplitRange(start, fileSize - start));
        return ranges;
    }

    /**
     * Returns the first row start at or before {@code cut}; fails fast when no
     * unambiguous boundary exists within the alignment window.
     */
    private static long alignToRowBoundary(
            FileStorage storage,
            String fileKey,
            long cut,
            long previousStart,
            boolean csvQuoteAware,
            char quoteChar) {

        long window = Math.min(ALIGNMENT_WINDOW, cut - previousStart);
        long windowStart = cut - window;
        byte[] buffer = readFully(storage, fileKey, windowStart, window);
        byte quote = (byte) quoteChar;

        // The quote depth at the cut equals the parity of all quotes in the
        // window (the window start is assumed to sit at depth 0; a quoted
        // field spanning the whole window is pathological and fails fast).
        // A candidate newline is a true terminator iff the quotes between it
        // and the cut preserve that depth.
        int depthAtCut = 0;
        if (csvQuoteAware) {
            for (byte b : buffer) {
                if (b == quote) {
                    depthAtCut ^= 1;
                }
            }
        }

        int parityAfterCandidate = 0;
        for (int i = buffer.length - 1; i >= 0; i--) {
            if (buffer[i] == '\n') {
                if (!csvQuoteAware || (parityAfterCandidate & 1) == depthAtCut) {
                    long boundary = windowStart + i + 1;
                    if (boundary > previousStart) {
                        return boundary;
                    }
                    break;
                }
            }
            if (csvQuoteAware && buffer[i] == quote) {
                parityAfterCandidate ^= 1;
            }
        }

        throw new IllegalStateException(
                "No row boundary within the " + (window / 1024 / 1024)
                        + "MB alignment window before offset " + cut + " of " + fileKey
                        + "; reduce split_size or check the file for giant rows");
    }

    private static byte[] readFully(
            FileStorage storage,
            String fileKey,
            long start,
            long length) {

        byte[] buffer = new byte[(int) length];
        try (InputStream input = storage.openRange(fileKey, start, length)) {
            int offset = 0;
            while (offset < buffer.length) {
                int read = input.read(buffer, offset, buffer.length - offset);
                if (read < 0) {
                    throw new IllegalStateException(
                            "File ended before the planned alignment window: " + fileKey);
                }
                offset += read;
            }
            return buffer;
        } catch (IOException failure) {
            throw new IllegalStateException(
                    "Could not read the alignment window of " + fileKey + " at offset " + start,
                    failure);
        }
    }
}
