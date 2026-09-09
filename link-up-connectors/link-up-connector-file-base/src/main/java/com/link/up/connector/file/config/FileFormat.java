package com.link.up.connector.file.config;

import java.util.Locale;

/** Supported bounded text file formats. */
public enum FileFormat {

    CSV(",", new String[]{".csv"}),
    TSV("\t", new String[]{".tsv"}),
    TEXT("\001", new String[]{".txt", ".text"}),
    JSONL(null, new String[]{".jsonl", ".ndjson"});

    private final String defaultDelimiter;
    private final String[] extensions;

    FileFormat(String defaultDelimiter, String[] extensions) {
        this.defaultDelimiter = defaultDelimiter;
        this.extensions = extensions;
    }

    public String getDefaultDelimiter() {
        return defaultDelimiter;
    }

    /** Maps a file-name extension (with leading dot) to a format, or null. */
    public static FileFormat fromExtension(String fileName) {
        String lowered = fileName.toLowerCase(Locale.ROOT);
        for (FileFormat format : values()) {
            for (String extension : format.extensions) {
                if (lowered.endsWith(extension)) {
                    return format;
                }
            }
        }
        return null;
    }

    /** True when the file name denotes gzip compression, e.g. rows.csv.gz. */
    public static boolean isGzipByName(String fileName) {
        return fileName.toLowerCase(Locale.ROOT).endsWith(".gz");
    }

    /** Strips the compression suffix so format detection still works on rows.csv.gz. */
    public static String stripCompressionSuffix(String fileName) {
        String lowered = fileName.toLowerCase(Locale.ROOT);
        return lowered.endsWith(".gz")
                ? fileName.substring(0, fileName.length() - 3)
                : fileName;
    }
}
