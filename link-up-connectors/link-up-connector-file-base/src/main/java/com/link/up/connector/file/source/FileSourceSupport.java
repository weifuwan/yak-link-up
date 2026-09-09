package com.link.up.connector.file.source;

import com.link.up.api.configuration.Option;
import com.link.up.api.configuration.util.OptionRule;
import com.link.up.api.table.catalog.CatalogTable;
import com.link.up.connector.file.config.FileFormat;
import com.link.up.connector.file.config.FileSourceBaseConfig;
import com.link.up.connector.file.config.FileSourceOptions;
import com.link.up.connector.file.converter.DelimitedRowConverter;
import com.link.up.connector.file.schema.FileSchemaResolver;
import com.link.up.connector.file.internal.FileStorage;
import com.link.up.connector.file.internal.FileStorageFactory;
import org.apache.commons.csv.CSVFormat;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.zip.GZIPInputStream;

/**
 * Shared wiring for the file-family leaf connectors: schema discovery and the
 * base option rule. Each leaf only supplies its identifier, its extra options
 * and a {@link FileStorageFactory}.
 */
public final class FileSourceSupport {

    private FileSourceSupport() {
    }

    /** The option rule every leaf extends: path required, base options optional. */
    public static OptionRule.Builder baseRule() {
        return OptionRule.builder()
                .required(FileSourceOptions.PATH)
                .optional(FileSourceOptions.baseOptions().toArray(new Option<?>[0]));
    }

    public static List<CatalogTable> discoverTableSchemas(
            FileSourceBaseConfig config,
            FileStorageFactory storageFactory) throws Exception {

        if (config.hasDeclaredSchema() || config.getFormat() == FileFormat.TEXT) {
            return Collections.singletonList(
                    FileSchemaResolver.toCatalogTable(
                            config,
                            FileSchemaResolver.fromDeclared(config.getDeclaredColumns())));
        }

        try (FileStorage storage = storageFactory.create()) {
            List<String> header = readFirstHeaderRow(config, storage);
            return Collections.singletonList(
                    FileSchemaResolver.toCatalogTable(
                            config,
                            FileSchemaResolver.fromHeader(header)));
        }
    }

    private static List<String> readFirstHeaderRow(
            FileSourceBaseConfig config,
            FileStorage storage) throws IOException {

        List<com.link.up.connector.file.internal.FileEntry> files =
                storage.listFiles(config.getPath(), config.isRecursive());
        for (com.link.up.connector.file.internal.FileEntry file : files) {
            if (file.getSize() == 0) {
                continue;
            }
            return parseHeaderLine(config, storage, file.getFileKey(), file.getSize());
        }
        throw new IllegalStateException(
                "No non-empty files under '" + config.getPath() + "' to discover a header row");
    }

    private static List<String> parseHeaderLine(
            FileSourceBaseConfig config,
            FileStorage storage,
            String fileKey,
            long size) throws IOException {

        InputStream range = storage.openRange(fileKey, 0L, Math.min(size, 1048576L));
        InputStream input = config.isGzipFile(fileNameOf(fileKey))
                ? new GZIPInputStream(range)
                : range;
        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(input, config.getEncoding()))) {

            String headerLine = reader.readLine();
            if (headerLine == null || headerLine.isEmpty()) {
                throw new IllegalStateException(
                        "Cannot discover a header row from an empty first line of " + fileKey);
            }

            CSVFormat format = config.getFormat() == FileFormat.TSV
                    ? DelimitedRowConverter.tsvFormat()
                    : DelimitedRowConverter.csvFormat(config.getQuoteChar(), config.getEscapeChar());
            List<String> names = DelimitedRowConverter.parseSingleRecord(format, headerLine);
            List<String> trimmed = new ArrayList<String>(names.size());
            for (String name : names) {
                trimmed.add(name == null ? "" : name.trim());
            }
            return trimmed;
        }
    }

    private static String fileNameOf(String fileKey) {
        int nameStart = Math.max(fileKey.lastIndexOf('/'), fileKey.lastIndexOf('\\'));
        return nameStart < 0 ? fileKey : fileKey.substring(nameStart + 1);
    }
}
