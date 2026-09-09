package com.link.up.connector.file.converter;

import com.link.up.api.table.catalog.TableSchema;
import com.link.up.connector.file.config.FileFormat;
import com.link.up.connector.file.config.FileSourceBaseConfig;
import com.link.up.connector.file.schema.FileSchemaResolver;
import org.apache.commons.csv.CSVFormat;

import java.util.List;
import java.util.Objects;

/**
 * Creates the record converter selected by the resolved format.
 *
 * <p>Delimited formats (csv/tsv) are parsed by the CSV layer in the reader,
 * which hands over assembled records that may span multiple physical lines;
 * line-based formats (text/jsonl) convert one line at a time.
 */
public final class FileRowConverters {

    private FileRowConverters() {
    }

    public static DelimitedRowConverter createDelimited(FileSourceBaseConfig config, TableSchema schema) {
        Objects.requireNonNull(config, "config must not be null");
        Objects.requireNonNull(schema, "schema must not be null");

        List<String> projected = FileSchemaResolver.projectedNames(schema, config.getFields());
        List<Integer> outputIndexes = DelimitedRowConverter.outputIndexes(schema, projected);

        switch (config.getFormat()) {
            case CSV:
                return DelimitedRowConverter.csv(
                        schema,
                        outputIndexes,
                        config.getNullValue(),
                        config.getQuoteChar(),
                        config.getEscapeChar());
            case TSV:
                return DelimitedRowConverter.tsv(schema, outputIndexes, config.getNullValue());
            default:
                throw new IllegalStateException(
                        "Not a delimited format: " + config.getFormat());
        }
    }

    public static FileRowConverter createLine(FileSourceBaseConfig config, TableSchema schema) {
        Objects.requireNonNull(config, "config must not be null");
        Objects.requireNonNull(schema, "schema must not be null");

        List<String> projected = FileSchemaResolver.projectedNames(schema, config.getFields());
        List<Integer> outputIndexes = DelimitedRowConverter.outputIndexes(schema, projected);

        switch (config.getFormat()) {
            case JSONL:
                return new JsonLineRowConverter(schema, outputIndexes);
            case TEXT:
                return new TextRowConverter();
            default:
                throw new IllegalStateException(
                        "Not a line-based format: " + config.getFormat());
        }
    }

    /** The CSV parsing format for the configured delimited format. */
    public static CSVFormat delimitedFormat(FileSourceBaseConfig config) {
        if (config.getFormat() == FileFormat.TSV) {
            return DelimitedRowConverter.tsvFormat();
        }
        if (config.getFormat() == FileFormat.CSV) {
            return DelimitedRowConverter.csvFormat(config.getQuoteChar(), config.getEscapeChar());
        }
        throw new IllegalStateException(
                "Not a delimited format: " + config.getFormat());
    }
}
