package com.link.up.connector.file.converter;

import com.link.up.api.table.catalog.TableSchema;
import com.link.up.api.table.type.FluxRow;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Converts one delimited text line (csv/tsv) to a FluxRow.
 *
 * <p>csv keeps RFC4180 quoting with the configured quote/escape characters;
 * tsv is parsed without quoting. Row field count must match the declared
 * schema exactly.
 */
public final class DelimitedRowConverter {

    private final TableSchema schema;
    private final List<Integer> outputIndexes;
    private final String nullValue;
    private final CSVFormat format;

    private DelimitedRowConverter(
            TableSchema schema,
            List<Integer> outputIndexes,
            String nullValue,
            CSVFormat format) {

        this.schema = schema;
        this.outputIndexes = outputIndexes;
        this.nullValue = nullValue;
        this.format = format;
    }

    public static DelimitedRowConverter csv(
            TableSchema schema,
            List<Integer> outputIndexes,
            String nullValue,
            char quoteChar,
            char escapeChar) {

        return new DelimitedRowConverter(
                schema,
                outputIndexes,
                nullValue,
                csvFormat(quoteChar, escapeChar));
    }

    public static DelimitedRowConverter tsv(
            TableSchema schema,
            List<Integer> outputIndexes,
            String nullValue) {

        return new DelimitedRowConverter(schema, outputIndexes, nullValue, tsvFormat());
    }

    /**
     * csv keeps RFC4180 quoting with the configured quote/escape characters.
     * When the escape equals the quote, doubling is handled by the quote
     * parser itself and the escape is left unset.
     */
    public static CSVFormat csvFormat(char quoteChar, char escapeChar) {
        return CSVFormat.DEFAULT.builder()
                .setDelimiter(',')
                .setQuote(quoteChar)
                .setEscape(quoteChar == escapeChar ? null : escapeChar)
                .setIgnoreEmptyLines(false)
                .setTrim(false)
                .build();
    }

    /** tsv splits on tab without quoting semantics. */
    public static CSVFormat tsvFormat() {
        return CSVFormat.DEFAULT.builder()
                .setDelimiter('\t')
                .setQuote(null)
                .setEscape(null)
                .setIgnoreEmptyLines(false)
                .setTrim(false)
                .build();
    }

    /** Parses exactly one record from one line. */
    public static List<String> parseSingleRecord(CSVFormat format, String line) {
        try (CSVParser parser = format.parse(new StringReader(line))) {
            List<CSVRecord> records = parser.getRecords();
            if (records.size() != 1) {
                throw new IllegalArgumentException(
                        "expected one record per line, but parsed " + records.size());
            }
            return records.get(0).toList();
        } catch (IOException failure) {
            throw new IllegalArgumentException(
                    "Malformed delimited line: " + failure.getMessage(),
                    failure);
        }
    }

    /**
     * Converts one parsed record. The record may span multiple physical lines
     * (quoted embedded newlines); the reader hands over whatever the
     * {@link org.apache.commons.csv.CSVParser} assembled.
     */
    public FluxRow convert(List<String> values, String rowContext) {
        if (values.size() != schema.getColumnCount()) {
            throw new IllegalArgumentException(
                    "Row " + rowContext + " has " + values.size() + " fields but the schema declares "
                            + schema.getColumnCount());
        }

        FluxRow row = new FluxRow(outputIndexes.size());
        for (int i = 0; i < outputIndexes.size(); i++) {
            int columnIndex = outputIndexes.get(i);
            String raw = values.get(columnIndex);
            if (nullValue != null && nullValue.equals(raw)) {
                raw = null;
            }
            try {
                row.setField(i, FileValueConverter.convert(
                        schema.getColumn(columnIndex).getDataType().getSqlType(),
                        scaleOf(columnIndex),
                        raw));
            } catch (RuntimeException failure) {
                throw new IllegalArgumentException(
                        "Column '" + schema.getColumn(columnIndex).getName() + "' " + rowContext
                                + ": " + failure.getMessage(),
                        failure);
            }
        }
        return row;
    }

    private int scaleOf(int columnIndex) {
        Integer scale = schema.getColumn(columnIndex).getScale();
        return scale == null ? -1 : scale;
    }

    static List<Integer> outputIndexes(
            TableSchema schema,
            List<String> projectedNames) {

        Objects.requireNonNull(schema, "schema must not be null");
        List<Integer> indexes = new ArrayList<Integer>(projectedNames.size());
        for (String name : projectedNames) {
            if (!schema.contains(name)) {
                throw new IllegalArgumentException(
                        "Projected field was not found in the schema: " + name);
            }
            indexes.add(schema.indexOf(name));
        }
        return indexes;
    }
}
