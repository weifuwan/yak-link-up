package com.link.up.connector.file.config;

import com.link.up.api.configuration.ReadonlyConfig;
import com.link.up.api.table.catalog.TablePath;

import java.io.Serializable;
import java.nio.charset.Charset;
import java.nio.charset.UnsupportedCharsetException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

/**
 * Storage-agnostic bounded File Source configuration, fully validated at
 * construction.
 *
 * <p>Format, compression, table name and split rules are resolved here once;
 * the enumerator, splitter and reader only consume the resolved values.
 * Storage-specific options (S3/SFTP) are parsed and validated by the leaf
 * connector modules, which also supply the {@code FileStorage} instance.
 */
public final class FileSourceBaseConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    private static final long MIN_SPLIT_SIZE = 1048576L;
    private static final long DEFAULT_SPLIT_SIZE = 134217728L;

    /**
     * Charsets where 0x0A and 0x22 are always single self-contained bytes, so
     * byte-level row alignment can never cut a character in half.
     */
    private static final Set<String> SPLIT_SAFE_ENCODINGS = new HashSet<String>(Arrays.asList(
            "UTF-8", "US-ASCII", "ISO-8859-1", "ISO-8859-15", "WINDOWS-1252",
            "GBK", "GB2312", "GB18030", "BIG5", "EUC-JP", "EUC-KR", "SHIFT_JIS"));

    private final String path;
    private final FileFormat format;
    private final List<FileColumnSpec> declaredColumns;
    private final boolean header;
    private final List<String> fields;
    private final Character explicitDelimiter;
    private final char quoteChar;
    private final char escapeChar;
    private final long skipHeaderRows;
    private final String nullValue;
    private final Charset encoding;
    private final String compressionMode;
    private final String tableName;
    private final long splitSize;
    private final boolean recursive;
    private final Pattern filePattern;

    private FileSourceBaseConfig(Builder builder) {
        this.path = builder.path;
        this.format = builder.format;
        this.declaredColumns = builder.declaredColumns == null
                ? null
                : Collections.unmodifiableList(new ArrayList<FileColumnSpec>(builder.declaredColumns));
        this.header = builder.header;
        this.fields = Collections.unmodifiableList(new ArrayList<String>(builder.fields));
        this.explicitDelimiter = builder.explicitDelimiter;
        this.quoteChar = builder.quoteChar;
        this.escapeChar = builder.escapeChar;
        this.skipHeaderRows = builder.skipHeaderRows;
        this.nullValue = builder.nullValue;
        this.encoding = builder.encoding;
        this.compressionMode = builder.compressionMode;
        this.tableName = builder.tableName;
        this.splitSize = builder.splitSize;
        this.recursive = builder.recursive;
        this.filePattern = builder.filePattern;
    }

    public static FileSourceBaseConfig of(ReadonlyConfig config) {
        Objects.requireNonNull(config, "config must not be null");
        return new Builder(config).build();
    }

    public String getPath() {
        return path;
    }

    public FileFormat getFormat() {
        return format;
    }

    /** Explicitly declared columns; null when column names come from the file header. */
    public List<FileColumnSpec> getDeclaredColumns() {
        return declaredColumns;
    }

    public boolean hasDeclaredSchema() {
        return declaredColumns != null;
    }

    public boolean isHeader() {
        return header;
    }

    public List<String> getFields() {
        return fields;
    }

    public boolean hasProjection() {
        return !fields.isEmpty();
    }

    /** Explicit delimiter for the text format; csv/tsv use their fixed delimiter. */
    public Character getExplicitDelimiter() {
        return explicitDelimiter;
    }

    public char getDelimiter() {
        if (format == FileFormat.CSV) {
            return ',';
        }
        if (format == FileFormat.TSV) {
            return '\t';
        }
        return explicitDelimiter == null ? '\001' : explicitDelimiter;
    }

    public char getQuoteChar() {
        return quoteChar;
    }

    public char getEscapeChar() {
        return escapeChar;
    }

    public long getSkipHeaderRows() {
        return skipHeaderRows;
    }

    public String getNullValue() {
        return nullValue;
    }

    public Charset getEncoding() {
        return encoding;
    }

    /**
     * Resolves compression for ONE file: explicit gz wins, auto detects from
     * the .gz extension. Directories may mix compressed and plain files, so
     * the decision is per file, never per job.
     */
    public boolean isGzipFile(String fileName) {
        if ("gz".equals(compressionMode)) {
            return true;
        }
        return "auto".equals(compressionMode) && FileFormat.isGzipByName(fileName);
    }

    public String getTableName() {
        return tableName;
    }

    public TablePath getTablePath() {
        return TablePath.of(tableName);
    }

    /**
     * Returns a copy whose engine-facing path is replaced; leaf connectors use
     * it to hand the engine the resolved location (e.g. an S3 key prefix)
     * while keeping the original path for table-name derivation.
     */
    public FileSourceBaseConfig withPath(String newPath) {
        Builder copy = new Builder(null);
        copy.path = newPath;
        copy.format = format;
        copy.declaredColumns = declaredColumns == null
                ? null
                : new ArrayList<FileColumnSpec>(declaredColumns);
        copy.header = header;
        copy.fields = new ArrayList<String>(fields);
        copy.explicitDelimiter = explicitDelimiter;
        copy.quoteChar = quoteChar;
        copy.escapeChar = escapeChar;
        copy.skipHeaderRows = skipHeaderRows;
        copy.nullValue = nullValue;
        copy.encoding = encoding;
        copy.compressionMode = compressionMode;
        copy.tableName = tableName;
        copy.splitSize = splitSize;
        copy.recursive = recursive;
        copy.filePattern = filePattern;
        return new FileSourceBaseConfig(copy);
    }

    public long getSplitSize() {
        return splitSize;
    }

    public boolean isRecursive() {
        return recursive;
    }

    public Pattern getFilePattern() {
        return filePattern;
    }

    @Override
    public String toString() {
        return "FileSourceBaseConfig{"
                + "path='" + path + '\''
                + ", format=" + format
                + ", table=" + tableName
                + ", compression=" + compressionMode
                + ", splitSize=" + splitSize
                + '}';
    }

    private static final class Builder {

        private final ReadonlyConfig config;

        private String path;
        private FileFormat format;
        private List<FileColumnSpec> declaredColumns;
        private boolean header;
        private List<String> fields = Collections.emptyList();
        private Character explicitDelimiter;
        private char quoteChar = '"';
        private char escapeChar = '"';
        private long skipHeaderRows;
        private String nullValue;
        private Charset encoding;
        private String compressionMode;
        private String tableName;
        private long splitSize = DEFAULT_SPLIT_SIZE;
        private boolean recursive = true;
        private Pattern filePattern;

        private Builder(ReadonlyConfig config) {
            this.config = config;
        }

        private FileSourceBaseConfig build() {
            parsePath();
            parseFormat();
            parseSchemaAndHeader();
            parseDelimitedOptions();
            parseEncodingAndCompression();
            parseTableName();
            parseSplitOptions();
            return new FileSourceBaseConfig(this);
        }

        private void parsePath() {
            if (config == null) {
                return;
            }
            path = requireText(FileSourceOptions.PATH, "path");
        }

        private void parseFormat() {
            String explicit = config.getOptional(FileSourceOptions.FORMAT)
                    .map(value -> value.trim().toLowerCase(Locale.ROOT))
                    .orElse(null);
            if (explicit != null) {
                try {
                    format = FileFormat.valueOf(explicit.toUpperCase(Locale.ROOT));
                } catch (IllegalArgumentException failure) {
                    throw new IllegalArgumentException(
                            "format must be one of csv, tsv, text, jsonl, but was: "
                                    + config.get(FileSourceOptions.FORMAT),
                            failure);
                }
                return;
            }

            String detectable = path;
            if (detectable.endsWith("/") || detectable.endsWith("\\")) {
                detectable = detectable.substring(0, detectable.length() - 1);
            }
            int nameStart = Math.max(detectable.lastIndexOf('/'), detectable.lastIndexOf('\\'));
            String fileName = nameStart < 0 ? detectable : detectable.substring(nameStart + 1);
            FileFormat detected = FileFormat.fromExtension(
                    FileFormat.stripCompressionSuffix(fileName));
            if (detected == null) {
                throw new IllegalArgumentException(
                        "format is required when the path has no recognizable file extension: " + path);
            }
            format = detected;
        }

        private void parseSchemaAndHeader() {
            header = config.get(FileSourceOptions.HEADER);

            List<Map> rawSchema = config.getOptional(FileSourceOptions.SCHEMA).orElse(null);
            if (rawSchema != null && header) {
                throw new IllegalArgumentException("schema and header are mutually exclusive");
            }
            if (header && format != FileFormat.CSV && format != FileFormat.TSV) {
                throw new IllegalArgumentException(
                        "header is only supported for csv and tsv, but the format is " + format.name());
            }

            if (rawSchema != null) {
                if (rawSchema.isEmpty()) {
                    throw new IllegalArgumentException("schema must not be empty when configured");
                }
                Set<String> seen = new HashSet<String>();
                List<FileColumnSpec> columns = new ArrayList<FileColumnSpec>(rawSchema.size());
                for (int i = 0; i < rawSchema.size(); i++) {
                    FileColumnSpec column = FileColumnSpec.parse(i, rawSchema.get(i));
                    if (!seen.add(column.getName())) {
                        throw new IllegalArgumentException("Duplicate file column: " + column.getName());
                    }
                    columns.add(column);
                }
                declaredColumns = columns;
            }

            if (declaredColumns == null && !header) {
                if (format == FileFormat.TEXT) {
                    declaredColumns = Collections.singletonList(textContentColumn());
                } else {
                    throw new IllegalArgumentException(
                            "File Source requires an explicit schema or header=true for format "
                                    + format.name());
                }
            }

            if (format == FileFormat.TEXT && declaredColumns != null) {
                FileColumnSpec only = declaredColumns.get(0);
                if (declaredColumns.size() != 1 || only.getSqlType() != com.link.up.api.table.type.SqlType.STRING) {
                    throw new IllegalArgumentException(
                            "text format yields a single string column; declare no schema or one string column");
                }
            }

            if (format == FileFormat.JSONL && declaredColumns == null) {
                throw new IllegalArgumentException(
                        "jsonl requires an explicit schema in this stage; inference is not available yet");
            }

            long configuredSkip = config.get(FileSourceOptions.SKIP_HEADER_ROWS);
            if (configuredSkip < 0) {
                throw new IllegalArgumentException("skip_header_rows must not be negative");
            }
            if (configuredSkip > 0 && format == FileFormat.JSONL) {
                throw new IllegalArgumentException(
                        "skip_header_rows is only supported for csv, tsv and text");
            }
            if (header && configuredSkip > 0) {
                throw new IllegalArgumentException(
                        "header already skips the first row; do not combine it with skip_header_rows");
            }
            skipHeaderRows = configuredSkip;

            List<String> rawFields = config.getOptional(FileSourceOptions.FIELDS).orElse(null);
            if (rawFields != null && !rawFields.isEmpty()) {
                Set<String> unique = new LinkedHashSet<String>();
                for (String field : rawFields) {
                    String normalized = field == null ? null : field.trim();
                    if (normalized == null || normalized.isEmpty()) {
                        throw new IllegalArgumentException("fields values must not be blank");
                    }
                    if (!unique.add(normalized)) {
                        throw new IllegalArgumentException("Duplicate projected field: " + normalized);
                    }
                }
                if (declaredColumns != null) {
                    for (String field : unique) {
                        boolean found = false;
                        for (FileColumnSpec column : declaredColumns) {
                            found |= column.getName().equals(field);
                        }
                        if (!found) {
                            throw new IllegalArgumentException(
                                    "Projected field was not declared in the schema: " + field);
                        }
                    }
                }
                fields = new ArrayList<String>(unique);
            }
        }

        private FileColumnSpec textContentColumn() {
            return FileColumnSpec.parse(0, singleEntryMap("content", "string"));
        }

        private Map<String, Object> singleEntryMap(String name, String type) {
            Map<String, Object> entry = new java.util.LinkedHashMap<String, Object>();
            entry.put("name", name);
            entry.put("type", type);
            return entry;
        }

        private void parseDelimitedOptions() {
            String rawDelimiter = config.getOptional(FileSourceOptions.DELIMITER)
                    .map(String::trim)
                    .orElse(null);
            if (rawDelimiter != null && format != FileFormat.TEXT) {
                throw new IllegalArgumentException(
                        "delimiter is only supported for the text format; csv stays RFC4180 and tsv stays tab");
            }
            if (rawDelimiter != null) {
                if (rawDelimiter.length() != 1) {
                    throw new IllegalArgumentException("delimiter must be exactly one character");
                }
                explicitDelimiter = rawDelimiter.charAt(0);
            }

            String rawQuote = config.getOptional(FileSourceOptions.QUOTE_CHAR)
                    .map(String::trim)
                    .orElse(null);
            String rawEscape = config.getOptional(FileSourceOptions.ESCAPE_CHAR)
                    .map(String::trim)
                    .orElse(null);
            if ((rawQuote != null || rawEscape != null) && format != FileFormat.CSV) {
                throw new IllegalArgumentException(
                        "quote_char/escape_char are only supported for the csv format");
            }
            if (rawQuote != null) {
                quoteChar = requireSingleChar(rawQuote, "quote_char");
            }
            if (rawEscape != null) {
                escapeChar = requireSingleChar(rawEscape, "escape_char");
            }

            String rawNullValue = config.getOptional(FileSourceOptions.NULL_VALUE).orElse(null);
            if (rawNullValue != null) {
                if (rawNullValue.trim().isEmpty()) {
                    throw new IllegalArgumentException("null_value must not be blank when configured");
                }
                nullValue = rawNullValue;
            }
        }

        private void parseEncodingAndCompression() {
            String encodingName = config.get(FileSourceOptions.ENCODING);
            try {
                encoding = Charset.forName(encodingName.trim());
            } catch (UnsupportedCharsetException | java.nio.charset.IllegalCharsetNameException failure) {
                throw new IllegalArgumentException(
                        "Unsupported encoding: " + encodingName,
                        failure);
            }
            // Byte-level split alignment scans for 0x0A / 0x22; that is only
            // sound for ASCII-compatible charsets where those bytes never
            // appear inside a multi-byte character.
            if (!SPLIT_SAFE_ENCODINGS.contains(encoding.name().toUpperCase(Locale.ROOT))) {
                throw new IllegalArgumentException(
                        "encoding '" + encoding.name() + "' is not supported in this stage: split alignment "
                                + "requires an ASCII-compatible charset such as UTF-8 or GBK");
            }

            String compression = config.get(FileSourceOptions.COMPRESSION)
                    .trim()
                    .toLowerCase(Locale.ROOT);
            if (!"none".equals(compression) && !"gz".equals(compression) && !"auto".equals(compression)) {
                throw new IllegalArgumentException(
                        "compression must be none, gz or auto, but was: " + compression);
            }

            if ("none".equals(compression) && FileFormat.isGzipByName(lastSegment(path))) {
                throw new IllegalArgumentException(
                        "compression=none conflicts with the .gz extension of the path: " + path);
            }
            compressionMode = compression;
        }

        private void parseTableName() {
            String configured = config.getOptional(FileSourceOptions.TABLE_NAME)
                    .map(String::trim)
                    .orElse(null);
            if (configured != null) {
                if (configured.isEmpty()) {
                    throw new IllegalArgumentException("table_name must not be blank");
                }
                tableName = configured;
                return;
            }

            String detectable = FileFormat.stripCompressionSuffix(lastSegment(path));
            int dot = detectable.lastIndexOf('.');
            String base = dot > 0 ? detectable.substring(0, dot) : detectable;
            String sanitized = base.replaceAll("[^A-Za-z0-9_.-]", "_");
            if (sanitized.isEmpty()) {
                throw new IllegalArgumentException(
                        "table_name is required when it cannot be derived from the path: " + path);
            }
            tableName = sanitized;
        }

        private void parseSplitOptions() {
            splitSize = config.get(FileSourceOptions.SPLIT_SIZE);
            if (splitSize < MIN_SPLIT_SIZE) {
                throw new IllegalArgumentException(
                        "split_size must be at least " + MIN_SPLIT_SIZE + " bytes, but was: " + splitSize);
            }
            recursive = config.get(FileSourceOptions.RECURSIVE);

            String rawPattern = config.getOptional(FileSourceOptions.FILE_PATTERN).orElse(null);
            if (rawPattern != null && !rawPattern.trim().isEmpty()) {
                try {
                    filePattern = Pattern.compile(rawPattern.trim());
                } catch (PatternSyntaxException failure) {
                    throw new IllegalArgumentException(
                            "file_pattern is not a valid regex: " + rawPattern,
                            failure);
                }
            }
        }

        private String lastSegment(String rawPath) {
            String detectable = rawPath;
            if (detectable.endsWith("/") || detectable.endsWith("\\")) {
                detectable = detectable.substring(0, detectable.length() - 1);
            }
            int nameStart = Math.max(detectable.lastIndexOf('/'), detectable.lastIndexOf('\\'));
            return nameStart < 0 ? detectable : detectable.substring(nameStart + 1);
        }

        private String requireText(
                com.link.up.api.configuration.Option<String> option,
                String label) {

            String value = config.get(option);
            if (value == null || value.trim().isEmpty()) {
                throw new IllegalArgumentException(label + " must not be blank");
            }
            return value.trim();
        }

        private char requireSingleChar(String value, String label) {
            if (value.length() != 1) {
                throw new IllegalArgumentException(label + " must be exactly one character");
            }
            return value.charAt(0);
        }
    }
}
