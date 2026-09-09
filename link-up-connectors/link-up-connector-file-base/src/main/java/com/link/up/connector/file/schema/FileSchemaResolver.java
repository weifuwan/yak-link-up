package com.link.up.connector.file.schema;

import com.link.up.api.table.catalog.CatalogTable;
import com.link.up.api.table.catalog.Column;
import com.link.up.api.table.catalog.TablePath;
import com.link.up.api.table.catalog.TableSchema;
import com.link.up.api.table.type.SqlType;
import com.link.up.connector.file.config.FileColumnSpec;
import com.link.up.connector.file.config.FileSourceBaseConfig;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Resolves the Source output schema for files without a catalog.
 *
 * <p>Priority per design: explicit schema option, then csv/tsv header
 * discovery (all columns STRING, no type guessing), then the text format's
 * single content column. jsonl inference is intentionally out of this stage.
 */
public final class FileSchemaResolver {

    private FileSchemaResolver() {
    }

    public static TableSchema fromDeclared(List<FileColumnSpec> columns) {
        TableSchema.Builder builder = TableSchema.builder();
        for (FileColumnSpec spec : columns) {
            builder.column(toColumn(spec));
        }
        return builder.build();
    }

    public static TableSchema fromHeader(List<String> headerNames) {
        Objects.requireNonNull(headerNames, "headerNames must not be null");
        if (headerNames.isEmpty()) {
            throw new IllegalArgumentException("Header row must not be empty");
        }

        Set<String> seen = new HashSet<String>();
        TableSchema.Builder builder = TableSchema.builder();
        for (String rawName : headerNames) {
            String name = rawName == null ? "" : rawName.trim();
            if (name.isEmpty()) {
                throw new IllegalArgumentException("Header row must not contain blank column names");
            }
            if (!seen.add(name)) {
                throw new IllegalArgumentException("Duplicate column in header row: " + name);
            }
            builder.column(Column.builder(name, com.link.up.api.table.type.BasicType.STRING_TYPE)
                    .nullable(true)
                    .build());
        }
        return builder.build();
    }

    public static CatalogTable toCatalogTable(
            FileSourceBaseConfig config,
            TableSchema schema) {

        return CatalogTable.builder(TablePath.of(config.getTableName()), schema)
                .comment("File source: " + config.getPath())
                .build();
    }

    /** Projected view of the schema in declared column order. */
    public static List<String> projectedNames(
            TableSchema schema,
            List<String> fields) {

        List<String> names = new ArrayList<String>(schema.getColumnCount());
        for (int i = 0; i < schema.getColumnCount(); i++) {
            names.add(schema.getColumn(i).getName());
        }
        if (fields == null || fields.isEmpty()) {
            return names;
        }

        List<String> projected = new ArrayList<String>(fields.size());
        for (String field : fields) {
            for (String name : names) {
                if (name.equals(field)) {
                    projected.add(name);
                    break;
                }
            }
        }
        return projected;
    }

    private static Column toColumn(FileColumnSpec spec) {
        Column.Builder builder = Column.builder(spec.getName(), spec.getDataType())
                .nullable(true);
        if (spec.getSqlType() == SqlType.DECIMAL) {
            builder.precision(spec.getPrecision());
            builder.scale(spec.getScale());
        }
        return builder.build();
    }
}
