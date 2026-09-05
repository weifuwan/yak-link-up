package com.link.up.connector.starrocks.sink;

import com.link.up.api.sink.PreparedSinkMetadata;
import com.link.up.api.sink.SinkPrepareContext;
import com.link.up.api.sink.SinkPreparer;
import com.link.up.api.table.catalog.CatalogTable;
import com.link.up.api.table.catalog.TablePath;
import com.link.up.connector.starrocks.config.StarRocksSinkConfig;

import java.util.LinkedHashMap;
import java.util.Map;

/** Prepares the target table metadata for the bounded StarRocks Stream Load Sink. */
final class StarRocksSinkPreparer implements SinkPreparer {

    private final StarRocksSinkConfig config;

    StarRocksSinkPreparer(StarRocksSinkConfig config) {
        this.config = config;
    }

    @Override
    public PreparedSinkMetadata prepare(SinkPrepareContext context) {
        if (context == null) {
            throw new IllegalArgumentException("context must not be null");
        }
        if (context.getSourceTables().size() != 1) {
            throw new IllegalArgumentException(
                    "StarRocks Stream Load Sink Stage 2 supports exactly one source table");
        }

        Map<TablePath, CatalogTable> targets =
                new LinkedHashMap<TablePath, CatalogTable>();

        for (Map.Entry<TablePath, CatalogTable> entry : context.getSourceTables().entrySet()) {
            CatalogTable sourceTable = entry.getValue();
            if (sourceTable == null || sourceTable.getTableSchema() == null) {
                throw new IllegalArgumentException(
                        "StarRocks Stream Load Sink requires source table schema");
            }

            TablePath targetPath = TablePath.of(config.getDatabase(), config.getTable());
            CatalogTable targetTable =
                    CatalogTable.builder(targetPath, sourceTable.getTableSchema())
                            .options(sourceTable.getOptions())
                            .build();
            targets.put(entry.getKey(), targetTable);
        }

        return new PreparedSinkMetadata(targets);
    }
}
