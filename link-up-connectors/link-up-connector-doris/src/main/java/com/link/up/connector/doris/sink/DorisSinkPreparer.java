package com.link.up.connector.doris.sink;

import com.link.up.api.sink.PreparedSinkMetadata;
import com.link.up.api.sink.SinkPrepareContext;
import com.link.up.api.sink.SinkPreparer;
import com.link.up.api.sink.TableDdl;
import com.link.up.api.table.catalog.*;
import com.link.up.connector.doris.catalog.DorisCatalog;
import com.link.up.connector.doris.catalog.DorisCatalogConfig;
import com.link.up.connector.doris.catalog.DorisCreateTableSqlBuilder;
import com.link.up.connector.doris.catalog.DorisTypeMapper;
import com.link.up.connector.doris.config.DorisSinkConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Doris Sink 预处理器。
 *
 * <p>在任务启动前执行一次性操作：
 * <ol>
 *   <li>解析目标表路径；</li>
 *   <li>执行自动建表（优先使用用户自定义 DDL，否则自动生成）；</li>
 *   <li>收集建表 DDL 信息用于报告。</li>
 * </ol>
 */
final class DorisSinkPreparer implements SinkPreparer {

    private static final Logger LOG =
            LoggerFactory.getLogger(DorisSinkPreparer.class);

    private final DorisSinkConfig config;

    DorisSinkPreparer(DorisSinkConfig config) {
        this.config = config;
    }

    @Override
    public PreparedSinkMetadata prepare(SinkPrepareContext context) throws Exception {
        Map<TablePath, CatalogTable> targets = new LinkedHashMap<>();
        Map<TablePath, Object> keys = new LinkedHashMap<>();
        Map<TablePath, TableDdl> tableDdls = new LinkedHashMap<>();

        DorisCatalogConfig catalogConfig = new DorisCatalogConfig(
                config.getFenodes(),
                config.getQueryPort(),
                config.getUsername(),
                config.getPassword(),
                config.getDatabase());

        DorisCatalog catalog = new DorisCatalog("doris-sink-preparer", catalogConfig);

        try {
            catalog.open();

            // 确保目标数据库存在
            String targetDatabase = config.getDatabase();
            if (!catalog.databaseExists(targetDatabase)) {
                LOG.info("Target database '{}' does not exist, creating...", targetDatabase);
                catalog.createDatabase(targetDatabase, true);
            }

            for (Map.Entry<TablePath, CatalogTable> entry : context.getSourceTables().entrySet()) {
                TablePath sourcePath = entry.getKey();
                CatalogTable sourceTable = entry.getValue();

                // 解析目标表路径
                TablePath targetPath = TablePath.of(config.getDatabase(), config.getTable());

                // 解析主键
                List<String> primaryKeys = resolvePrimaryKeys(sourceTable);

                // 构建目标 CatalogTable（携带 key-type 配置）
                CatalogTable targetTable = buildTargetTable(targetPath, sourceTable, primaryKeys);

                boolean targetExistedBefore = catalog.tableExists(targetPath);
                String createTableSql;
                boolean executed;
                long durationMillis;

                if (!targetExistedBefore) {
                    long startedAtNanos = System.nanoTime();

                    // 优先使用用户自定义 DDL
                    String userDdl = config.getCreateTableDdl();
                    if (userDdl != null && !userDdl.trim().isEmpty()) {
                        createTableSql = userDdl.trim();
                        LOG.info("Using user-provided DDL for table {}", targetPath);
                        catalog.executeDdl(createTableSql);
                    } else {
                        // 自动生成 DDL
                        DorisTypeMapper typeMapper = new DorisTypeMapper();
                        DorisCreateTableSqlBuilder builder = new DorisCreateTableSqlBuilder(
                                targetPath, targetTable, typeMapper, config.getBuckets());
                        createTableSql = builder.build();
                        LOG.info("Auto-generated DDL for table {}: {}", targetPath, createTableSql);
                        catalog.executeDdl(createTableSql);
                    }

                    durationMillis = TimeUnit.NANOSECONDS.toMillis(
                            System.nanoTime() - startedAtNanos);
                    executed = true;
                } else {
                    createTableSql = "";
                    durationMillis = 0;
                    executed = false;
                }

                String reason = executed
                        ? TableDdl.REASON_TARGET_TABLE_CREATED
                        : targetExistedBefore
                        ? TableDdl.REASON_TARGET_TABLE_ALREADY_EXISTS
                        : TableDdl.REASON_CREATE_TABLE_NOT_EXECUTED;

                TableDdl tableDdl = new TableDdl(
                        "doris",
                        sourcePath.toString(),
                        targetPath.toString(),
                        createTableSql,
                        executed,
                        executed ? TableDdl.STATUS_SUCCEEDED : TableDdl.STATUS_SKIPPED,
                        reason,
                        durationMillis,
                        null,
                        null);

                targets.put(sourcePath, targetTable);
                if (!primaryKeys.isEmpty()) {
                    keys.put(sourcePath, primaryKeys);
                }
                tableDdls.put(sourcePath, tableDdl);
            }

            return new PreparedSinkMetadata(targets, keys, tableDdls);
        } finally {
            catalog.close();
        }
    }

    private CatalogTable buildTargetTable(
            TablePath targetPath,
            CatalogTable sourceTable,
            List<String> primaryKeys) {

        TableSchema.Builder schemaBuilder = TableSchema.builder()
                .columns(sourceTable.getTableSchema().getColumns());

        if (!primaryKeys.isEmpty()) {
            schemaBuilder.primaryKey(PrimaryKey.of("pk_" + targetPath.getTableName(), primaryKeys));
        }

        Map<String, String> options = new LinkedHashMap<>(sourceTable.getOptions());
        // 将配置的 key-type 传递到 CatalogTable options，供 DorisCreateTableSqlBuilder 读取
        String keyType = config.getKeyType();
        if (keyType != null && !keyType.trim().isEmpty()) {
            options.put(DorisCreateTableSqlBuilder.TABLE_OPTION_KEY_TYPE, keyType.trim());
        }

        return CatalogTable.builder(targetPath, schemaBuilder.build())
                .options(options)
                .build();
    }

    private List<String> resolvePrimaryKeys(CatalogTable sourceTable) {
        PrimaryKey pk = sourceTable.getTableSchema().getPrimaryKey();
        if (pk != null && pk.getColumnNames() != null && !pk.getColumnNames().isEmpty()) {
            return new ArrayList<>(pk.getColumnNames());
        }
        return Collections.emptyList();
    }
}
