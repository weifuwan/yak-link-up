package com.link.up.connector.doris.sink;

import com.google.auto.service.AutoService;
import com.link.up.api.configuration.ReadonlyConfig;
import com.link.up.api.configuration.util.OptionRule;
import com.link.up.api.connector.schema.ConnectorCapability;
import com.link.up.api.factory.SinkFactory;
import com.link.up.api.sink.PreparedSinkMetadata;
import com.link.up.api.sink.SinkPreparer;
import com.link.up.api.sink.SinkWriter;
import com.link.up.api.table.type.FluxRow;
import com.link.up.connector.doris.config.DorisSinkConfig;
import com.link.up.connector.doris.config.DorisSinkOptions;

import java.util.Collections;
import java.util.EnumSet;
import java.util.Set;

/**
 * Doris Sink SPI factory。
 *
 * <p>通过 Stream Load 将数据写入 Apache Doris，
 * 支持 JSON / CSV 格式、两阶段提交、自动建表等能力。
 */
@AutoService(SinkFactory.class)
public final class DorisSinkFactory implements SinkFactory {

    @Override
    public String factoryIdentifier() {
        return "doris";
    }

    @Override
    public Set<ConnectorCapability> capabilities() {
        return Collections.unmodifiableSet(
                EnumSet.of(
                        ConnectorCapability.AUTO_CREATE_TABLE,
                        ConnectorCapability.UPSERT));
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(
                        DorisSinkOptions.FENODES,
                        DorisSinkOptions.USERNAME,
                        DorisSinkOptions.DATABASE,
                        DorisSinkOptions.TABLE)
                .optional(
                        DorisSinkOptions.BENODES,
                        DorisSinkOptions.DIRECT_TO_BE,
                        DorisSinkOptions.QUERY_PORT,
                        DorisSinkOptions.PASSWORD,
                        DorisSinkOptions.SINK_LABEL_PREFIX,
                        DorisSinkOptions.SINK_ENABLE_2PC,
                        DorisSinkOptions.SINK_ENABLE_DELETE,
                        DorisSinkOptions.SINK_CHECK_INTERVAL_MS,
                        DorisSinkOptions.SINK_MAX_RETRIES,
                        DorisSinkOptions.SINK_BUFFER_SIZE,
                        DorisSinkOptions.SINK_BUFFER_COUNT,
                        DorisSinkOptions.DORIS_BATCH_SIZE,
                        DorisSinkOptions.LOAD_FORMAT,
                        DorisSinkOptions.CSV_COLUMN_SEPARATOR,
                        DorisSinkOptions.DORIS_CONFIG,
                        DorisSinkOptions.CONNECT_TIMEOUT_MS,
                        DorisSinkOptions.SOCKET_TIMEOUT_MS,
                        // 建表配置
                        DorisSinkOptions.SINK_CREATE_TABLE_DDL,
                        DorisSinkOptions.SINK_KEY_TYPE,
                        DorisSinkOptions.SINK_BUCKETS,
                        // Stream Load 扩展参数
                        DorisSinkOptions.SINK_LOAD_TIMEOUT_SEC,
                        DorisSinkOptions.SINK_MAX_FILTER_RATIO,
                        DorisSinkOptions.SINK_COLUMNS,
                        DorisSinkOptions.SINK_WHERE,
                        DorisSinkOptions.SINK_PARTITIONS,
                        DorisSinkOptions.SINK_STRICT_MODE,
                        DorisSinkOptions.SINK_TIMEZONE,
                        DorisSinkOptions.SINK_EXEC_MEM_LIMIT,
                        DorisSinkOptions.SINK_JSONPATHS,
                        DorisSinkOptions.SINK_STRIP_OUTER_ARRAY,
                        DorisSinkOptions.SINK_JSON_ROOT,
                        DorisSinkOptions.SINK_SEND_BATCH_PARALLELISM,
                        DorisSinkOptions.SINK_LOAD_TO_SINGLE_TABLET,
                        DorisSinkOptions.SINK_LINE_DELIMITER,
                        DorisSinkOptions.SINK_ENCLOSE,
                        DorisSinkOptions.SINK_ESCAPE,
                        DorisSinkOptions.SINK_NUM_AS_STRING,
                        DorisSinkOptions.SINK_FUZZY_PARSE,
                        DorisSinkOptions.SINK_COMPRESS_TYPE,
                        DorisSinkOptions.SINK_TRIM_DOUBLE_QUOTES,
                        DorisSinkOptions.SINK_SKIP_LINES,
                        DorisSinkOptions.SINK_LOAD_COMMENT)
                .build();
    }

    @Override
    public SinkPreparer createPreparer(ReadonlyConfig config) {
        return new DorisSinkPreparer(DorisSinkConfig.of(config));
    }

    @Override
    public SinkWriter<FluxRow> createSink(
            ReadonlyConfig config,
            PreparedSinkMetadata metadata) {

        return new DorisSinkWriter(
                DorisSinkConfig.of(config),
                metadata);
    }
}
