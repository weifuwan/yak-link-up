package com.link.up.connector.starrocks.sink;

import com.google.auto.service.AutoService;
import com.link.up.api.configuration.ReadonlyConfig;
import com.link.up.api.configuration.util.OptionRule;
import com.link.up.api.connector.schema.ConnectorCapability;
import com.link.up.api.factory.SinkFactory;
import com.link.up.api.sink.PreparedSinkMetadata;
import com.link.up.api.sink.SinkPreparer;
import com.link.up.api.sink.SinkWriter;
import com.link.up.api.table.type.FluxRow;
import com.link.up.connector.starrocks.config.StarRocksSinkConfig;
import com.link.up.connector.starrocks.config.StarRocksSinkOptions;

import java.util.Collections;
import java.util.Set;

/** SPI factory for the bounded StarRocks Stream Load Sink. */
@AutoService(SinkFactory.class)
public final class StarRocksSinkFactory implements SinkFactory {

    @Override
    public String factoryIdentifier() {
        return "starrocks";
    }

    @Override
    public Set<ConnectorCapability> capabilities() {
        return Collections.emptySet();
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(
                        StarRocksSinkOptions.NODE_URLS,
                        StarRocksSinkOptions.USERNAME,
                        StarRocksSinkOptions.DATABASE,
                        StarRocksSinkOptions.TABLE)
                .optional(
                        StarRocksSinkOptions.PASSWORD,
                        StarRocksSinkOptions.LABEL_PREFIX,
                        StarRocksSinkOptions.LOAD_FORMAT,
                        StarRocksSinkOptions.BATCH_MAX_ROWS,
                        StarRocksSinkOptions.BATCH_MAX_BYTES,
                        StarRocksSinkOptions.MAX_RETRIES,
                        StarRocksSinkOptions.RETRY_BACKOFF_MS,
                        StarRocksSinkOptions.MAX_RETRY_BACKOFF_MS,
                        StarRocksSinkOptions.CONNECT_TIMEOUT_MS,
                        StarRocksSinkOptions.SOCKET_TIMEOUT_MS,
                        StarRocksSinkOptions.LABEL_STATE_TIMEOUT_MS,
                        StarRocksSinkOptions.LABEL_STATE_POLL_MS,
                        StarRocksSinkOptions.COLUMN_SEPARATOR,
                        StarRocksSinkOptions.ROW_DELIMITER,
                        StarRocksSinkOptions.STREAM_LOAD_PARAMS)
                .build();
    }

    @Override
    public SinkPreparer createPreparer(ReadonlyConfig config) {
        return new StarRocksSinkPreparer(StarRocksSinkConfig.of(config));
    }

    @Override
    public SinkWriter<FluxRow> createSink(
            ReadonlyConfig config,
            PreparedSinkMetadata metadata) {
        return new StarRocksSinkWriter(
                StarRocksSinkConfig.of(config),
                metadata);
    }
}
