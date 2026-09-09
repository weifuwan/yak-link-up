package com.link.up.connector.file.s3;

import com.google.auto.service.AutoService;
import com.link.up.api.configuration.ReadonlyConfig;
import com.link.up.api.configuration.util.OptionRule;
import com.link.up.api.connector.schema.ConnectorCapability;
import com.link.up.api.source.Source;
import com.link.up.api.source.SourceFactoryContext;
import com.link.up.api.table.catalog.CatalogTable;
import com.link.up.api.table.factory.TableSourceFactory;
import com.link.up.connector.file.config.FileSourceBaseConfig;
import com.link.up.connector.file.source.FileSource;
import com.link.up.connector.file.source.FileSourceSplit;
import com.link.up.connector.file.source.FileSourceSupport;

import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * SPI factory for the S3 member of the file family (identifier
 * {@code s3file}).
 */
@AutoService(TableSourceFactory.class)
public final class S3FileSourceFactory
        implements TableSourceFactory<FileSourceSplit> {

    @Override
    public String factoryIdentifier() {
        return "s3file";
    }

    @Override
    public Set<ConnectorCapability> capabilities() {
        return Collections.unmodifiableSet(
                EnumSet.of(
                        ConnectorCapability.TABLE_SCHEMA_DISCOVERY,
                        ConnectorCapability.PARTITION_SPLIT));
    }

    @Override
    public Source<FileSourceSplit> createSource(SourceFactoryContext context) {
        S3FileSourceConfig config = baseConfig(context);
        return new FileSource(config.getBase(), config.createFactory());
    }

    @Override
    public List<CatalogTable> discoverTableSchemas(SourceFactoryContext context) throws Exception {
        S3FileSourceConfig config = baseConfig(context);
        return FileSourceSupport.discoverTableSchemas(config.getBase(), config.createFactory());
    }

    @Override
    public OptionRule optionRule() {
        return FileSourceSupport.baseRule()
                .optional(
                        S3FileSourceOptions.ENDPOINT,
                        S3FileSourceOptions.BUCKET,
                        S3FileSourceOptions.ACCESS_KEY,
                        S3FileSourceOptions.SECRET_KEY,
                        S3FileSourceOptions.REGION,
                        S3FileSourceOptions.PATH_STYLE_ACCESS)
                .build();
    }

    private S3FileSourceConfig baseConfig(SourceFactoryContext context) {
        Objects.requireNonNull(context, "context must not be null");
        ReadonlyConfig options = Objects.requireNonNull(
                context.getOptions(),
                "source options must not be null");
        return S3FileSourceConfig.of(options);
    }
}
