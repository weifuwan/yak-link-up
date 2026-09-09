package com.link.up.connector.file.sftp;

import com.google.auto.service.AutoService;
import com.link.up.api.configuration.ReadonlyConfig;
import com.link.up.api.configuration.util.OptionRule;
import com.link.up.api.connector.schema.ConnectorCapability;
import com.link.up.api.source.Source;
import com.link.up.api.source.SourceFactoryContext;
import com.link.up.api.table.catalog.CatalogTable;
import com.link.up.api.table.factory.TableSourceFactory;
import com.link.up.connector.file.source.FileSource;
import com.link.up.connector.file.source.FileSourceSplit;
import com.link.up.connector.file.source.FileSourceSupport;

import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * SPI factory for the SFTP member of the file family (identifier
 * {@code sftpfile}).
 */
@AutoService(TableSourceFactory.class)
public final class SftpFileSourceFactory
        implements TableSourceFactory<FileSourceSplit> {

    @Override
    public String factoryIdentifier() {
        return "sftpfile";
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
        SftpFileSourceConfig config = baseConfig(context);
        return new FileSource(config.getBase(), config.createFactory());
    }

    @Override
    public List<CatalogTable> discoverTableSchemas(SourceFactoryContext context) throws Exception {
        SftpFileSourceConfig config = baseConfig(context);
        return FileSourceSupport.discoverTableSchemas(config.getBase(), config.createFactory());
    }

    @Override
    public OptionRule optionRule() {
        return FileSourceSupport.baseRule()
                .optional(
                        SftpFileSourceOptions.HOST,
                        SftpFileSourceOptions.PORT,
                        SftpFileSourceOptions.USER,
                        SftpFileSourceOptions.PASSWORD,
                        SftpFileSourceOptions.PRIVATE_KEY,
                        SftpFileSourceOptions.STRICT_HOST_KEY_CHECKING)
                .build();
    }

    private SftpFileSourceConfig baseConfig(SourceFactoryContext context) {
        Objects.requireNonNull(context, "context must not be null");
        ReadonlyConfig options = Objects.requireNonNull(
                context.getOptions(),
                "source options must not be null");
        return SftpFileSourceConfig.of(options);
    }
}
