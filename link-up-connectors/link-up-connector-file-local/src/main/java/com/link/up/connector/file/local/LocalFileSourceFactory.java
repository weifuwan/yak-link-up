package com.link.up.connector.file.local;

import com.google.auto.service.AutoService;
import com.link.up.api.configuration.ReadonlyConfig;
import com.link.up.api.configuration.util.OptionRule;
import com.link.up.api.connector.schema.ConnectorCapability;
import com.link.up.api.source.Source;
import com.link.up.api.source.SourceFactoryContext;
import com.link.up.api.table.catalog.CatalogTable;
import com.link.up.api.table.factory.TableSourceFactory;
import com.link.up.connector.file.config.FileSourceBaseConfig;
import com.link.up.connector.file.internal.LocalFileStorage;
import com.link.up.connector.file.source.FileSource;
import com.link.up.connector.file.source.FileSourceSplit;
import com.link.up.connector.file.source.FileSourceSupport;

import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * SPI factory for the local-file member of the file family (identifier
 * {@code localfile}). Reading, splitting and schema discovery are shared
 * engine code; this leaf only wires the local transport.
 */
@AutoService(TableSourceFactory.class)
public final class LocalFileSourceFactory
        implements TableSourceFactory<FileSourceSplit> {

    @Override
    public String factoryIdentifier() {
        return "localfile";
    }

    @Override
    public Set<ConnectorCapability> capabilities() {
        return Collections.unmodifiableSet(
                EnumSet.of(
                        ConnectorCapability.TABLE_SCHEMA_DISCOVERY,
                        ConnectorCapability.PARTITION_SPLIT));
    }

    @Override
    public Source<FileSourceSplit> createSource(SourceFactoryContext context) throws Exception {
        return new FileSource(baseConfig(context), LocalFileStorage::new);
    }

    @Override
    public List<CatalogTable> discoverTableSchemas(SourceFactoryContext context) throws Exception {
        return FileSourceSupport.discoverTableSchemas(
                baseConfig(context), LocalFileStorage::new);
    }

    @Override
    public OptionRule optionRule() {
        return FileSourceSupport.baseRule().build();
    }

    private FileSourceBaseConfig baseConfig(SourceFactoryContext context) {
        Objects.requireNonNull(context, "context must not be null");
        ReadonlyConfig options = Objects.requireNonNull(
                context.getOptions(),
                "source options must not be null");
        FileSourceBaseConfig config = FileSourceBaseConfig.of(options);
        String lowered = config.getPath().toLowerCase(java.util.Locale.ROOT);
        if (lowered.startsWith("s3://") || lowered.startsWith("sftp://")) {
            throw new IllegalArgumentException(
                    "localfile reads the local filesystem; use the matching file connector for '"
                            + config.getPath() + "'");
        }
        return config;
    }
}
