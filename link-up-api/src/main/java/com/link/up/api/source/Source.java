package com.link.up.api.source;

import com.link.up.api.table.catalog.CatalogTable;
import com.link.up.api.table.catalog.TablePath;
import com.link.up.api.table.type.FluxRow;

import java.io.Serializable;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * 离线数据源。
 *
 * <p>Split discovery is expressed through {@link SourceSplitEnumerator}. The
 * legacy {@code createSplits(...)} methods remain as compatibility hooks so an
 * existing connector can migrate independently from the framework runtime.</p>
 *
 * @param <SplitT> Source 分片类型
 */
public interface Source<SplitT extends SourceSplit>
        extends Serializable {

    /**
     * Creates the split enumerator for one planning attempt.
     *
     * <p>New connectors should override this method instead of adding split
     * discovery logic directly to the Source object. The default adapter keeps
     * existing connectors source-compatible by delegating to the legacy
     * {@link #createSplits(Map, int)} method.</p>
     */
    default SourceSplitEnumerator<SplitT> createEnumerator(
            Map<TablePath, CatalogTable> tables,
            SourceEnumeratorContext context)
            throws Exception {

        Objects.requireNonNull(
                tables,
                "tables must not be null");
        Objects.requireNonNull(
                context,
                "context must not be null");

        final int parallelism = context.getParallelism();
        final Map<TablePath, CatalogTable> preparedTables =
                Collections.unmodifiableMap(
                        new LinkedHashMap<TablePath, CatalogTable>(tables));

        return new SourceSplitEnumerator<SplitT>() {
            @Override
            public List<SplitT> enumerateSplits()
                    throws Exception {
                return Source.this.createSplits(
                        preparedTables,
                        parallelism);
            }
        };
    }

    /**
     * Legacy split discovery hook.
     *
     * @deprecated Implement {@link #createEnumerator(Map, SourceEnumeratorContext)}
     * instead. Existing connectors may keep overriding this method during the
     * migration period.
     */
    @Deprecated
    default List<SplitT> createSplits(
            Map<TablePath, CatalogTable> tables)
            throws Exception {

        throw new UnsupportedOperationException(
                "Source must implement createEnumerator(...) or legacy createSplits(...)");
    }

    /**
     * Legacy split discovery hook with reader parallelism.
     *
     * <p>The default keeps older connectors that only implemented the
     * single-argument method working unchanged.</p>
     *
     * @deprecated Implement {@link #createEnumerator(Map, SourceEnumeratorContext)}
     * instead.
     */
    @Deprecated
    default List<SplitT> createSplits(
            Map<TablePath, CatalogTable> tables,
            int parallelism)
            throws Exception {

        if (parallelism <= 0) {
            throw new IllegalArgumentException(
                    "parallelism must be greater than 0");
        }
        return createSplits(tables);
    }

    /**
     * Validates execution reader parallelism before connector preparation
     * continues. The framework invokes this during Source preparation so an
     * unsupported mode fails before sink preparation side effects occur.
     */
    default void validateParallelism(int parallelism) {
        if (parallelism <= 0) {
            throw new IllegalArgumentException(
                    "parallelism must be greater than 0");
        }
    }

    /**
     * 创建 SourceReader。
     * <p>
     * 每个执行任务必须使用独立 Reader，
     * Reader 不应在多个线程之间共享。
     */
    SourceReader<FluxRow, SplitT> createReader(
            Map<TablePath, CatalogTable> tables,
            int batchSize);
}
