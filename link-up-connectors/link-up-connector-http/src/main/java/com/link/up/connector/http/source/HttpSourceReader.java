package com.link.up.connector.http.source;

import com.link.up.api.source.RecordBatch;
import com.link.up.api.source.SourceReader;
import com.link.up.api.table.catalog.CatalogTable;
import com.link.up.api.table.catalog.TablePath;
import com.link.up.api.table.type.FluxRow;
import com.link.up.connector.http.client.HttpSourceClient;
import com.link.up.connector.http.config.HttpSourceConfig;
import com.link.up.connector.http.config.PageType;
import com.link.up.connector.http.parser.HttpResponseParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * HTTP 离线数据读取器。
 *
 * <p>Reader 负责：
 * <ol>
 *   <li>执行 HTTP 请求</li>
 *   <li>解析响应数据为 FluxRow</li>
 *   <li>处理分页逻辑</li>
 * </ol>
 */
public final class HttpSourceReader
        implements SourceReader<FluxRow, HttpSourceSplit> {

    private static final Logger LOG =
            LoggerFactory.getLogger(HttpSourceReader.class);

    private final HttpSourceConfig config;
    private final CatalogTable catalogTable;
    private final int batchSize;
    private final HttpSourceClient client;

    private HttpSourceSplit currentSplit;
    private boolean opened;
    private boolean finished;

    // 分页状态
    private int currentPage;
    private String currentCursor;
    private boolean paginationExhausted;

    // 缓冲：上一次请求解析出的行，尚未全部输出
    private List<FluxRow> buffer = Collections.emptyList();
    private int bufferIndex;

    public HttpSourceReader(
            HttpSourceConfig config,
            Map<TablePath, CatalogTable> tables,
            int batchSize) {

        if (batchSize <= 0) {
            throw new IllegalArgumentException("batchSize must be greater than 0");
        }

        this.config = Objects.requireNonNull(config, "config must not be null");
        this.catalogTable = tables.values().iterator().next();
        this.batchSize = batchSize;
        this.client = new HttpSourceClient(config);
    }

    @Override
    public void open(List<HttpSourceSplit> splits) throws Exception {
        if (opened) {
            throw new IllegalStateException("HttpSourceReader has already been opened");
        }
        this.currentSplit = splits.isEmpty() ? new HttpSourceSplit() : splits.get(0);
        this.currentPage = config.getStartPageNumber();
        this.currentCursor = null;
        this.paginationExhausted = !config.hasPagination();
        this.buffer = Collections.emptyList();
        this.bufferIndex = 0;
        this.finished = false;
        this.opened = true;

        LOG.info("HTTP Source Reader opened, url={}, method={}, format={}, pagination={}",
                config.getUrl(), config.getMethod(), config.getFormat(),
                config.hasPagination() ? config.getPageType() : "none");
    }

    @Override
    public RecordBatch<FluxRow> readBatch() throws Exception {
        if (!opened) {
            throw new IllegalStateException("HttpSourceReader has not been opened");
        }
        if (finished) {
            return RecordBatch.endOfInput();
        }

        while (true) {
            // 先从缓冲区取
            while (bufferIndex < buffer.size()) {
                int end = Math.min(bufferIndex + batchSize, buffer.size());
                List<FluxRow> batch = new ArrayList<>(buffer.subList(bufferIndex, end));
                bufferIndex = end;
                return RecordBatch.of(currentSplit, batch);
            }

            // 缓冲区耗尽，获取下一页
            if (paginationExhausted) {
                finished = true;
                return RecordBatch.endOfInput();
            }

            fetchNextPage();
        }
    }

    private void fetchNextPage() throws Exception {
        // 构建当前页的请求参数
        Map<String, String> effectiveHeaders = new LinkedHashMap<>(config.getHeaders());
        Map<String, String> effectiveParams = new LinkedHashMap<>();
        String effectiveBody = config.getBody();

        if (config.hasPagination()) {
            applyPagination(effectiveHeaders, effectiveParams);

            // 占位符模式下，同步替换 body 中的分页占位符
            if (config.isUsePlaceholderReplacement() && effectiveBody != null) {
                effectiveBody = replaceBodyPlaceholder(effectiveBody);
            }
        }

        LOG.debug("HTTP request page={}, cursor={}", currentPage, currentCursor);

        String responseBody = client.execute(effectiveHeaders, effectiveParams, effectiveBody);

        // 解析响应
        List<FluxRow> rows = HttpResponseParser.parseResponse(
                responseBody, config, catalogTable.getTableSchema());

        buffer = rows;
        bufferIndex = 0;

        // 判断分页是否结束
        if (config.hasPagination()) {
            advancePagination(responseBody, rows.size());
        } else {
            paginationExhausted = true;
        }
    }

    private void applyPagination(
            Map<String, String> headers,
            Map<String, String> params) {

        if (config.getPageType() == PageType.CURSOR) {
            applyCursorPagination(headers, params);
        } else {
            applyPageNumberPagination(headers, params);
        }
    }

    private void applyPageNumberPagination(
            Map<String, String> headers,
            Map<String, String> params) {

        String pageField = config.getPageField();
        String pageValue = String.valueOf(currentPage);

        if (config.isUsePlaceholderReplacement()) {
            // 占位符替换模式：替换 headers、params、body 中的 ${page}
            replacePlaceholder(headers, pageField, pageValue);
            replacePlaceholder(params, pageField, pageValue);
        } else {
            // key-based 模式：直接设置参数值
            params.put(pageField, pageValue);
        }
    }

    private void applyCursorPagination(
            Map<String, String> headers,
            Map<String, String> params) {

        if (currentCursor != null && config.getCursorField() != null) {
            if (config.isUsePlaceholderReplacement()) {
                replacePlaceholder(headers, config.getCursorField(), currentCursor);
                replacePlaceholder(params, config.getCursorField(), currentCursor);
            } else {
                params.put(config.getCursorField(), currentCursor);
            }
        }
    }

    private void advancePagination(String responseBody, int rowCount) {
        if (config.getPageType() == PageType.CURSOR) {
            advanceCursorPagination(responseBody);
        } else {
            advancePageNumberPagination(rowCount);
        }
    }

    private void advancePageNumberPagination(int rowCount) {
        long totalPageSize = config.getTotalPageSize();

        if (totalPageSize > 0) {
            if (currentPage >= totalPageSize + config.getStartPageNumber() - 1) {
                LOG.info("分页结束：已达总页数={}, 当前页={}",
                        totalPageSize, currentPage);
                paginationExhausted = true;
                return;
            }
        } else {
            if (rowCount < config.getPageBatchSize()) {
                LOG.info("分页结束：返回行数={} < batch_size={}, 当前页={}",
                        rowCount, config.getPageBatchSize(), currentPage);
                paginationExhausted = true;
                return;
            }
        }

        currentPage++;
        LOG.debug("翻页至 page={}", currentPage);
    }

    private void advanceCursorPagination(String responseBody) {
        if (config.getCursorResponseField() == null
                || config.getCursorResponseField().isEmpty()) {
            LOG.info("Cursor 分页结束：未配置 cursor_response_field");
            paginationExhausted = true;
            return;
        }

        try {
            String cursorValue = HttpResponseParser.extractSingleStringValue(
                    responseBody,
                    config.getCursorResponseField());

            if (cursorValue == null
                    || cursorValue.isEmpty()
                    || "null".equals(cursorValue)) {
                LOG.info("Cursor 分页结束：响应中未找到有效游标值");
                paginationExhausted = true;
                return;
            }

            currentCursor = cursorValue;
            LOG.debug("Cursor 更新为: {}", currentCursor);
        } catch (Exception e) {
            LOG.warn("提取游标值失败，分页结束: {}", e.getMessage());
            paginationExhausted = true;
        }
    }

    /**
     * 替换 body 中的分页占位符（${page} 或 ${cursor}）。
     */
    private String replaceBodyPlaceholder(String body) {
        String result = body;

        if (config.getPageType() == PageType.CURSOR) {
            if (currentCursor != null && config.getCursorField() != null) {
                String placeholder = "${" + config.getCursorField() + "}";
                if (result.contains(placeholder)) {
                    result = result.replace(placeholder, currentCursor);
                }
            }
        } else {
            String pageField = config.getPageField();
            String placeholder = "${" + pageField + "}";
            if (result.contains(placeholder)) {
                result = result.replace(placeholder, String.valueOf(currentPage));
            }
        }

        return result;
    }

    // ── 占位符替换 ──────────────────────────────────────────

    private static void replacePlaceholder(
            Map<String, String> map,
            String field,
            String value) {

        if (map == null) {
            return;
        }
        for (Map.Entry<String, String> entry : map.entrySet()) {
            String v = entry.getValue();
            if (v != null && v.contains("${" + field + "}")) {
                entry.setValue(v.replace("${" + field + "}", value));
            }
        }
    }

    @Override
    public void close() throws Exception {
        if (!opened) {
            return;
        }
        client.close();
        opened = false;
        LOG.info("HTTP Source Reader closed");
    }
}
