package com.link.up.connector.http.source;

import com.link.up.connector.http.config.HttpSourceConfig;
import com.link.up.connector.http.config.PageType;
import com.link.up.connector.http.converter.HttpResponseParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Owns mutable pagination state for one {@link HttpSourceReader}.
 *
 * <p>The reader asks this object for the current request values and reports
 * each response back after parsing. HTTP transport remains in the client and
 * response conversion remains in the converter package.</p>
 */
final class HttpPaginationState {

    private static final Logger LOG =
            LoggerFactory.getLogger(HttpPaginationState.class);

    private final HttpSourceConfig config;

    private int currentPage;
    private String currentCursor;
    private boolean exhausted;

    HttpPaginationState(HttpSourceConfig config) {
        this.config = Objects.requireNonNull(
                config,
                "config must not be null");
        this.currentPage = config.getStartPageNumber();
        this.exhausted = false;
    }

    HttpPageRequest currentRequest() {
        Map<String, String> headers =
                new LinkedHashMap<String, String>(config.getHeaders());
        Map<String, String> params =
                new LinkedHashMap<String, String>();
        String body = config.getBody();

        if (!config.hasPagination()) {
            return new HttpPageRequest(headers, params, body);
        }

        if (config.getPageType() == PageType.CURSOR) {
            applyCursor(headers, params);
        } else {
            applyPageNumber(headers, params);
        }

        if (config.isUsePlaceholderReplacement() && body != null) {
            body = replaceBodyPlaceholder(body);
        }

        return new HttpPageRequest(headers, params, body);
    }

    void advance(String responseBody, int rowCount) {
        if (!config.hasPagination()) {
            exhausted = true;
            return;
        }

        if (config.getPageType() == PageType.CURSOR) {
            advanceCursor(responseBody);
        } else {
            advancePageNumber(rowCount);
        }
    }

    boolean isExhausted() {
        return exhausted;
    }

    int getCurrentPage() {
        return currentPage;
    }

    String getCurrentCursor() {
        return currentCursor;
    }

    private void applyPageNumber(
            Map<String, String> headers,
            Map<String, String> params) {

        String field = config.getPageField();
        String value = String.valueOf(currentPage);

        if (config.isUsePlaceholderReplacement()) {
            replacePlaceholder(headers, field, value);
            replacePlaceholder(params, field, value);
            return;
        }

        params.put(field, value);
    }

    private void applyCursor(
            Map<String, String> headers,
            Map<String, String> params) {

        if (currentCursor == null || config.getCursorField() == null) {
            return;
        }

        if (config.isUsePlaceholderReplacement()) {
            replacePlaceholder(
                    headers,
                    config.getCursorField(),
                    currentCursor);
            replacePlaceholder(
                    params,
                    config.getCursorField(),
                    currentCursor);
            return;
        }

        params.put(
                config.getCursorField(),
                currentCursor);
    }

    private void advancePageNumber(int rowCount) {
        long totalPageSize = config.getTotalPageSize();

        if (totalPageSize > 0) {
            long lastPage =
                    totalPageSize
                            + config.getStartPageNumber()
                            - 1L;

            if (currentPage >= lastPage) {
                LOG.info(
                        "HTTP pagination finished: totalPages={}, currentPage={}",
                        totalPageSize,
                        currentPage);
                exhausted = true;
                return;
            }
        } else if (rowCount < config.getPageBatchSize()) {
            LOG.info(
                    "HTTP pagination finished: rowCount={} < pageBatchSize={}, currentPage={}",
                    rowCount,
                    config.getPageBatchSize(),
                    currentPage);
            exhausted = true;
            return;
        }

        currentPage++;
        LOG.debug("HTTP pagination advanced: page={}", currentPage);
    }

    private void advanceCursor(String responseBody) {
        String responseField = config.getCursorResponseField();

        if (responseField == null || responseField.isEmpty()) {
            LOG.info(
                    "HTTP cursor pagination finished: cursor response field is not configured");
            exhausted = true;
            return;
        }

        try {
            String cursorValue =
                    HttpResponseParser.extractSingleStringValue(
                            responseBody,
                            responseField);

            if (cursorValue == null
                    || cursorValue.isEmpty()
                    || "null".equals(cursorValue)) {

                LOG.info(
                        "HTTP cursor pagination finished: response contains no cursor");
                exhausted = true;
                return;
            }

            currentCursor = cursorValue;
            LOG.debug(
                    "HTTP cursor pagination advanced: cursor={}",
                    currentCursor);

        } catch (Exception failure) {
            LOG.warn(
                    "HTTP cursor pagination stopped because cursor extraction failed: {}",
                    failure.getMessage());
            exhausted = true;
        }
    }

    private String replaceBodyPlaceholder(String body) {
        if (config.getPageType() == PageType.CURSOR) {
            if (currentCursor == null || config.getCursorField() == null) {
                return body;
            }

            return replace(
                    body,
                    config.getCursorField(),
                    currentCursor);
        }

        return replace(
                body,
                config.getPageField(),
                String.valueOf(currentPage));
    }

    private static void replacePlaceholder(
            Map<String, String> values,
            String field,
            String replacement) {

        if (field == null) {
            return;
        }

        for (Map.Entry<String, String> entry : values.entrySet()) {
            String value = entry.getValue();
            if (value != null) {
                entry.setValue(
                        replace(
                                value,
                                field,
                                replacement));
            }
        }
    }

    private static String replace(
            String value,
            String field,
            String replacement) {

        String placeholder = "${" + field + "}";
        return value.contains(placeholder)
                ? value.replace(placeholder, replacement)
                : value;
    }
}
