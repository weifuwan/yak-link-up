package com.link.up.connector.starrocks.client.sink;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;

/** Parsed StarRocks Stream Load response. */
public final class StarRocksStreamLoadResponse {

    private static final ObjectMapper JSON_MAPPER = new ObjectMapper();

    private final String status;
    private final String label;
    private final String message;
    private final String errorUrl;
    private final long numberTotalRows;
    private final long numberLoadedRows;
    private final long numberFilteredRows;
    private final String txnId;
    private final String body;

    private StarRocksStreamLoadResponse(
            String status,
            String label,
            String message,
            String errorUrl,
            long numberTotalRows,
            long numberLoadedRows,
            long numberFilteredRows,
            String txnId,
            String body) {
        this.status = status;
        this.label = label;
        this.message = message;
        this.errorUrl = errorUrl;
        this.numberTotalRows = numberTotalRows;
        this.numberLoadedRows = numberLoadedRows;
        this.numberFilteredRows = numberFilteredRows;
        this.txnId = txnId;
        this.body = body;
    }

    public static StarRocksStreamLoadResponse parse(String body) throws IOException {
        if (body == null || body.trim().isEmpty()) {
            throw new IOException("StarRocks Stream Load returned an empty response body");
        }
        JsonNode root = JSON_MAPPER.readTree(body);
        if (root == null || !root.isObject()) {
            throw new IOException("StarRocks Stream Load response is not a JSON object: " + body);
        }
        String status = firstText(root, "Status", "status");
        if (status == null || status.trim().isEmpty()) {
            throw new IOException("StarRocks Stream Load response has no Status: " + body);
        }
        return new StarRocksStreamLoadResponse(
                status,
                firstText(root, "Label", "label"),
                firstText(root, "Message", "message"),
                firstText(root, "ErrorURL", "errorURL", "errorUrl"),
                firstLong(root, "NumberTotalRows", "numberTotalRows"),
                firstLong(root, "NumberLoadedRows", "numberLoadedRows"),
                firstLong(root, "NumberFilteredRows", "numberFilteredRows"),
                firstText(root, "TxnId", "txnId", "TransactionId"),
                body);
    }

    public static StarRocksStreamLoadResponse resolvedCommitted(
            String label,
            String originalBody) {
        return new StarRocksStreamLoadResponse(
                "Success",
                label,
                "Existing Stream Load label is already COMMITTED/VISIBLE",
                null,
                0L,
                0L,
                0L,
                null,
                originalBody);
    }

    public boolean isSuccess() {
        return equalsStatus("Success") || equalsStatus("Publish Timeout");
    }

    public boolean isLabelAlreadyExists() {
        if (equalsStatus("Label Already Exists")) {
            return true;
        }
        return message != null
                && message.contains("Label [")
                && message.contains("has already been used");
    }

    private boolean equalsStatus(String expected) {
        return status != null && expected.equalsIgnoreCase(status.trim());
    }

    private static String firstText(JsonNode root, String... names) {
        for (String name : names) {
            JsonNode value = root.get(name);
            if (value != null && !value.isNull()) {
                return value.asText();
            }
        }
        return null;
    }

    private static long firstLong(JsonNode root, String... names) {
        for (String name : names) {
            JsonNode value = root.get(name);
            if (value != null && !value.isNull()) {
                if (value.isNumber()) {
                    return value.asLong();
                }
                try {
                    return Long.parseLong(value.asText());
                } catch (NumberFormatException ignored) {
                    return 0L;
                }
            }
        }
        return 0L;
    }

    public String getStatus() { return status; }
    public String getLabel() { return label; }
    public String getMessage() { return message; }
    public String getErrorUrl() { return errorUrl; }
    public long getNumberTotalRows() { return numberTotalRows; }
    public long getNumberLoadedRows() { return numberLoadedRows; }
    public long getNumberFilteredRows() { return numberFilteredRows; }
    public String getTxnId() { return txnId; }
    public String getBody() { return body; }
}
