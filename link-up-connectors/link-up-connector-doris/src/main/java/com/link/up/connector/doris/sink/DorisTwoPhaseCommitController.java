package com.link.up.connector.doris.sink;

import com.link.up.connector.doris.client.DorisStreamLoadClient;
import com.link.up.connector.doris.client.DorisStreamLoadClient.StreamLoadResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/** Owns Doris task-local 2PC transaction state for one sink writer. */
final class DorisTwoPhaseCommitController {

    private static final Logger LOG =
            LoggerFactory.getLogger(
                    DorisTwoPhaseCommitController.class);

    private final DorisStreamLoadClient client;
    private final boolean enabled;
    private final List<String> pendingTransactionIds =
            new ArrayList<String>();

    DorisTwoPhaseCommitController(
            DorisStreamLoadClient client,
            boolean enabled) {

        this.client = Objects.requireNonNull(
                client,
                "client must not be null");
        this.enabled = enabled;
    }

    void record(StreamLoadResponse response)
            throws IOException {

        Objects.requireNonNull(
                response,
                "response must not be null");

        if (!enabled) {
            return;
        }

        String transactionId = response.getTxnId();

        if (transactionId == null
                || transactionId.trim().isEmpty()) {
            throw new IOException(
                    "2PC enabled but Doris returned no TxnId. Response: "
                            + response.getBody());
        }

        if (!response.isPrepared()) {
            throw new IOException(
                    "2PC enabled but TxnState is not PREPARE: txnId="
                            + transactionId
                            + ", txnState="
                            + response.getTxnState()
                            + ", response="
                            + response.getBody());
        }

        pendingTransactionIds.add(transactionId);

        LOG.debug(
                "Collected Doris 2PC transaction: txnId={}, pendingCount={}",
                transactionId,
                pendingTransactionIds.size());
    }

    void commit() throws IOException {
        if (!enabled || pendingTransactionIds.isEmpty()) {
            return;
        }

        int count = pendingTransactionIds.size();

        LOG.info(
                "Committing {} pending Doris 2PC transactions",
                count);

        try {
            client.commitTransactions(pendingTransactionIds);
        } catch (IOException failure) {
            LOG.error(
                    "Doris 2PC commit failed; {} transactions may be in an inconsistent state",
                    count,
                    failure);

            throw new IOException(
                    "Doris 2PC commit failed with "
                            + count
                            + " pending transactions. Some may have been committed. "
                            + "Check Doris transaction state before retrying.",
                    failure);
        }

        pendingTransactionIds.clear();

        LOG.info(
                "Committed {} Doris 2PC transactions",
                count);
    }

    void abort() {
        if (!enabled || pendingTransactionIds.isEmpty()) {
            return;
        }

        int count = pendingTransactionIds.size();

        LOG.warn(
                "Aborting {} pending Doris 2PC transactions",
                count);

        client.abortTransactions(pendingTransactionIds);
        pendingTransactionIds.clear();

        LOG.info(
                "Aborted {} Doris 2PC transactions",
                count);
    }

    void close() {
        if (!enabled || pendingTransactionIds.isEmpty()) {
            return;
        }

        LOG.warn(
                "Closing Doris sink with {} uncommitted 2PC transactions",
                pendingTransactionIds.size());

        abort();
    }

    String retryAdvice() {
        return enabled
                ? "Doris 2PC mode: data is PREPARED but not committed. "
                        + "Safe to retry — uncommitted transactions will be aborted."
                : "Doris Stream Load commits per batch; "
                        + "verify already loaded data before retrying.";
    }

    boolean isEnabled() {
        return enabled;
    }

    int pendingCount() {
        return pendingTransactionIds.size();
    }
}
