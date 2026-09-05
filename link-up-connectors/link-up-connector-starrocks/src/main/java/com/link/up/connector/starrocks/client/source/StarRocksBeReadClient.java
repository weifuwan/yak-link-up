package com.link.up.connector.starrocks.client.source;

import com.link.up.api.table.type.FluxRow;
import com.link.up.api.table.type.FluxRowType;
import com.link.up.connector.starrocks.client.source.model.StarRocksQueryPartition;
import com.link.up.connector.starrocks.config.StarRocksSourceConfig;
import com.link.up.connector.starrocks.converter.StarRocksArrowRowReader;
import com.starrocks.shade.org.apache.thrift.TException;
import com.starrocks.shade.org.apache.thrift.protocol.TBinaryProtocol;
import com.starrocks.shade.org.apache.thrift.protocol.TProtocol;
import com.starrocks.shade.org.apache.thrift.transport.TSocket;
import com.starrocks.thrift.TScanBatchResult;
import com.starrocks.thrift.TScanCloseParams;
import com.starrocks.thrift.TScanNextBatchParams;
import com.starrocks.thrift.TScanOpenParams;
import com.starrocks.thrift.TScanOpenResult;
import com.starrocks.thrift.TStarrocksExternalService;
import com.starrocks.thrift.TStatusCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** Task-local Thrift client for StarRocks BE native scanner. */
public final class StarRocksBeReadClient implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(StarRocksBeReadClient.class);
    private static final String DEFAULT_CLUSTER_NAME = "default_cluster";

    private final StarRocksSourceConfig config;
    private final StarRocksQueryPartition partition;
    private final FluxRowType rowType;

    private TSocket socket;
    private TStarrocksExternalService.Client client;
    private String contextId;
    private int readerOffset;
    private boolean eos;
    private List<FluxRow> currentRows = Collections.emptyList();
    private int currentRowIndex;

    public StarRocksBeReadClient(
            StarRocksSourceConfig config,
            StarRocksQueryPartition partition,
            FluxRowType rowType) {
        this.config = config;
        this.partition = partition;
        this.rowType = rowType;
    }

    public void open() throws Exception {
        String[] hostPort = parseBeAddress(partition.getBeAddress());
        String host = hostPort[0];
        int port = Integer.parseInt(hostPort[1]);

        socket =
                new TSocket(
                        host,
                        port,
                        config.getConnectTimeoutMs(),
                        config.getConnectTimeoutMs());
        socket.open();
        TProtocol protocol = new TBinaryProtocol.Factory().getProtocol(socket);
        client = new TStarrocksExternalService.Client(protocol);

        TScanOpenParams params = new TScanOpenParams();
        params.setTablet_ids(new ArrayList<Long>(partition.getTabletIds()));
        params.setOpaqued_query_plan(partition.getOpaquedQueryPlan());
        params.setCluster(DEFAULT_CLUSTER_NAME);
        params.setDatabase(partition.getDatabase());
        params.setTable(partition.getTable());
        params.setUser(config.getUsername());
        params.setPasswd(config.getPassword());
        params.setBatch_size(config.getBatchRows());
        params.setKeep_alive_min((short) Math.min(Short.MAX_VALUE, config.getKeepAliveMin()));
        params.setQuery_timeout(config.getQueryTimeoutSec());
        params.setMem_limit(config.getMemLimit());
        if (!config.getScanParams().isEmpty()) {
            params.setProperties(config.getScanParams());
        }

        TScanOpenResult result = client.open_scanner(params);
        ensureOk("open_scanner", result == null ? null : result.getStatus());
        if (result == null || result.getContext_id() == null || result.getContext_id().trim().isEmpty()) {
            throw new IOException("StarRocks BE open_scanner returned an empty context id");
        }

        contextId = result.getContext_id();
        readerOffset = 0;
        eos = false;
        currentRows = Collections.emptyList();
        currentRowIndex = 0;

        LOG.info(
                "Opened StarRocks native scanner: table={}.{}, be={}, tablets={}, contextId={}",
                partition.getDatabase(),
                partition.getTable(),
                partition.getBeAddress(),
                partition.getTabletIds().size(),
                contextId);
    }

    public boolean hasNext() throws Exception {
        while (currentRowIndex >= currentRows.size() && !eos) {
            fetchNextBatch();
        }
        return currentRowIndex < currentRows.size();
    }

    public FluxRow next() throws Exception {
        if (!hasNext()) {
            throw new IllegalStateException("No more StarRocks rows in current split");
        }
        return currentRows.get(currentRowIndex++);
    }

    private void fetchNextBatch() throws Exception {
        TScanNextBatchParams params = new TScanNextBatchParams();
        params.setContext_id(contextId);
        params.setOffset(readerOffset);

        TScanBatchResult result = client.get_next(params);
        ensureOk("get_next", result == null ? null : result.getStatus());
        if (result == null) {
            throw new IOException("StarRocks BE get_next returned null result");
        }

        eos = result.isEos();
        byte[] payload = result.getRows();
        currentRows =
                payload == null || payload.length == 0
                        ? Collections.<FluxRow>emptyList()
                        : StarRocksArrowRowReader.read(payload, rowType);
        currentRowIndex = 0;
        readerOffset += currentRows.size();
    }

    private static void ensureOk(String operation, com.starrocks.thrift.TStatus status)
            throws IOException {
        if (status == null || status.getStatus_code() != TStatusCode.OK) {
            throw new IOException(
                    "StarRocks BE "
                            + operation
                            + " failed: status="
                            + (status == null ? "null" : status.getStatus_code())
                            + ", errors="
                            + (status == null ? "[]" : status.getError_msgs()));
        }
    }

    private static String[] parseBeAddress(String address) {
        if (address == null) {
            throw new IllegalArgumentException("BE address must not be null");
        }
        String value = address.trim();
        int separator = value.lastIndexOf(':');
        if (separator <= 0 || separator == value.length() - 1) {
            throw new IllegalArgumentException(
                    "Illegal StarRocks BE address, expected host:port: " + address);
        }
        return new String[]{value.substring(0, separator), value.substring(separator + 1)};
    }

    @Override
    public void close() throws Exception {
        Exception failure = null;
        if (client != null && contextId != null) {
            TScanCloseParams params = new TScanCloseParams();
            params.setContext_id(contextId);
            try {
                client.close_scanner(params);
            } catch (TException e) {
                failure = e;
                LOG.warn(
                        "Failed to close StarRocks scanner: be={}, contextId={}",
                        partition.getBeAddress(),
                        contextId,
                        e);
            }
        }
        if (socket != null) {
            socket.close();
        }
        client = null;
        socket = null;
        contextId = null;
        currentRows = Collections.emptyList();
        currentRowIndex = 0;
        if (failure != null) {
            throw failure;
        }
    }
}
