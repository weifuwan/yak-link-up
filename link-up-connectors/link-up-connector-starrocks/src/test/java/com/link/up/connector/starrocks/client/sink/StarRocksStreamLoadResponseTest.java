package com.link.up.connector.starrocks.client.sink;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class StarRocksStreamLoadResponseTest {

    @Test
    public void parsesSuccessResponse() throws Exception {
        StarRocksStreamLoadResponse response =
                StarRocksStreamLoadResponse.parse(
                        "{\"Status\":\"Success\",\"Label\":\"l1\","
                                + "\"NumberTotalRows\":3,\"NumberLoadedRows\":3,"
                                + "\"NumberFilteredRows\":0,\"TxnId\":42}");

        assertTrue(response.isSuccess());
        assertFalse(response.isLabelAlreadyExists());
        assertEquals("l1", response.getLabel());
        assertEquals(3L, response.getNumberLoadedRows());
        assertEquals("42", response.getTxnId());
    }

    @Test
    public void publishTimeoutIsSuccessfulStreamLoadOutcome() throws Exception {
        StarRocksStreamLoadResponse response =
                StarRocksStreamLoadResponse.parse(
                        "{\"Status\":\"Publish Timeout\",\"Label\":\"l2\"}");
        assertTrue(response.isSuccess());
    }

    @Test
    public void detectsCurrentLabelReuseFromLegacyFailureMessage() throws Exception {
        StarRocksStreamLoadResponse response =
                StarRocksStreamLoadResponse.parse(
                        "{\"Status\":\"Fail\",\"Label\":\"l3\","
                                + "\"Message\":\"Label [l3] has already been used\"}");
        assertTrue(response.isLabelAlreadyExists());
    }
}
