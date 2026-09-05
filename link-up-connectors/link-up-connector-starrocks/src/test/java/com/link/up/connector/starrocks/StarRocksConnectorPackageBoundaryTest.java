package com.link.up.connector.starrocks;

import com.link.up.connector.starrocks.client.sink.StarRocksStreamLoadClient;
import com.link.up.connector.starrocks.client.source.StarRocksBeReadClient;
import com.link.up.connector.starrocks.sink.StarRocksSinkFactory;
import com.link.up.connector.starrocks.source.StarRocksSourceFactory;
import org.junit.Test;

import java.io.File;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class StarRocksConnectorPackageBoundaryTest {

    @Test
    public void nativeSourceAndStreamLoadSinkLiveInStandaloneConnector() {
        assertEquals(
                "com.link.up.connector.starrocks.source",
                StarRocksSourceFactory.class.getPackage().getName());
        assertEquals(
                "com.link.up.connector.starrocks.sink",
                StarRocksSinkFactory.class.getPackage().getName());
        assertTrue(
                StarRocksBeReadClient.class.getName()
                        .startsWith("com.link.up.connector.starrocks."));
        assertTrue(
                StarRocksStreamLoadClient.class.getName()
                        .startsWith("com.link.up.connector.starrocks."));
    }

    @Test
    public void connectorDoesNotCreateJdbcPackageBoundary() {
        File root = new File("src/main/java/com/link/up/connector/starrocks");
        assertFalse(new File(root, "jdbc").exists());
    }

    @Test
    public void forbiddenGenericRootPackagesMustNotExist() {
        File root = new File("src/main/java/com/link/up/connector/starrocks");
        String[] forbidden = {"common", "core", "helper", "misc", "utils"};
        for (String name : forbidden) {
            assertFalse(
                    "Forbidden StarRocks connector root package exists: " + name,
                    new File(root, name).exists());
        }
    }
}
