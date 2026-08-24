package com.link.up.connector.jdbc;

import com.link.up.connector.jdbc.catalog.JdbcCatalogUtils;
import com.link.up.connector.jdbc.client.JdbcConnectionPreflight;
import org.junit.Test;

import java.io.File;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class JdbcConnectorPackageBoundaryTest {

    @Test
    public void catalogAndConnectionHelpersBelongToRolePackages() {
        assertEquals(
                "com.link.up.connector.jdbc.catalog",
                JdbcCatalogUtils.class.getPackage().getName());
        assertEquals(
                "com.link.up.connector.jdbc.client",
                JdbcConnectionPreflight.class.getPackage().getName());
    }

    @Test
    public void splitStatisticsPlanningBelongsToSourceRole()
            throws Exception {

        Class<?> service =
                Class.forName(
                        "com.link.up.connector.jdbc.source.JdbcSplitPlanningService");

        assertEquals(
                "com.link.up.connector.jdbc.source",
                service.getPackage().getName());
    }

    @Test
    public void genericUtilsRootPackageMustNotExist() {
        File root =
                new File(
                        "src/main/java/com/link/up/connector/jdbc");

        assertFalse(
                "JDBC connector must not recreate a generic utils package",
                new File(root, "utils").exists());
        assertFalse(
                "JDBC connector must not add a generic common package",
                new File(root, "common").exists());
        assertFalse(
                "JDBC connector must not add a generic helper package",
                new File(root, "helper").exists());
        assertFalse(
                "JDBC connector must not add a generic misc package",
                new File(root, "misc").exists());
    }

    @Test
    public void legacyCoreMayContainOnlyKnownRoleSubdomains() {
        File core =
                new File(
                        "src/main/java/com/link/up/connector/jdbc/core");

        assertTrue(
                "The current legacy JDBC core directory is expected "
                        + "during incremental migration",
                core.isDirectory());

        File[] children =
                core.listFiles(File::isDirectory);

        Set<String> actual =
                new HashSet<String>();

        if (children != null) {
            for (File child : children) {
                actual.add(child.getName());
            }
        }

        Set<String> allowed =
                new HashSet<String>(
                        Arrays.asList(
                                "converter",
                                "dialect",
                                "split"));

        assertEquals(
                "Do not add new JDBC core subdomains; "
                        + "create a top-level role package instead",
                allowed,
                actual);
    }
}
