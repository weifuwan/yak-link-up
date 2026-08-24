package com.link.up.connector.doris;

import com.link.up.connector.doris.converter.DorisRowSerializer;
import org.junit.Test;

import java.io.File;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class DorisConnectorPackageBoundaryTest {

    @Test
    public void rowSerializationBelongsToConverterRole() {
        assertEquals(
                "com.link.up.connector.doris.converter",
                DorisRowSerializer.class.getPackage().getName());
    }

    @Test
    public void forbiddenGenericRootPackagesMustNotExist() {
        File root =
                new File(
                        "src/main/java/com/link/up/connector/doris");

        String[] forbidden = {
                "common",
                "core",
                "helper",
                "misc",
                "parser",
                "serializer",
                "utils"
        };

        for (String name : forbidden) {
            assertFalse(
                    "Forbidden Doris connector root package exists: " + name,
                    new File(root, name).exists());
        }
    }
}
