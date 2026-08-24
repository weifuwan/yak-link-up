package com.link.up.connector.doris;

import com.link.up.connector.doris.converter.DorisRowSerializer;
import com.link.up.connector.doris.sink.DorisSinkWriter;
import org.junit.Test;

import java.io.File;
import java.lang.reflect.Field;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class DorisConnectorPackageBoundaryTest {

    @Test
    public void rowSerializationBelongsToConverterRole() {
        assertEquals(
                "com.link.up.connector.doris.converter",
                DorisRowSerializer.class.getPackage().getName());
    }

    @Test
    public void sinkWriterDelegatesTwoPhaseCommitState() {
        boolean found = false;

        for (Field field : DorisSinkWriter.class.getDeclaredFields()) {
            if ("com.link.up.connector.doris.sink.DorisTwoPhaseCommitController"
                    .equals(field.getType().getName())) {
                found = true;
            }
        }

        assertTrue(
                "DorisSinkWriter should delegate 2PC state to a focused role",
                found);
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
                    "Forbidden Doris connector root package exists: "
                            + name,
                    new File(root, name).exists());
        }
    }
}
