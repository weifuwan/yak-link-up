package com.link.up.connector.http;

import com.link.up.api.source.SourceEnumeratorContext;
import com.link.up.api.source.SourceSplitEnumerator;
import com.link.up.connector.http.converter.HttpResponseParser;
import com.link.up.connector.http.source.HttpSource;
import com.link.up.connector.http.source.HttpSourceReader;
import com.link.up.connector.http.source.HttpSourceSplit;
import com.link.up.connector.http.source.HttpSourceSplitEnumerator;
import org.junit.Test;

import java.io.File;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class HttpConnectorPackageBoundaryTest {

    @Test
    public void responseConversionBelongsToConverterRole() {
        assertEquals(
                "com.link.up.connector.http.converter",
                HttpResponseParser.class.getPackage().getName());
    }

    @Test
    public void readerDelegatesMutablePaginationState() {
        boolean found = false;

        for (Field field : HttpSourceReader.class.getDeclaredFields()) {
            if ("com.link.up.connector.http.source.HttpPaginationState"
                    .equals(field.getType().getName())) {
                found = true;
            }
        }

        assertTrue(
                "HttpSourceReader should delegate pagination state to a focused role",
                found);
    }

    @Test
    public void sourceUsesNativeEnumeratorContract()
            throws Exception {

        Method method =
                HttpSource.class.getDeclaredMethod(
                        "createEnumerator",
                        Map.class,
                        SourceEnumeratorContext.class);

        assertEquals(
                HttpSource.class,
                method.getDeclaringClass());

        SourceSplitEnumerator<HttpSourceSplit> enumerator =
                new HttpSourceSplitEnumerator();

        List<HttpSourceSplit> splits =
                enumerator.enumerateSplits();

        assertEquals(1, splits.size());
        assertEquals(
                "http-split-0",
                splits.get(0).splitId());
        assertEquals(
                "default.default",
                splits.get(0).dataSetId());
    }

    @Test
    public void forbiddenGenericRootPackagesMustNotExist() {
        assertMissingRootPackages(
                "common",
                "core",
                "helper",
                "misc",
                "parser",
                "serializer",
                "utils");
    }

    private void assertMissingRootPackages(
            String... names) {

        File root =
                new File(
                        "src/main/java/com/link/up/connector/http");

        for (String name : names) {
            assertFalse(
                    "Forbidden HTTP connector root package exists: "
                            + name,
                    new File(root, name).exists());
        }
    }
}
