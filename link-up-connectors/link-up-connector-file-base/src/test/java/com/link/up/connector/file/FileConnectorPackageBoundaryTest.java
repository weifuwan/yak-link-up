package com.link.up.connector.file;

import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/** Locks the module boundaries documented in FILE_SOURCE_DESIGN.md. */
public class FileConnectorPackageBoundaryTest {

    @Test
    public void sdkTypesStayInsideInternalPackage() throws IOException {
        Path root = Paths.get("src/main/java/com/link/up/connector/file");

        try (Stream<Path> sources = Files.walk(root)) {
            for (Path source : sources.filter(path -> sourceName(path).endsWith(".java"))
                    .collect(java.util.stream.Collectors.toList())) {
                String relative = root.relativize(source).toString();
                if (relative.replace('\\', '/').startsWith("internal/")) {
                    continue;
                }
                String content = new String(Files.readAllBytes(source), StandardCharsets.UTF_8.name());
                assertFalse(
                        "AWS SDK imports must stay inside the internal package: " + relative,
                        content.contains("software.amazon.awssdk"));
                assertFalse(
                        "JSch imports must stay inside the internal package: " + relative,
                        content.contains("com.jcraft.jsch"));
            }
        }
    }

    @Test
    public void noGarbagePackagesAllowed() {
        File root = new File("src/main/java/com/link/up/connector/file");

        assertFalse(new File(root, "common").exists());
        assertFalse(new File(root, "utils").exists());
        assertFalse(new File(root, "helper").exists());
        assertFalse(new File(root, "misc").exists());
    }

    @Test
    public void rolePackagesExist() {
        File root = new File("src/main/java/com/link/up/connector/file");

        assertTrue(new File(root, "config").isDirectory());
        assertTrue(new File(root, "source").isDirectory());
        assertTrue(new File(root, "converter").isDirectory());
        assertTrue(new File(root, "schema").isDirectory());
        assertTrue(new File(root, "internal").isDirectory());
    }

    @Test
    public void storageImplementationsLiveInInternalPackage() {
        assertEquals("com.link.up.connector.file.internal",
                com.link.up.connector.file.internal.LocalFileStorage.class.getPackage().getName());
    }

    private static String sourceName(Path path) {
        Path fileName = path.getFileName();
        return fileName == null ? "" : fileName.toString();
    }
}
