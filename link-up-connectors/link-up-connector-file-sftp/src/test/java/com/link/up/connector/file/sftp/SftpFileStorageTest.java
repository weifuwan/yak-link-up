package com.link.up.connector.file.sftp;

import com.link.up.api.configuration.ReadonlyConfig;
import com.link.up.connector.file.config.FileSourceBaseConfig;
import com.link.up.connector.file.sftp.SftpFileSourceConfig;
import org.apache.sshd.common.file.virtualfs.VirtualFileSystemFactory;
import com.link.up.connector.file.internal.FileEntry;
import org.apache.sshd.server.SshServer;
import org.apache.sshd.server.keyprovider.SimpleGeneratorHostKeyProvider;
import org.apache.sshd.sftp.server.SftpSubsystemFactory;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Runs the storage against a real in-JVM SFTP server (Apache MINA SSHD), so
 * list/range/exists are validated over the actual wire protocol without any
 * external environment.
 */
public class SftpFileStorageTest {

    private static final String USER = "sync";
    private static final String PASSWORD = "secret";

    private static SshServer server;
    private static int port;
    private static Path root;

    @BeforeClass
    public static void startServer() throws Exception {
        root = Files.createTempDirectory("sftp-it");

        Files.write(root.resolve("a.csv"), "id,city\n1,hz\n".getBytes(StandardCharsets.UTF_8));
        Files.createDirectories(root.resolve("sub"));
        Files.write(root.resolve("sub/b.csv"), "row-b\n".getBytes(StandardCharsets.UTF_8));

        server = SshServer.setUpDefaultServer();
        server.setPort(0);
        SimpleGeneratorHostKeyProvider keyProvider = new SimpleGeneratorHostKeyProvider();
        keyProvider.setAlgorithm("RSA");
        keyProvider.setKeySize(2048);
        server.setKeyPairProvider(keyProvider);
        server.setPasswordAuthenticator(
                (username, password, session) ->
                        USER.equals(username) && PASSWORD.equals(password));
        VirtualFileSystemFactory fsFactory = new VirtualFileSystemFactory();
        fsFactory.setDefaultHomeDir(root);
        server.setFileSystemFactory(fsFactory);
        server.setSubsystemFactories(Collections.singletonList(new SftpSubsystemFactory()));
        server.start();
        port = server.getPort();
    }

    @AfterClass
    public static void stopServer() throws Exception {
        if (server != null) {
            server.stop(true);
        }
    }

    @Test
    public void shouldListFilesRecursivelySorted() {
        SftpFileStorage storage = storage();

        List<FileEntry> files = storage.listFiles("/", true);

        assertEquals(2, files.size());
        assertEquals("/a.csv", files.get(0).getFileKey());
        assertEquals("/sub/b.csv", files.get(1).getFileKey());
        assertEquals(13L, files.get(0).getSize());
        storage.close();
    }

    @Test
    public void shouldListFlatWithoutSubdirectories() {
        SftpFileStorage storage = storage();

        List<FileEntry> files = storage.listFiles("/", false);

        assertEquals(1, files.size());
        assertEquals("/a.csv", files.get(0).getFileKey());
        storage.close();
    }

    @Test
    public void shouldOpenMiddleRange() throws Exception {
        SftpFileStorage storage = storage();

        try (InputStream input = storage.openRange("/a.csv", 3L, 4L)) {
            byte[] bytes = new byte[4];
            int read = 0;
            while (read < 4) {
                int chunk = input.read(bytes, read, 4 - read);
                if (chunk < 0) {
                    break;
                }
                read += chunk;
            }
            assertEquals("city", new String(bytes, 0, read, StandardCharsets.UTF_8));
        }
        storage.close();
    }

    @Test
    public void shouldCheckExistence() {
        SftpFileStorage storage = storage();

        assertTrue(storage.exists("/a.csv"));
        assertFalse(storage.exists("/missing.csv"));
        storage.close();
    }

    private SftpFileStorage storage() {
        // text keeps the config minimal: no schema/columns are needed to
        // exercise the storage layer itself.
        Map<String, Object> values = new java.util.LinkedHashMap<>();
        values.put("path", "/");
        values.put("storage_type", "sftp");
        values.put("format", "text");
        values.put("table_name", "sftp_root");
        values.put("host", "localhost");
        values.put("port", port);
        values.put("user", USER);
        values.put("password", PASSWORD);

        SftpFileSourceConfig config = SftpFileSourceConfig.of(ReadonlyConfig.fromMap(values));
        return new SftpFileStorage(config);
    }
}
