package com.link.up.connector.file.sftp;

import com.link.up.api.configuration.ReadonlyConfig;
import com.link.up.connector.file.config.FileSourceBaseConfig;
import com.link.up.connector.file.internal.FileStorage;
import com.link.up.connector.file.internal.FileStorageFactory;

import java.io.Serializable;
import java.util.Objects;

/**
 * SFTP member configuration of the file family: the shared base config plus
 * the transport options. The path is an absolute remote directory or file.
 */
public final class SftpFileSourceConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    private final FileSourceBaseConfig base;
    private final String host;
    private final int port;
    private final String user;
    private final String password;
    private final String privateKey;
    private final boolean strictHostKeyChecking;

    private SftpFileSourceConfig(
            FileSourceBaseConfig base,
            String host,
            int port,
            String user,
            String password,
            String privateKey,
            boolean strictHostKeyChecking) {

        this.base = base;
        this.host = host;
        this.port = port;
        this.user = user;
        this.password = password;
        this.privateKey = privateKey;
        this.strictHostKeyChecking = strictHostKeyChecking;
    }

    public static SftpFileSourceConfig of(ReadonlyConfig config) {
        Objects.requireNonNull(config, "config must not be null");
        FileSourceBaseConfig base = FileSourceBaseConfig.of(config);

        String path = base.getPath();
        String lowered = path.toLowerCase(java.util.Locale.ROOT);
        if (lowered.startsWith("sftp://")) {
            path = path.substring("sftp://".length());
        }
        if (!path.startsWith("/")) {
            throw new IllegalArgumentException(
                    "sftp path must be absolute, but was: " + path);
        }

        String host = requireText(config, SftpFileSourceOptions.HOST, "host");
        String user = requireText(config, SftpFileSourceOptions.USER, "user");

        int port = config.get(SftpFileSourceOptions.PORT);
        if (port <= 0 || port > 65535) {
            throw new IllegalArgumentException(
                    "port must be within 1..65535, but was: " + port);
        }

        String password = config.getOptional(SftpFileSourceOptions.PASSWORD)
                .map(String::trim)
                .orElse(null);
        String privateKey = config.getOptional(SftpFileSourceOptions.PRIVATE_KEY)
                .map(String::trim)
                .orElse(null);
        if (password == null && privateKey == null) {
            throw new IllegalArgumentException(
                    "sftp requires an authentication method: configure password or private_key");
        }

        boolean strict = config.get(SftpFileSourceOptions.STRICT_HOST_KEY_CHECKING);

        FileSourceBaseConfig resolvedBase = path.equals(base.getPath())
                ? base
                : base.withPath(path);
        return new SftpFileSourceConfig(resolvedBase, host, port, user, password, privateKey, strict);
    }

    private static String requireText(
            ReadonlyConfig config,
            com.link.up.api.configuration.Option<String> option,
            String label) {

        String value = config.get(option);
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalArgumentException(label + " must not be blank");
        }
        return value.trim();
    }

    public FileSourceBaseConfig getBase() {
        return base;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public String getUser() {
        return user;
    }

    public String getPassword() {
        return password;
    }

    public String getPrivateKey() {
        return privateKey;
    }

    public boolean isStrictHostKeyChecking() {
        return strictHostKeyChecking;
    }

    public FileStorageFactory createFactory() {
        return this::createStorage;
    }

    public FileStorage createStorage() {
        return new SftpFileStorage(this);
    }
}
