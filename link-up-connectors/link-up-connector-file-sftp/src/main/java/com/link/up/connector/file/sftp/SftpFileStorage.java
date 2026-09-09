package com.link.up.connector.file.sftp;

import com.link.up.connector.file.config.FileSourceBaseConfig;
import com.link.up.connector.file.internal.FileEntry;
import com.link.up.connector.file.internal.FileStorage;

import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.SftpException;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Vector;

/**
 * SFTP storage backed by JSch. The session and its single SFTP channel are
 * owned by this instance and disconnected on close; vendor types stay inside
 * the internal package.
 *
 * <p>{@link #openRange} streams the whole remote file and discards up to the
 * requested offset — JSch exposes no server-side seek on the InputStream
 * variant of {@code get}. Correct for the alignment-window reads and standard
 * split sizes; revisit only if huge offsets become a measured bottleneck.
 */
public final class SftpFileStorage implements FileStorage {

    private static final int CONNECT_TIMEOUT_MILLIS = 30_000;

    private final SftpFileSourceConfig config;
    private com.jcraft.jsch.Session session;
    private ChannelSftp channel;

    public SftpFileStorage(SftpFileSourceConfig config) {
        this.config = config;
        connect();
    }

    private void connect() {
        try {
            JSch jsch = new JSch();
            if (config.getPrivateKey() != null) {
                jsch.addIdentity(config.getPrivateKey());
            }
            session = jsch.getSession(config.getUser(), config.getHost(), config.getPort());
            if (config.getPassword() != null) {
                session.setPassword(config.getPassword());
            }
            session.setConfig("StrictHostKeyChecking",
                    config.isStrictHostKeyChecking() ? "yes" : "no");
            session.connect(CONNECT_TIMEOUT_MILLIS);

            channel = (ChannelSftp) session.openChannel("sftp");
            channel.connect(CONNECT_TIMEOUT_MILLIS);
        } catch (JSchException failure) {
            close();
            throw new IllegalArgumentException(
                    "Could not connect to SFTP " + config.getHost() + ":" + config.getPort()
                            + " as user " + config.getUser(),
                    failure);
        }
    }

    @Override
    public List<FileEntry> listFiles(String basePath, boolean recursive) {
        List<FileEntry> files = new ArrayList<FileEntry>();
        collect(files, normalizeBase(basePath), recursive);
        files.sort((left, right) -> left.getFileKey().compareTo(right.getFileKey()));
        return Collections.unmodifiableList(files);
    }

    private void collect(List<FileEntry> files, String dir, boolean recursive) {
        try {
            Vector<ChannelSftp.LsEntry> entries = channel.ls(dir);
            for (ChannelSftp.LsEntry entry : entries) {
                String name = entry.getFilename();
                if (".".equals(name) || "..".equals(name)) {
                    continue;
                }
                String child = join(dir, name);
                if (entry.getAttrs().isDir()) {
                    if (recursive) {
                        collect(files, child, true);
                    }
                    continue;
                }
                files.add(new FileEntry(child, entry.getAttrs().getSize()));
            }
        } catch (SftpException failure) {
            throw new IllegalArgumentException(
                    "Could not list SFTP directory '" + dir + "' (sftp code " + failure.id + ")",
                    failure);
        }
    }

    @Override
    public InputStream openRange(String fileKey, long start, long length) {
        InputStream raw;
        try {
            raw = channel.get(fileKey);
        } catch (SftpException failure) {
            throw new IllegalArgumentException(
                    "Could not open SFTP file '" + fileKey + "' (sftp code " + failure.id + ")",
                    failure);
        }
        return new BoundedSkipStream(raw, start, length, fileKey);
    }

    @Override
    public boolean wholeFileOnly() {
        return true;
    }

    @Override
    public boolean exists(String fileKey) {
        try {
            channel.stat(fileKey);
            return true;
        } catch (SftpException failure) {
            if (failure.id == ChannelSftp.SSH_FX_NO_SUCH_FILE) {
                return false;
            }
            throw new IllegalArgumentException(
                    "Could not stat SFTP file '" + fileKey + "' (sftp code " + failure.id + ")",
                    failure);
        }
    }

    @Override
    public void close() {
        if (channel != null) {
            channel.disconnect();
            channel = null;
        }
        if (session != null) {
            session.disconnect();
            session = null;
        }
    }

    private static String normalizeBase(String basePath) {
        if (basePath == null || basePath.isEmpty()) {
            return "/";
        }
        return basePath;
    }

    private static String join(String dir, String name) {
        return dir.endsWith("/") ? dir + name : dir + "/" + name;
    }

    /**
     * Discards the requested offset with bulk reads (InputStream.skip may be
     * byte-wise on the JSch stream), then hard-stops after the planned length.
     */
    static final class BoundedSkipStream extends InputStream {

        private final InputStream delegate;
        private final String fileKey;
        private long toSkip;
        private long remaining;
        private boolean positioned;

        BoundedSkipStream(
                InputStream delegate,
                long start,
                long length,
                String fileKey) {

            this.delegate = delegate;
            this.toSkip = start;
            this.remaining = length;
            this.fileKey = fileKey;
        }

        @Override
        public int read() throws IOException {
            position();
            if (remaining <= 0) {
                return -1;
            }
            int read = delegate.read();
            if (read >= 0) {
                remaining--;
            }
            return read;
        }

        @Override
        public int read(byte[] target, int offset, int count) throws IOException {
            position();
            if (remaining <= 0) {
                return -1;
            }
            int read = delegate.read(target, offset, (int) Math.min(count, remaining));
            if (read > 0) {
                remaining -= read;
            }
            return read;
        }

        @Override
        public void close() throws IOException {
            delegate.close();
        }

        private void position() throws IOException {
            if (positioned) {
                return;
            }
            positioned = true;
            byte[] discard = new byte[64 * 1024];
            while (toSkip > 0) {
                int read = delegate.read(discard, 0, (int) Math.min(discard.length, toSkip));
                if (read < 0) {
                    throw new IOException(
                            "SFTP file ended before the requested offset: " + fileKey);
                }
                toSkip -= read;
            }
        }
    }
}
