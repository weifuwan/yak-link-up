package com.link.up.connector.file.s3;

import com.link.up.connector.file.config.FileSourceBaseConfig;
import com.link.up.connector.file.internal.FileEntry;
import com.link.up.connector.file.internal.FileStorage;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

/**
 * S3 storage backed by the AWS SDK v2 client. SDK types stay inside the
 * internal package; the bucket is fixed per instance because one job reads
 * one s3://bucket/prefix path.
 */
public final class S3FileStorage implements FileStorage {

    private final S3Client client;
    private final String bucket;

    public S3FileStorage(S3FileSourceConfig config) {
        this.bucket = Objects.requireNonNull(config.getBucket(), "bucket must not be null");
        this.client = buildClient(config);
    }

    private static S3Client buildClient(S3FileSourceConfig config) {
        S3ClientBuilder builder = S3Client.builder();
        if (config.getEndpoint() != null) {
            builder.endpointOverride(URI.create(config.getEndpoint()));
        }
        builder.region(Region.of(config.getRegion() != null ? config.getRegion() : "us-east-1"));
        builder.forcePathStyle(config.isPathStyleAccess());
        if (config.getAccessKey() != null) {
            builder.credentialsProvider(
                    StaticCredentialsProvider.create(
                            AwsBasicCredentials.create(config.getAccessKey(), config.getSecretKey())));
        } else {
            builder.credentialsProvider(DefaultCredentialsProvider.create());
        }
        return builder.build();
    }

    /** Visible for tests that inject a fake client. */
    S3FileStorage(
            S3Client client,
            String bucket) {

        this.client = Objects.requireNonNull(client, "client must not be null");
        this.bucket = Objects.requireNonNull(bucket, "bucket must not be null");
    }

    @Override
    public List<FileEntry> listFiles(
            String basePath,
            boolean recursive) {

        List<FileEntry> files = new ArrayList<FileEntry>();
        // Exact object wins over prefix listing: "s3://bucket/data" must not
        // drag in "data2.csv" and friends just because they share the prefix.
        if (basePath != null && !basePath.isEmpty() && !basePath.endsWith("/")) {
            try {
                long size = client.headObject(
                        HeadObjectRequest.builder().bucket(bucket).key(basePath).build())
                        .contentLength();
                files.add(new FileEntry(basePath, size));
                return files;
            } catch (NoSuchKeyException missing) {
                // Not an exact object; treat the path as a prefix below.
            }
        }

        String prefix = basePath == null || basePath.isEmpty() ? null : basePath;
        String continuationToken = null;

        do {
            ListObjectsV2Request.Builder request = ListObjectsV2Request.builder()
                    .bucket(bucket)
                    .delimiter(recursive ? null : "/")
                    .prefix(prefix)
                    .maxKeys(1000);
            if (continuationToken != null) {
                request.continuationToken(continuationToken);
            }

            ListObjectsV2Response response = client.listObjectsV2(request.build());
            response.contents().forEach(object -> files.add(
                    new FileEntry(object.key(), object.size())));
            continuationToken = response.isTruncated() ? response.nextContinuationToken() : null;
        } while (continuationToken != null);

        files.sort(Comparator.comparing(FileEntry::getFileKey));
        return files;
    }

    @Override
    public InputStream openRange(
            String fileKey,
            long start,
            long length) {

        GetObjectRequest request = GetObjectRequest.builder()
                .bucket(bucket)
                .key(fileKey)
                .range("bytes=" + start + "-" + (start + length - 1))
                .build();

        ResponseInputStream<GetObjectResponse> content = client.getObject(request);
        return new BoundedObjectStream(content, length, fileKey);
    }

    @Override
    public boolean exists(String fileKey) {
        try {
            client.headObject(
                    HeadObjectRequest.builder()
                            .bucket(bucket)
                            .key(fileKey)
                            .build());
            return true;
        } catch (NoSuchKeyException missing) {
            return false;
        }
    }

    @Override
    public void close() {
        client.close();
    }

    /** Hard stop at the planned range even if the store returns extra bytes. */
    private static final class BoundedObjectStream extends FilterInputStream {

        private final String fileKey;
        private long remaining;

        private BoundedObjectStream(
                InputStream delegate,
                long length,
                String fileKey) {

            super(delegate);
            this.remaining = length;
            this.fileKey = fileKey;
        }

        @Override
        public int read() throws IOException {
            if (remaining <= 0) {
                return -1;
            }
            int read = super.read();
            if (read >= 0) {
                remaining--;
            }
            return read;
        }

        @Override
        public int read(byte[] target, int offset, int count) throws IOException {
            if (remaining <= 0) {
                return -1;
            }
            int read = super.read(target, offset, (int) Math.min(count, remaining));
            if (read > 0) {
                remaining -= read;
            }
            return read;
        }

        @Override
        public void close() throws IOException {
            try {
                super.close();
            } finally {
                remaining = 0;
            }
        }

        @Override
        public String toString() {
            return "BoundedObjectStream{fileKey='" + fileKey + "'}";
        }
    }
}
