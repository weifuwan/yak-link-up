package com.link.up.connector.file.s3;

import com.link.up.api.configuration.ReadonlyConfig;
import com.link.up.connector.file.config.FileSourceBaseConfig;
import com.link.up.connector.file.internal.FileStorage;
import com.link.up.connector.file.internal.FileStorageFactory;

import java.io.Serializable;
import java.util.Locale;
import java.util.Objects;

/**
 * S3 member configuration of the file family: the shared base config plus the
 * transport options. The path may carry the bucket as an {@code s3://}
 * scheme, or the bucket is configured separately.
 */
public final class S3FileSourceConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    private final FileSourceBaseConfig base;
    private final String bucket;
    private final String prefix;
    private final String endpoint;
    private final String region;
    private final String accessKey;
    private final String secretKey;
    private final boolean pathStyleAccess;

    private S3FileSourceConfig(
            FileSourceBaseConfig base,
            String bucket,
            String prefix,
            String endpoint,
            String region,
            String accessKey,
            String secretKey,
            boolean pathStyleAccess) {

        this.base = base;
        this.bucket = bucket;
        this.prefix = prefix;
        this.endpoint = endpoint;
        this.region = region;
        this.accessKey = accessKey;
        this.secretKey = secretKey;
        this.pathStyleAccess = pathStyleAccess;
    }

    public static S3FileSourceConfig of(ReadonlyConfig config) {
        Objects.requireNonNull(config, "config must not be null");
        FileSourceBaseConfig rawBase = FileSourceBaseConfig.of(config);

        String rawPath = rawBase.getPath();
        String bucket;
        String prefix;
        String lowered = rawPath.toLowerCase(Locale.ROOT);
        if (lowered.startsWith("s3://")) {
            String withoutScheme = rawPath.substring("s3://".length());
            int slash = withoutScheme.indexOf('/');
            bucket = slash < 0 ? withoutScheme : withoutScheme.substring(0, slash);
            prefix = slash < 0 ? "" : withoutScheme.substring(slash + 1);

            String optionBucket = config.getOptional(S3FileSourceOptions.BUCKET)
                    .map(String::trim)
                    .orElse(null);
            if (optionBucket != null && !optionBucket.equals(bucket)) {
                throw new IllegalArgumentException(
                        "bucket '" + optionBucket + "' conflicts with the bucket in the s3 path");
            }
        } else {
            bucket = config.getOptional(S3FileSourceOptions.BUCKET)
                    .map(String::trim)
                    .orElseThrow(() -> new IllegalArgumentException(
                            "bucket is required when the path is not an s3:// location"));
            prefix = rawPath;
        }
        if (bucket.isEmpty()) {
            throw new IllegalArgumentException("bucket must not be empty");
        }

        String endpoint = config.getOptional(S3FileSourceOptions.ENDPOINT)
                .map(String::trim)
                .orElse(null);
        if (endpoint != null
                && !endpoint.toLowerCase(Locale.ROOT).startsWith("http://")
                && !endpoint.toLowerCase(Locale.ROOT).startsWith("https://")) {
            throw new IllegalArgumentException("endpoint must start with http:// or https://");
        }

        String region = config.getOptional(S3FileSourceOptions.REGION)
                .map(String::trim)
                .orElse(null);
        if (region == null && endpoint == null) {
            throw new IllegalArgumentException(
                    "region is required for AWS S3 endpoints; configure region or a custom endpoint");
        }

        String accessKey = config.getOptional(S3FileSourceOptions.ACCESS_KEY)
                .map(String::trim)
                .orElse(null);
        String secretKey = config.getOptional(S3FileSourceOptions.SECRET_KEY)
                .map(String::trim)
                .orElse(null);
        if ((accessKey == null) != (secretKey == null)) {
            throw new IllegalArgumentException("access_key and secret_key must be configured together");
        }

        boolean pathStyleAccess = config.get(S3FileSourceOptions.PATH_STYLE_ACCESS);

        // The engine-facing path is the object-key prefix, not the s3:// URL.
        FileSourceBaseConfig base = rawBase.withPath(prefix);
        return new S3FileSourceConfig(base, bucket, prefix, endpoint, region, accessKey, secretKey, pathStyleAccess);
    }

    public FileSourceBaseConfig getBase() {
        return base;
    }

    public String getBucket() {
        return bucket;
    }

    public String getPrefix() {
        return prefix;
    }

    public String getEndpoint() {
        return endpoint;
    }

    public String getRegion() {
        return region;
    }

    public String getAccessKey() {
        return accessKey;
    }

    public String getSecretKey() {
        return secretKey;
    }

    public boolean isPathStyleAccess() {
        return pathStyleAccess;
    }

    FileStorageFactory createFactory() {
        return this::createStorage;
    }

    FileStorage createStorage() {
        return new S3FileStorage(this);
    }
}
