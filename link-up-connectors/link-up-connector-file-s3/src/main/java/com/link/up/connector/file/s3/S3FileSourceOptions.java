package com.link.up.connector.file.s3;

import com.link.up.api.configuration.Option;
import com.link.up.api.configuration.Options;
import com.link.up.api.connector.schema.ConnectorOptionScope;

/** S3-specific options of the file family. */
public final class S3FileSourceOptions {

    private S3FileSourceOptions() {
    }

    public static final Option<String> ENDPOINT =
            Options.key("endpoint")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("S3-compatible endpoint, e.g. MinIO/OSS/COS")
                    .withSemanticType("S3_ENDPOINT")
                    .withScope(ConnectorOptionScope.DATASOURCE);

    public static final Option<String> BUCKET =
            Options.key("bucket")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("S3 bucket; may be carried by an s3:// path instead")
                    .withSemanticType("S3_BUCKET")
                    .withScope(ConnectorOptionScope.DATASOURCE);

    public static final Option<String> ACCESS_KEY =
            Options.key("access_key")
                    .stringType()
                    .noDefaultValue()
                    .sensitive()
                    .withDescription("S3 access key; falls back to the SDK default credential chain when absent")
                    .withSemanticType("ACCESS_KEY")
                    .withScope(ConnectorOptionScope.DATASOURCE);

    public static final Option<String> SECRET_KEY =
            Options.key("secret_key")
                    .stringType()
                    .noDefaultValue()
                    .sensitive()
                    .withDescription("S3 secret key; falls back to the SDK default credential chain when absent")
                    .withSemanticType("SECRET_KEY")
                    .withScope(ConnectorOptionScope.DATASOURCE);

    public static final Option<String> REGION =
            Options.key("region")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("S3 region; required for AWS endpoints, optional for custom endpoints")
                    .withSemanticType("S3_REGION")
                    .withScope(ConnectorOptionScope.DATASOURCE);

    public static final Option<Boolean> PATH_STYLE_ACCESS =
            Options.key("path_style_access")
                    .booleanType()
                    .defaultValue(Boolean.FALSE)
                    .withDescription("Use path-style addressing, required by MinIO and similar stores")
                    .withSemanticType("S3_PATH_STYLE")
                    .withScope(ConnectorOptionScope.DATASOURCE);
}
