package com.link.up.connector.file.sftp;

import com.link.up.api.configuration.Option;
import com.link.up.api.configuration.Options;
import com.link.up.api.connector.schema.ConnectorOptionScope;

/** SFTP-specific options of the file family. */
public final class SftpFileSourceOptions {

    private SftpFileSourceOptions() {
    }

    public static final Option<String> HOST =
            Options.key("host")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("SFTP server host")
                    .withSemanticType("SFTP_HOST")
                    .withScope(ConnectorOptionScope.DATASOURCE);

    public static final Option<Integer> PORT =
            Options.key("port")
                    .intType()
                    .defaultValue(22)
                    .withDescription("SFTP server port")
                    .withSemanticType("SFTP_PORT")
                    .withScope(ConnectorOptionScope.DATASOURCE);

    public static final Option<String> USER =
            Options.key("user")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("SFTP login user")
                    .withSemanticType("SFTP_USER")
                    .withScope(ConnectorOptionScope.DATASOURCE);

    public static final Option<String> PASSWORD =
            Options.key("password")
                    .stringType()
                    .noDefaultValue()
                    .sensitive()
                    .withDescription("SFTP password; one of password/private_key is required")
                    .withSemanticType("SFTP_PASSWORD")
                    .withScope(ConnectorOptionScope.DATASOURCE);

    public static final Option<String> PRIVATE_KEY =
            Options.key("private_key")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Path to the SSH identity file; one of password/private_key is required")
                    .withSemanticType("SFTP_PRIVATE_KEY")
                    .withScope(ConnectorOptionScope.DATASOURCE);

    public static final Option<Boolean> STRICT_HOST_KEY_CHECKING =
            Options.key("strict_host_key_checking")
                    .booleanType()
                    .defaultValue(Boolean.FALSE)
                    .withDescription("Verify the server host key against known_hosts; disabled by default")
                    .withSemanticType("SFTP_HOST_KEY")
                    .withScope(ConnectorOptionScope.DATASOURCE);
}
