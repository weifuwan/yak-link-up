package com.link.up.connector.jdbc.client;

import com.link.up.api.configuration.ReadonlyConfig;
import com.link.up.connector.jdbc.config.JdbcConnectionConfig;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLException;
import java.util.Objects;
import java.util.Properties;

/**
 * JDBC read-only connection preflight.
 *
 * <p>Only loads the configured driver, opens a connection and calls
 * {@link Connection#isValid(int)}. It does not execute SQL, start a
 * transaction or modify database objects.</p>
 */
public final class JdbcConnectionPreflight {

    private JdbcConnectionPreflight() {
    }

    public static void validate(
            ReadonlyConfig options,
            ClassLoader classLoader)
            throws Exception {

        Objects.requireNonNull(
                options,
                "options must not be null");

        ClassLoader loader =
                Objects.requireNonNull(
                        classLoader,
                        "classLoader must not be null");

        JdbcConnectionConfig config =
                JdbcConnectionConfig.of(options);

        Class<?> driverType =
                Class.forName(
                        config.getDriverName(),
                        true,
                        loader);

        if (!Driver.class.isAssignableFrom(driverType)) {
            throw new IllegalArgumentException(
                    "Configured JDBC driver does not implement java.sql.Driver: "
                            + config.getDriverName());
        }

        Driver driver =
                (Driver) driverType
                        .getDeclaredConstructor()
                        .newInstance();

        Properties properties = config.toProperties();

        Connection connection =
                driver.connect(
                        config.getUrl(),
                        properties);

        if (connection == null) {
            throw new SQLException(
                    "JDBC driver rejected URL: "
                            + config.getUrl());
        }

        try {
            int timeout =
                    Math.max(
                            1,
                            config.getConnectionCheckTimeoutSeconds());

            if (!connection.isValid(timeout)) {
                throw new SQLException(
                        "JDBC connection validation returned false");
            }
        } finally {
            connection.close();
        }
    }
}
