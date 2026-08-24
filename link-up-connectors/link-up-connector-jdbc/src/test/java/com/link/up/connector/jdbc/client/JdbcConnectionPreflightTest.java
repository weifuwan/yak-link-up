package com.link.up.connector.jdbc.client;

import com.link.up.api.configuration.ReadonlyConfig;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class JdbcConnectionPreflightTest {

    @Test
    public void shouldValidateReachableDatabaseWithoutWriting() throws Exception {
        String url =
                "jdbc:h2:mem:preflight_success;DB_CLOSE_DELAY=-1";

        try (Connection connection =
                     DriverManager.getConnection(url);
             Statement statement =
                     connection.createStatement()) {
            statement.execute(
                    "CREATE TABLE probe_guard(id INT PRIMARY KEY)");
        }

        JdbcConnectionPreflight.validate(
                config(url),
                Thread.currentThread()
                        .getContextClassLoader());

        try (Connection connection =
                     DriverManager.getConnection(url);
             Statement statement =
                     connection.createStatement();
             ResultSet resultSet =
                     statement.executeQuery(
                             "SELECT COUNT(*) FROM probe_guard")) {
            resultSet.next();
            assertEquals(
                    0,
                    resultSet.getInt(1));
        }
    }

    @Test
    public void shouldFailForUnreachableDatabase() throws Exception {
        try {
            JdbcConnectionPreflight.validate(
                    config(
                            "jdbc:h2:tcp://127.0.0.1:1/does-not-exist"),
                    Thread.currentThread()
                            .getContextClassLoader());
            fail("Expected JDBC preflight failure");
        } catch (Exception expected) {
            // Connection refusal or timeout proves the failure is propagated.
        }
    }

    private ReadonlyConfig config(
            String url) {

        Map<String, Object> values =
                new LinkedHashMap<String, Object>();

        values.put(
                "url",
                url);
        values.put(
                "driver",
                "org.h2.Driver");
        values.put(
                "connection_check_timeout_sec",
                Integer.valueOf(2));

        return ReadonlyConfig.fromMap(values);
    }
}
