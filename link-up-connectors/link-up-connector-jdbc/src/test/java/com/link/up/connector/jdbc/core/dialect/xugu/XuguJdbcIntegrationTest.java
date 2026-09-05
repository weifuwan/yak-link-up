package com.link.up.connector.jdbc.core.dialect.xugu;

import com.link.up.api.configuration.ReadonlyConfig;
import com.link.up.api.table.catalog.CatalogTable;
import com.link.up.api.table.catalog.Column;
import com.link.up.api.table.catalog.PrimaryKey;
import com.link.up.api.table.catalog.TablePath;
import com.link.up.api.table.catalog.TableSchema;
import com.link.up.api.table.type.BasicType;
import com.link.up.connector.jdbc.catalog.JdbcCatalogConfig;
import com.link.up.connector.jdbc.catalog.xugu.XuguCatalog;
import com.link.up.connector.jdbc.config.JdbcConnectionConfig;
import com.link.up.connector.jdbc.internal.JdbcConnectionProvider;
import org.junit.Assume;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Opt-in integration coverage for a real XuguDB instance.
 *
 * <p>The test is skipped unless {@code XUGU_JDBC_URL} is present. Typical
 * environment variables:</p>
 *
 * <pre>
 * XUGU_JDBC_URL=jdbc:xugu://127.0.0.1:5138/SYSTEM
 * XUGU_JDBC_USER=SYSDBA
 * XUGU_JDBC_PASSWORD=...
 * XUGU_JDBC_SCHEMA=SYSDBA
 * </pre>
 */
public class XuguJdbcIntegrationTest {

    private static final String DRIVER = "com.xugu.cloudjdbc.Driver";

    @Test
    public void catalogMetadataMergeAndSessionSchemaRoundTrip() throws Exception {
        String url = environment("XUGU_JDBC_URL");
        Assume.assumeTrue("XUGU_JDBC_URL is not configured", hasText(url));

        String username = environment("XUGU_JDBC_USER");
        String password = environment("XUGU_JDBC_PASSWORD");
        String schema = environment("XUGU_JDBC_SCHEMA");
        String compatibleMode = environment("XUGU_COMPATIBLE_MODE");

        Class.forName(DRIVER);
        if (!hasText(schema)) {
            try (Connection connection = openRawConnection(url, username, password);
                 PreparedStatement statement =
                         connection.prepareStatement("SELECT CURRENT_SCHEMA()");
                 ResultSet rs = statement.executeQuery()) {
                assertTrue(rs.next());
                schema = rs.getString(1);
            }
        }
        assertTrue("Unable to resolve Xugu current schema", hasText(schema));

        String database = XuguJdbcUrl.databaseName(url);
        assertNotNull(database);

        String suffix = Long.toHexString(System.nanoTime())
                .toUpperCase(Locale.ROOT);
        String tableName = "XGIT" + suffix + "_A_B";
        // When metadata table names are treated as LIKE patterns, _A_B also
        // matches XAYB. Keeping a deliberate decoy makes the integration test
        // catch wildcard leakage in getColumns/getTables.
        String decoyName = "XGIT" + suffix + "XAYB";
        TablePath tablePath = TablePath.of(database, schema, tableName);

        Map<String, String> catalogProperties =
                new LinkedHashMap<String, String>();
        catalogProperties.put("useLike", "true");
        if (hasText(compatibleMode)) {
            catalogProperties.put("compatiblemode", compatibleMode);
        }

        JdbcCatalogConfig catalogConfig = new JdbcCatalogConfig(
                url,
                username,
                password,
                DRIVER,
                catalogProperties,
                false);
        XuguCatalog catalog = new XuguCatalog(
                "xugu-integration",
                catalogConfig,
                schema);

        JdbcConnectionConfig connectionConfig = connectionConfig(
                url,
                username,
                password,
                schema,
                compatibleMode);
        XuguDialect dialect = new XuguDialect(connectionConfig);

        try {
            catalog.open();

            TableSchema tableSchema = TableSchema.builder()
                    .column(Column.builder("ID", BasicType.LONG_TYPE)
                            .nullable(false)
                            .build())
                    .column(Column.builder("NAME", BasicType.STRING_TYPE)
                            .length(64L)
                            .build())
                    .primaryKey(PrimaryKey.of(
                            "PK_XGIT_" + suffix,
                            Arrays.asList("ID")))
                    .build();
            catalog.createTable(
                    CatalogTable.builder(tablePath, tableSchema).build(),
                    false);

            try (Connection connection = openRawConnection(url, username, password);
                 Statement statement = connection.createStatement()) {
                statement.execute(
                        "CREATE TABLE " + qualified(schema, decoyName)
                                + " (\"DECOY_ONLY\" INTEGER)");
            }

            assertTrue(catalog.tableExists(tablePath));
            CatalogTable discovered = catalog.getTable(tablePath);
            assertEquals(2, discovered.getTableSchema().getColumnCount());
            assertTrue(discovered.getTableSchema().contains("ID"));
            assertTrue(discovered.getTableSchema().contains("NAME"));
            assertFalse(discovered.getTableSchema().contains("DECOY_ONLY"));

            try (Connection connection = openRawConnection(url, username, password);
                 PreparedStatement insert = connection.prepareStatement(
                         "INSERT INTO " + qualified(schema, tableName)
                                 + " (\"ID\", \"NAME\") VALUES (?, ?)")) {
                insert.setLong(1, 1L);
                insert.setString(2, "before");
                insert.executeUpdate();
            }

            String mergeSql = dialect.buildUpsertSql(
                    tablePath,
                    Arrays.asList("ID", "NAME"),
                    Collections.singletonList("ID"))
                    .get();
            try (Connection connection = openRawConnection(url, username, password);
                 PreparedStatement merge = connection.prepareStatement(mergeSql)) {
                merge.setLong(1, 1L);
                merge.setString(2, "after");
                merge.executeUpdate();
            }

            // The connector-level schema must also become the real Xugu session
            // current_schema so user custom SQL can use unqualified table names.
            try (JdbcConnectionProvider provider =
                         new JdbcConnectionProvider(connectionConfig, dialect)) {
                Connection connection = provider.getOrEstablishConnection();
                try (PreparedStatement query = connection.prepareStatement(
                        "SELECT \"NAME\" FROM " + quote(tableName)
                                + " WHERE \"ID\" = 1");
                     ResultSet rs = query.executeQuery()) {
                    assertTrue(rs.next());
                    assertEquals("after", rs.getString(1));
                }
            }
        } finally {
            catalog.close();
            bestEffortDrop(url, username, password, schema, tableName);
            bestEffortDrop(url, username, password, schema, decoyName);
        }
    }

    @Test
    public void optionalTimeFractionCheckDocumentsDriverBehavior() throws Exception {
        String url = environment("XUGU_JDBC_URL");
        boolean enabled = Boolean.parseBoolean(
                environment("XUGU_IT_CHECK_TIME_PRECISION"));
        Assume.assumeTrue(
                "Set XUGU_IT_CHECK_TIME_PRECISION=true with XUGU_JDBC_URL to verify TIME(3)",
                enabled && hasText(url));

        String username = environment("XUGU_JDBC_USER");
        String password = environment("XUGU_JDBC_PASSWORD");
        String schema = environment("XUGU_JDBC_SCHEMA");
        Class.forName(DRIVER);

        if (!hasText(schema)) {
            try (Connection connection = openRawConnection(url, username, password);
                 PreparedStatement statement =
                         connection.prepareStatement("SELECT CURRENT_SCHEMA()");
                 ResultSet rs = statement.executeQuery()) {
                assertTrue(rs.next());
                schema = rs.getString(1);
            }
        }

        String tableName = "XGITTIME"
                + Long.toHexString(System.nanoTime()).toUpperCase(Locale.ROOT);
        try {
            try (Connection connection = openRawConnection(url, username, password);
                 Statement statement = connection.createStatement()) {
                statement.execute(
                        "CREATE TABLE " + qualified(schema, tableName)
                                + " (\"T\" TIME(3))");
                statement.execute(
                        "INSERT INTO " + qualified(schema, tableName)
                                + " (\"T\") VALUES ('12:34:56.123')");
            }

            try (Connection connection = openRawConnection(url, username, password);
                 PreparedStatement statement = connection.prepareStatement(
                         "SELECT \"T\" FROM " + qualified(schema, tableName));
                 ResultSet rs = statement.executeQuery()) {
                assertTrue(rs.next());
                assertEquals(
                        "Xugu JDBC TIME(3) text output lost fractional precision",
                        "12:34:56.123",
                        rs.getString(1));
            }
        } finally {
            bestEffortDrop(url, username, password, schema, tableName);
        }
    }

    private static JdbcConnectionConfig connectionConfig(
            String url,
            String username,
            String password,
            String schema,
            String compatibleMode) {
        Map<String, Object> values = new LinkedHashMap<String, Object>();
        values.put("url", url);
        values.put("driver", DRIVER);
        if (hasText(username)) {
            values.put("username", username);
        }
        if (password != null) {
            values.put("password", password);
        }
        if (hasText(schema)) {
            values.put("schema", schema);
        }
        if (hasText(compatibleMode)) {
            values.put("compatible_mode", compatibleMode);
        }
        return JdbcConnectionConfig.of(ReadonlyConfig.fromMap(values));
    }

    private static Connection openRawConnection(
            String url,
            String username,
            String password) throws SQLException {
        Properties properties = new Properties();
        if (hasText(username)) {
            properties.setProperty("user", username);
        }
        if (password != null) {
            properties.setProperty("password", password);
        }
        return DriverManager.getConnection(url, properties);
    }

    private static void bestEffortDrop(
            String url,
            String username,
            String password,
            String schema,
            String tableName) {
        if (!hasText(schema) || !hasText(tableName)) {
            return;
        }
        try (Connection connection = openRawConnection(url, username, password);
             Statement statement = connection.createStatement()) {
            statement.execute("DROP TABLE " + qualified(schema, tableName));
        } catch (Exception ignored) {
            // Cleanup is best effort so the original integration assertion stays visible.
        }
    }

    private static String qualified(String schema, String table) {
        return quote(schema) + "." + quote(table);
    }

    private static String quote(String identifier) {
        return "\"" + identifier.replace("\"", "\"\"") + "\"";
    }

    private static String environment(String key) {
        return System.getenv(key);
    }

    private static boolean hasText(String value) {
        return value != null && !value.trim().isEmpty();
    }
}
