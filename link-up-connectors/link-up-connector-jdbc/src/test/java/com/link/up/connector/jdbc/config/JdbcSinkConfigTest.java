package com.link.up.connector.jdbc.config;

import com.link.up.api.configuration.ReadonlyConfig;
import com.link.up.api.table.catalog.TablePath;
import com.link.up.connector.jdbc.sink.DirtyDataPolicy;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigResolveOptions;
import org.junit.Test;

import static org.junit.Assert.*;

public class JdbcSinkConfigTest {

    private static final String MYSQL_DRIVER = "com.mysql.cj.jdbc.Driver";

    @Test
    public void shouldResolveTargetTableTemplateForMySqlPath() {
        JdbcSinkConfig config = config("table = \"sink_${schema_name}_${table_name}\"");

        assertEquals(
                "sink_flux_test_orders",
                config.resolveTargetTablePath(TablePath.of("flux_test", "orders")));
    }

    @Test
    public void shouldResolveTargetTableTemplateForSchemaPath() {
        JdbcSinkConfig config = config("table_path = \"${schema_name}.${table_name}_copy\"");

        assertEquals(
                "public.orders_copy",
                config.resolveTargetTablePath(TablePath.of("catalog", "public", "orders")));
    }

    @Test
    public void shouldPreferConfiguredTableOverCustomSql() {
        JdbcSinkConfig config = config("table = \"sink_${table_name}\"\ncustom_sql = \"INSERT INTO ignored VALUES (?)\"");

        assertFalse(config.hasCustomSql());
        assertTrue(config.resolveTargetTablePath(TablePath.of("orders")).equals("sink_orders"));
    }

    @Test
    public void shouldRetainSinkTemplateWhenHoconAllowsUnresolvedSubstitutions() {
        JdbcSinkConfig config = JdbcSinkConfig.of(
                ReadonlyConfig.fromConfig(
                        ConfigFactory.parseString(
                                "url = \"jdbc:mysql://localhost:3306/test1\"\n"
                                        + "driver = \"" + MYSQL_DRIVER + "\"\n"
                                        + "table = \"test1.sink_${table_name}\"")
                                .resolve(
                                        ConfigResolveOptions.defaults()
                                                .setAllowUnresolved(true))));

        assertEquals(
                "test1.sink_orders",
                config.resolveTargetTablePath(TablePath.of("flux_test", "orders")));
    }

    @Test
    public void shouldFailFastForDirtyDataByDefault() {
        assertEquals(DirtyDataPolicy.FAIL_FAST, config("").getDirtyDataPolicy());
        assertFalse(config("").shouldSkipDirtyData());
    }

    @Test
    public void shouldUseBoundedPreparedStatementCacheAndQueryTimeout() {
        JdbcSinkConfig config = config("prepared_statement_cache_size = 8\nquery_timeout_sec = 15");

        assertEquals(8, config.getPreparedStatementCacheSize());
        assertEquals(15, config.getQueryTimeoutSec());
    }

    @Test
    public void shouldAllowSkippingDirtyRows() {
        JdbcSinkConfig config = config("dirty_data_policy = SKIP");

        assertEquals(DirtyDataPolicy.SKIP, config.getDirtyDataPolicy());
        assertTrue(config.shouldSkipDirtyData());
    }

    private JdbcSinkConfig config(String sinkOptions) {
        return JdbcSinkConfig.of(
                ReadonlyConfig.fromConfig(
                        ConfigFactory.parseString(
                                "url = \"jdbc:mysql://localhost:3306/flux_test\"\n"
                                        + "driver = \"" + MYSQL_DRIVER + "\"\n"
                                        + sinkOptions)));
    }
}
