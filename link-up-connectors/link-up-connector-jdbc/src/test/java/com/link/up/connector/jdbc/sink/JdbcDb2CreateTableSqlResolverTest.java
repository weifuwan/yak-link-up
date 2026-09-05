package com.link.up.connector.jdbc.sink;

import com.link.up.api.configuration.ReadonlyConfig;
import com.link.up.api.table.catalog.TablePath;
import com.link.up.connector.jdbc.config.JdbcConnectionConfig;
import org.junit.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class JdbcDb2CreateTableSqlResolverTest {

    @Test
    public void resolvesDb2TargetSchemaFromCurrentSchema() {
        Map<String, Object> values = new LinkedHashMap<String, Object>();
        values.put(
                "url",
                "jdbc:db2://127.0.0.1:50000/SAMPLE:currentSchema=APP;");
        values.put("driver", "com.ibm.db2.jcc.DB2Driver");
        values.put("username", "db2inst1");

        JdbcConnectionConfig config = JdbcConnectionConfig.of(
                ReadonlyConfig.fromMap(values));

        assertEquals(
                TablePath.of("SAMPLE", "APP", "ORDERS"),
                JdbcCreateTableSqlResolver.resolveTargetPath(
                        config,
                        TablePath.of("ORDERS")));
    }

    @Test
    public void sourceDatabaseAndSchemaDoNotLeakIntoDb2Target() {
        Map<String, Object> values = new LinkedHashMap<String, Object>();
        values.put("url", "jdbc:db2://127.0.0.1:50000/SAMPLE");
        values.put("driver", "com.ibm.db2.jcc.DB2Driver");
        values.put("username", "db2inst1");
        values.put("schema", "TARGET");

        JdbcConnectionConfig config = JdbcConnectionConfig.of(
                ReadonlyConfig.fromMap(values));

        assertEquals(
                TablePath.of("SAMPLE", "TARGET", "ORDERS"),
                JdbcCreateTableSqlResolver.resolveTargetPath(
                        config,
                        TablePath.of("source_db", "public", "ORDERS")));
    }
}
