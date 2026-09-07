package com.link.up.connector.jdbc.catalog.gbase.gbase8c;

import com.link.up.api.table.catalog.CatalogTable;
import com.link.up.api.table.catalog.Column;
import com.link.up.api.table.catalog.TablePath;
import com.link.up.api.table.catalog.TableSchema;
import com.link.up.api.table.catalog.exception.CatalogException;
import com.link.up.api.table.type.BasicType;
import com.link.up.connector.jdbc.catalog.JdbcCatalogConfig;
import org.junit.Test;

import java.util.Collections;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class GBase8cCatalogTest {

    @Test
    public void existingTableSinkRejectsAutomaticTableCreation() {
        GBase8cCatalog catalog = catalog();

        try {
            catalog.createTable(table(), false);
            fail("GBase 8c existing-table Sink must not auto-create target tables");
        } catch (CatalogException expected) {
            assertTrue(expected.getMessage().contains("existing-table Sink"));
            assertTrue(expected.getMessage().contains("create table"));
        }
    }

    @Test
    public void existingTableSinkRejectsDestructiveSchemaRecreationBeforeDrop() {
        GBase8cCatalog catalog = catalog();

        try {
            catalog.dropTable(
                    TablePath.of("app", "public", "orders"),
                    false);
            fail("GBase 8c existing-table Sink must not drop target tables");
        } catch (CatalogException expected) {
            assertTrue(expected.getMessage().contains("drop table"));
        }
    }

    @Test
    public void existingTableSinkRejectsSchemaEvolution() {
        GBase8cCatalog catalog = catalog();

        try {
            catalog.addColumn(
                    TablePath.of("app", "public", "orders"),
                    Column.builder("extra", BasicType.STRING_TYPE).build());
            fail("GBase 8c existing-table Sink must not add target columns");
        } catch (CatalogException expected) {
            assertTrue(expected.getMessage().contains("add column"));
        }
    }

    private static GBase8cCatalog catalog() {
        return new GBase8cCatalog(
                "gbase8c",
                new JdbcCatalogConfig(
                        "jdbc:gbase8c://127.0.0.1:5432/app",
                        "gbase",
                        "password",
                        "com.gbase8c.Driver",
                        Collections.emptyMap(),
                        false),
                "public");
    }

    private static CatalogTable table() {
        TableSchema schema =
                TableSchema.builder()
                        .columns(
                                Collections.singletonList(
                                        Column.builder("id", BasicType.LONG_TYPE)
                                                .nullable(false)
                                                .build()))
                        .build();

        return CatalogTable.builder(
                        TablePath.of("app", "public", "orders"),
                        schema)
                .build();
    }
}
