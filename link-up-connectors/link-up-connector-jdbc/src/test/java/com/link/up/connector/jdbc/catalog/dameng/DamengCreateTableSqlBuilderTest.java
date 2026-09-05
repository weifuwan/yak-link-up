package com.link.up.connector.jdbc.catalog.dameng;

import com.link.up.api.table.catalog.CatalogTable;
import com.link.up.api.table.catalog.Column;
import com.link.up.api.table.catalog.PrimaryKey;
import com.link.up.api.table.catalog.TablePath;
import com.link.up.api.table.catalog.TableSchema;
import com.link.up.api.table.type.BasicType;
import com.link.up.api.table.type.DecimalType;
import com.link.up.connector.jdbc.core.dialect.dameng.DamengTypeMapper;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertTrue;

public class DamengCreateTableSqlBuilderTest {

    @Test
    public void buildsDamengCreateTableWithIdentityStorageAndComments() {
        TablePath path = TablePath.of(null, "APP", "USERS");
        TableSchema schema = TableSchema.builder()
                .column(Column.builder("ID", BasicType.LONG_TYPE)
                        .nullable(false)
                        .autoIncrement(true)
                        .build())
                .column(Column.builder("NAME", BasicType.STRING_TYPE)
                        .length(100L)
                        .comment("display name")
                        .build())
                .column(Column.builder("AMOUNT", new DecimalType(38, 10))
                        .build())
                .primaryKey(PrimaryKey.of("PK_USERS", Arrays.asList("ID")))
                .build();
        CatalogTable table = CatalogTable.builder(path, schema)
                .comment("users table")
                .option(DamengCatalog.TABLE_OPTION_TABLESPACE, "MAIN")
                .option(DamengCatalog.TABLE_OPTION_FILLFACTOR, "80")
                .build();

        String ddl = new DamengCreateTableSqlBuilder(
                path,
                table,
                new DamengTypeMapper())
                .build();

        assertTrue(ddl.contains("CREATE TABLE \"APP\".\"USERS\""));
        assertTrue(ddl.contains("\"ID\" BIGINT IDENTITY(1,1) NOT NULL"));
        assertTrue(ddl.contains("\"NAME\" VARCHAR2(400)"));
        assertTrue(ddl.contains("\"AMOUNT\" DECIMAL(38,10)"));
        assertTrue(ddl.contains("CONSTRAINT \"PK_USERS\" PRIMARY KEY (\"ID\")"));
        assertTrue(ddl.contains("STORAGE (FILLFACTOR 80, ON \"MAIN\")"));
        assertTrue(ddl.contains("COMMENT ON TABLE \"APP\".\"USERS\" IS 'users table'"));
        assertTrue(ddl.contains(
                "COMMENT ON COLUMN \"APP\".\"USERS\".\"NAME\" IS 'display name'"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void autoCreateFailsFastForDecimalPrecisionAboveDamengLimit() {
        TablePath path = TablePath.of(null, "APP", "PAYMENTS");
        TableSchema schema = TableSchema.builder()
                .column(Column.builder("AMOUNT", new DecimalType(39, 10)).build())
                .build();
        CatalogTable table = CatalogTable.builder(path, schema).build();
        new DamengCreateTableSqlBuilder(path, table, new DamengTypeMapper()).build();
    }
}
