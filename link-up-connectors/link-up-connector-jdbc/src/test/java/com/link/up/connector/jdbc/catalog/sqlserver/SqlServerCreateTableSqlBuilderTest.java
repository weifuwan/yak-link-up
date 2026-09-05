package com.link.up.connector.jdbc.catalog.sqlserver;

import com.link.up.api.table.catalog.CatalogTable;
import com.link.up.api.table.catalog.Column;
import com.link.up.api.table.catalog.PrimaryKey;
import com.link.up.api.table.catalog.TablePath;
import com.link.up.api.table.catalog.TableSchema;
import com.link.up.api.table.type.BasicType;
import com.link.up.connector.jdbc.core.dialect.sqlserver.SqlServerTypeMapper;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class SqlServerCreateTableSqlBuilderTest {

    @Test
    public void buildsOfflineCreateTableWithIdentityAndComments() {
        TablePath path = TablePath.of("app", "dbo", "orders");
        TableSchema schema = TableSchema.builder()
                .column(Column.builder("id", BasicType.LONG_TYPE)
                        .nullable(false).autoIncrement(true).build())
                .column(Column.builder("name", BasicType.STRING_TYPE)
                        .length(100L).comment("display name").build())
                .primaryKey(PrimaryKey.of("PK_orders", Arrays.asList("id")))
                .build();
        CatalogTable table = CatalogTable.builder(path, schema)
                .comment("orders table")
                .build();

        List<String> statements = new SqlServerCreateTableSqlBuilder(
                path, table, new SqlServerTypeMapper()).buildStatements();
        String create = statements.get(0);
        assertTrue(create.contains("CREATE TABLE [app].[dbo].[orders]"));
        assertTrue(create.contains("[id] BIGINT IDENTITY(1,1) NOT NULL"));
        assertTrue(create.contains("[name] NVARCHAR(100) NULL"));
        assertTrue(create.contains("CONSTRAINT [PK_orders] PRIMARY KEY ([id])"));
        assertEquals(3, statements.size());
        assertTrue(statements.get(1).contains("sp_addextendedproperty"));
        assertTrue(statements.get(2).contains("@level2name=N'name'"));
    }
}
