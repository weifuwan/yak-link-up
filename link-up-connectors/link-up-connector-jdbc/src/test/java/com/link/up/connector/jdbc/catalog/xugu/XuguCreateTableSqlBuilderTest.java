package com.link.up.connector.jdbc.catalog.xugu;

import com.link.up.api.table.catalog.CatalogTable;
import com.link.up.api.table.catalog.Column;
import com.link.up.api.table.catalog.PrimaryKey;
import com.link.up.api.table.catalog.TablePath;
import com.link.up.api.table.catalog.TableSchema;
import com.link.up.api.table.type.BasicType;
import com.link.up.api.table.type.DecimalType;
import com.link.up.connector.jdbc.core.dialect.xugu.XuguTypeMapper;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import static org.junit.Assert.assertTrue;

public class XuguCreateTableSqlBuilderTest {

    @Test
    public void buildsIdentityPrimaryKeyLobsAndComments() {
        TablePath path = TablePath.of("SYSTEM", "APP", "USERS");
        TableSchema schema = TableSchema.builder()
                .column(Column.builder("ID", BasicType.LONG_TYPE)
                        .nullable(false)
                        .autoIncrement(true)
                        .build())
                .column(Column.builder("NAME", BasicType.STRING_TYPE)
                        .length(64L)
                        .comment("display name")
                        .build())
                .column(Column.builder("AMOUNT", new DecimalType(38, 18)).build())
                .column(Column.builder("PAYLOAD", BasicType.STRING_TYPE)
                        .length(100_000L)
                        .build())
                .primaryKey(PrimaryKey.of("PK_USERS", Arrays.asList("ID")))
                .build();
        CatalogTable table = CatalogTable.builder(path, schema)
                .comment("users table")
                .build();

        String ddl = new XuguCreateTableSqlBuilder(
                path,
                table,
                new XuguTypeMapper())
                .build();

        assertTrue(ddl.contains("CREATE TABLE \"APP\".\"USERS\""));
        assertTrue(ddl.contains("\"ID\" BIGINT IDENTITY(1,1) NOT NULL"));
        assertTrue(ddl.contains("\"NAME\" VARCHAR(64)"));
        assertTrue(ddl.contains("\"AMOUNT\" NUMERIC(38,18)"));
        assertTrue(ddl.contains("\"PAYLOAD\" CLOB"));
        assertTrue(ddl.contains(
                "CONSTRAINT \"PK_USERS\" PRIMARY KEY (\"ID\")"));
        assertTrue(ddl.contains(
                "COMMENT ON TABLE \"APP\".\"USERS\" IS 'users table'"));
        assertTrue(ddl.contains(
                "COMMENT ON COLUMN \"APP\".\"USERS\".\"NAME\" IS 'display name'"));
    }

    @Test
    public void sameDialectPreservesSafeScalarsButNormalizesUnsafeTypes() {
        TablePath path = TablePath.of("SYSTEM", "APP", "EVENTS");
        TableSchema schema = TableSchema.builder()
                .column(Column.builder("PAYLOAD", BasicType.STRING_TYPE)
                        .sourceType("JSON")
                        .build())
                .column(Column.builder("FLAGS", BasicType.STRING_TYPE)
                        .sourceType("BIT(8)")
                        .length(8L)
                        .build())
                .column(Column.builder("AMOUNT", BasicType.STRING_TYPE)
                        .sourceType("NUMERIC(39,2)")
                        .build())
                .column(Column.builder("EVENT_AT", BasicType.TIMESTAMP_TZ_TYPE)
                        .sourceType("TIMESTAMP(6) WITH TIME ZONE")
                        .precision(6)
                        .build())
                .column(Column.builder("ITEMS", BasicType.STRING_TYPE)
                        .sourceType("ARRAY")
                        .build())
                .column(Column.builder("RID", BasicType.STRING_TYPE)
                        .sourceType("ROWID")
                        .build())
                .build();
        CatalogTable table = CatalogTable.builder(path, schema)
                .option(XuguCatalog.TABLE_OPTION_DIALECT, XuguCatalog.DIALECT)
                .build();

        String ddl = new XuguCreateTableSqlBuilder(
                path,
                table,
                new XuguTypeMapper())
                .build();

        assertTrue(ddl.contains("\"PAYLOAD\" JSON"));
        assertTrue(ddl.contains("\"FLAGS\" BIT(8)"));
        assertTrue(ddl.contains("\"AMOUNT\" CLOB"));
        assertTrue(ddl.contains("\"EVENT_AT\" VARCHAR(64)"));
        assertTrue(ddl.contains("\"ITEMS\" CLOB"));
        assertTrue(ddl.contains("\"RID\" CLOB"));
    }

    @Test
    public void longMultibytePrimaryKeyNameStaysWithinXuguByteLimit() {
        StringBuilder name = new StringBuilder("PK_");
        for (int i = 0; i < 80; i++) {
            name.append('主');
        }

        TablePath path = TablePath.of("SYSTEM", "APP", "LONG_KEYS");
        TableSchema schema = TableSchema.builder()
                .column(Column.builder("ID", BasicType.LONG_TYPE).build())
                .primaryKey(PrimaryKey.of(name.toString(), Arrays.asList("ID")))
                .build();
        CatalogTable table = CatalogTable.builder(path, schema).build();

        String ddl = new XuguCreateTableSqlBuilder(
                path,
                table,
                new XuguTypeMapper())
                .build();

        String marker = "CONSTRAINT \"";
        int start = ddl.indexOf(marker) + marker.length();
        int end = ddl.indexOf("\" PRIMARY KEY", start);
        String constraintName = ddl.substring(start, end);
        assertTrue(constraintName.getBytes(StandardCharsets.UTF_8).length <= 127);
        assertTrue(constraintName.matches(".*_[0-9a-f]{8}$"));
    }

    @Test
    public void escapesCommentLiterals() {
        TablePath path = TablePath.of("SYSTEM", "APP", "NOTES");
        TableSchema schema = TableSchema.builder()
                .column(Column.builder("BODY", BasicType.STRING_TYPE)
                        .length(64L)
                        .comment("doctor's note")
                        .build())
                .build();
        CatalogTable table = CatalogTable.builder(path, schema)
                .comment("team's notes")
                .build();

        String ddl = new XuguCreateTableSqlBuilder(
                path,
                table,
                new XuguTypeMapper())
                .build();

        assertTrue(ddl.contains("IS 'team''s notes'"));
        assertTrue(ddl.contains("IS 'doctor''s note'"));
    }
}
