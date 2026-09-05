package com.link.up.connector.jdbc.catalog.iris;

import com.link.up.api.table.catalog.CatalogTable;
import com.link.up.api.table.catalog.Column;
import com.link.up.api.table.catalog.PrimaryKey;
import com.link.up.api.table.catalog.TablePath;
import com.link.up.api.table.catalog.TableSchema;
import com.link.up.api.table.type.BasicType;
import com.link.up.api.table.type.DecimalType;
import com.link.up.connector.jdbc.core.dialect.iris.IrisTypeMapper;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertTrue;

public class IrisCreateTableSqlBuilderTest {

    @Test
    public void buildsSerialPrimaryKeyDescriptionsAndLosslessTypes() {
        TablePath path = TablePath.of("USER", "App", "Users");
        TableSchema schema = TableSchema.builder()
                .column(Column.builder("Id", BasicType.LONG_TYPE)
                        .nullable(false)
                        .autoIncrement(true)
                        .build())
                .column(Column.builder("Name", BasicType.STRING_TYPE)
                        .length(64L)
                        .comment("display name")
                        .build())
                .column(Column.builder("Amount", new DecimalType(37, 18)).build())
                .column(Column.builder("Payload", BasicType.STRING_TYPE)
                        .length(100_000L)
                        .build())
                .primaryKey(PrimaryKey.of("PK_Users", Arrays.asList("Id")))
                .build();
        CatalogTable table = CatalogTable.builder(path, schema)
                .comment("users table")
                .build();

        String ddl = new IrisCreateTableSqlBuilder(
                path,
                table,
                new IrisTypeMapper())
                .build();

        assertTrue(ddl.contains("CREATE TABLE \"App\".\"Users\""));
        assertTrue(ddl.contains("%DESCRIPTION 'users table'"));
        assertTrue(ddl.contains("\"Id\" SERIAL NOT NULL"));
        assertTrue(ddl.contains(
                "\"Name\" VARCHAR(64) %DESCRIPTION 'display name'"));
        assertTrue(ddl.contains("\"Amount\" NUMERIC(37,18)"));
        assertTrue(ddl.contains("\"Payload\" LONGVARCHAR"));
        assertTrue(ddl.contains(
                "CONSTRAINT \"PK_Users\" PRIMARY KEY (\"Id\")"));
    }

    @Test
    public void sameDialectPreservesGuidButNormalizesReadOnlyRowversion() {
        TablePath path = TablePath.of("USER", "App", "Events");
        TableSchema schema = TableSchema.builder()
                .column(Column.builder("EventId", BasicType.STRING_TYPE)
                        .sourceType("GUID")
                        .length(36L)
                        .build())
                .column(Column.builder("Version", BasicType.LONG_TYPE)
                        .sourceType("ROWVERSION")
                        .build())
                .build();
        CatalogTable table = CatalogTable.builder(path, schema)
                .option(IrisCatalog.TABLE_OPTION_DIALECT, IrisCatalog.DIALECT)
                .build();

        String ddl = new IrisCreateTableSqlBuilder(
                path,
                table,
                new IrisTypeMapper())
                .build();

        assertTrue(ddl.contains("\"EventId\" GUID"));
        assertTrue(ddl.contains("\"Version\" BIGINT"));
    }

    @Test
    public void unrepresentableSameDialectNumericFallsBackToLongVarchar() {
        TablePath path = TablePath.of("USER", "App", "Amounts");
        TableSchema schema = TableSchema.builder()
                .column(Column.builder("Amount", BasicType.STRING_TYPE)
                        .sourceType("NUMERIC(20,0)")
                        .build())
                .build();
        CatalogTable table = CatalogTable.builder(path, schema)
                .option(IrisCatalog.TABLE_OPTION_DIALECT, IrisCatalog.DIALECT)
                .build();

        String ddl = new IrisCreateTableSqlBuilder(
                path,
                table,
                new IrisTypeMapper())
                .build();

        assertTrue(ddl.contains("\"Amount\" LONGVARCHAR"));
    }

    @Test
    public void escapesDescriptionsAsSqlLiterals() {
        TablePath path = TablePath.of("USER", "App", "Notes");
        TableSchema schema = TableSchema.builder()
                .column(Column.builder("Body", BasicType.STRING_TYPE)
                        .length(64L)
                        .comment("doctor's note")
                        .build())
                .build();
        CatalogTable table = CatalogTable.builder(path, schema)
                .comment("team's notes")
                .build();

        String ddl = new IrisCreateTableSqlBuilder(
                path,
                table,
                new IrisTypeMapper())
                .build();

        assertTrue(ddl.contains("%DESCRIPTION 'team''s notes'"));
        assertTrue(ddl.contains("%DESCRIPTION 'doctor''s note'"));
    }
}
