package com.link.up.connector.jdbc.core.dialect.oracle;

import com.link.up.api.table.catalog.Column;
import com.link.up.api.table.type.BasicType;
import com.link.up.api.table.type.DecimalType;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class OracleTypeMapperTest {

    private final OracleTypeMapper mapper =
            new OracleTypeMapper();

    @Test
    public void mapsFluxTypesToOracleTypes() {
        assertEquals(
                "VARCHAR2(128)",
                mapper.toDatabaseType(
                        Column.builder(
                                "name",
                                BasicType.STRING_TYPE)
                                .length(128L)
                                .build()));

        assertEquals(
                "BLOB",
                mapper.toDatabaseType(
                        Column.builder(
                                "payload",
                                BasicType.BYTES_TYPE)
                                .build()));

        assertEquals(
                "NUMBER(20,4)",
                mapper.toDatabaseType(
                        Column.builder(
                                "amount",
                                new DecimalType(
                                        20,
                                        4))
                                .precision(20)
                                .scale(4)
                                .build()));

        assertEquals(
                "TIMESTAMP WITH TIME ZONE",
                mapper.toDatabaseType(
                        Column.builder(
                                "created_at",
                                BasicType.TIMESTAMP_TZ_TYPE)
                                .build()));

        assertEquals(
                "NUMBER(1)",
                mapper.toDatabaseType(
                        Column.builder(
                                "enabled",
                                BasicType.BOOLEAN_TYPE)
                                .build()));
    }

    @Test
    public void longStringsAndBinaryValuesUseLobs() {
        assertEquals(
                "CLOB",
                mapper.toDatabaseType(
                        Column.builder(
                                "content",
                                BasicType.STRING_TYPE)
                                .length(5000L)
                                .build()));

        assertEquals(
                "RAW(100)",
                mapper.toDatabaseType(
                        Column.builder(
                                "digest",
                                BasicType.BYTES_TYPE)
                                .length(100L)
                                .build()));
    }

    @Test
    public void preservesSafeOracleNativeTypes() {
        assertEquals(
                "VARCHAR2(64)",
                mapper.toDatabaseType(
                        Column.builder(
                                "code",
                                BasicType.STRING_TYPE)
                                .sourceType(
                                        "VARCHAR2(64)")
                                .build(),
                        true));

        assertEquals(
                "NUMBER(12,2)",
                mapper.toDatabaseType(
                        Column.builder(
                                "amount",
                                new DecimalType(
                                        12,
                                        2))
                                .sourceType(
                                        "NUMBER(12,2)")
                                .build(),
                        true));
    }

    @Test(expected = IllegalArgumentException.class)
    public void rejectsTimeOnlySinkType() {
        mapper.toDatabaseType(
                Column.builder(
                        "time_only",
                        BasicType.TIME_TYPE)
                        .build());
    }
}
