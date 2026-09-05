package com.link.up.connector.jdbc.core.dialect.postgres;

import com.link.up.api.table.catalog.Column;
import com.link.up.api.table.type.BasicType;
import com.link.up.api.table.type.DecimalType;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class PostgresTypeMapperTest {

    private final PostgresTypeMapper mapper =
            new PostgresTypeMapper();

    @Test
    public void mapsFluxTypesToPostgresTypes() {
        assertEquals(
                "VARCHAR(128)",
                mapper.toDatabaseType(
                        Column.builder(
                                "name",
                                BasicType.STRING_TYPE)
                                .length(128L)
                                .build()));

        assertEquals(
                "BYTEA",
                mapper.toDatabaseType(
                        Column.builder(
                                "payload",
                                BasicType.BYTES_TYPE)
                                .build()));

        assertEquals(
                "NUMERIC(20,4)",
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
    }

    @Test
    public void preservesSafePostgresNativeTypes() {
        assertEquals(
                "jsonb",
                mapper.toDatabaseType(
                        Column.builder(
                                "payload",
                                BasicType.STRING_TYPE)
                                .sourceType(
                                        "jsonb")
                                .build(),
                        true));

        assertEquals(
                "uuid",
                mapper.toDatabaseType(
                        Column.builder(
                                "id",
                                BasicType.STRING_TYPE)
                                .sourceType(
                                        "uuid")
                                .build(),
                        true));
    }

    @Test
    public void unsupportedNativeTypeFallsBackToFluxType() {
        assertEquals(
                "TEXT",
                mapper.toDatabaseType(
                        Column.builder(
                                "custom_value",
                                BasicType.STRING_TYPE)
                                .sourceType(
                                        "my_enum")
                                .build(),
                        true));
    }
}
