package com.link.up.connector.jdbc.core.dialect.duckdb;

import com.link.up.api.table.catalog.Column;
import com.link.up.api.table.type.BasicType;
import com.link.up.api.table.type.DecimalType;
import org.junit.Test;

import java.sql.Types;

import static org.junit.Assert.assertEquals;

public class DuckDbTypeMapperTest {

    private final DuckDbTypeMapper mapper = new DuckDbTypeMapper();

    @Test
    public void widensUnsignedIntegersWithoutOverflow() {
        assertEquals(
                BasicType.SHORT_TYPE,
                mapper.mapType(Types.SMALLINT, "UTINYINT", 3, 0));
        assertEquals(
                BasicType.INT_TYPE,
                mapper.mapType(Types.INTEGER, "USMALLINT", 5, 0));
        assertEquals(
                BasicType.LONG_TYPE,
                mapper.mapType(Types.BIGINT, "UINTEGER", 10, 0));
        assertEquals(
                new DecimalType(20, 0),
                mapper.mapType(Types.DECIMAL, "UBIGINT", 20, 0));
    }

    @Test
    public void hugeIntegerFamiliesStayExactText() {
        assertEquals(
                BasicType.STRING_TYPE,
                mapper.mapType(Types.OTHER, "HUGEINT", 39, 0));
        assertEquals(
                BasicType.STRING_TYPE,
                mapper.mapType(Types.OTHER, "UHUGEINT", 39, 0));
        assertEquals(
                BasicType.STRING_TYPE,
                mapper.mapType(Types.OTHER, "BIGNUM", 0, 0));
    }

    @Test
    public void supportsDuckDbDecimalPrecisionUpToThirtyEight() {
        Column column = Column.builder("amount", new DecimalType(38, 12)).build();
        assertEquals("DECIMAL(38,12)", mapper.toDatabaseType(column));
    }

    @Test(expected = IllegalArgumentException.class)
    public void rejectsTargetDecimalPrecisionAboveDuckDbLimit() {
        mapper.toDatabaseType(
                Column.builder("amount", new DecimalType(39, 12)).build());
    }

    @Test
    public void invalidSourceDecimalShapesStayExactText() {
        assertEquals(
                BasicType.STRING_TYPE,
                mapper.mapType(Types.DECIMAL, "DECIMAL", 39, 2));
        assertEquals(
                BasicType.STRING_TYPE,
                mapper.mapType(Types.DECIMAL, "DECIMAL", 20, -2));
        assertEquals(
                BasicType.STRING_TYPE,
                mapper.mapType(Types.DECIMAL, "DECIMAL", 10, 12));
    }

    @Test
    public void bitAndComplexTypesStayTextual() {
        assertEquals(
                BasicType.STRING_TYPE,
                mapper.mapType(Types.OTHER, "BIT", 64, 0));
        assertEquals(
                BasicType.STRING_TYPE,
                mapper.mapType(Types.ARRAY, "INTEGER[]", 0, 0));
        assertEquals(
                BasicType.STRING_TYPE,
                mapper.mapType(Types.STRUCT, "STRUCT(a INTEGER)", 0, 0));
    }

    @Test
    public void mapsNanosecondTemporalTargetsExplicitly() {
        assertEquals(
                "TIME_NS",
                mapper.toDatabaseType(
                        Column.builder("event_time", BasicType.TIME_TYPE)
                                .precision(9)
                                .build()));
        assertEquals(
                "TIMESTAMP_NS",
                mapper.toDatabaseType(
                        Column.builder("created_at", BasicType.TIMESTAMP_TYPE)
                                .precision(9)
                                .build()));
    }

    @Test(expected = IllegalArgumentException.class)
    public void rejectsTimePrecisionAboveNanoseconds() {
        mapper.toDatabaseType(
                Column.builder("event_time", BasicType.TIME_TYPE)
                        .precision(10)
                        .build());
    }

    @Test(expected = IllegalArgumentException.class)
    public void rejectsTimestampPrecisionAboveNanoseconds() {
        mapper.toDatabaseType(
                Column.builder("created_at", BasicType.TIMESTAMP_TYPE)
                        .precision(10)
                        .build());
    }

    @Test
    public void mapsTimezoneTypesWithoutDroppingOffset() {
        assertEquals(
                BasicType.STRING_TYPE,
                mapper.mapType(
                        Types.TIME_WITH_TIMEZONE,
                        "TIME WITH TIME ZONE",
                        0,
                        6));
        assertEquals(
                BasicType.TIMESTAMP_TZ_TYPE,
                mapper.mapType(
                        Types.TIMESTAMP_WITH_TIMEZONE,
                        "TIMESTAMP WITH TIME ZONE",
                        0,
                        6));
        assertEquals(
                "TIMESTAMPTZ",
                mapper.toDatabaseType(
                        Column.builder("event_at", BasicType.TIMESTAMP_TZ_TYPE)
                                .precision(6)
                                .build()));
    }

    @Test(expected = IllegalArgumentException.class)
    public void rejectsNanosecondTimezoneTargetBecauseDuckDbTimestamptzIsMicros() {
        mapper.toDatabaseType(
                Column.builder("event_at", BasicType.TIMESTAMP_TZ_TYPE)
                        .precision(9)
                        .build());
    }

    @Test
    public void genericStringsAndBytesUseUnboundedNativeTypes() {
        assertEquals(
                "VARCHAR",
                mapper.toDatabaseType(
                        Column.builder("payload", BasicType.STRING_TYPE).build()));
        assertEquals(
                "BLOB",
                mapper.toDatabaseType(
                        Column.builder("payload", BasicType.BYTES_TYPE).build()));
    }

    @Test
    public void preservesSafeScalarSourceTypesButNotNestedTypes() {
        Column json = Column.builder("payload", BasicType.STRING_TYPE)
                .sourceType("JSON")
                .build();
        assertEquals("JSON", mapper.toDatabaseType(json, true));

        Column list = Column.builder("items", BasicType.STRING_TYPE)
                .sourceType("INTEGER[]")
                .build();
        assertEquals("VARCHAR", mapper.toDatabaseType(list, true));
    }
}
