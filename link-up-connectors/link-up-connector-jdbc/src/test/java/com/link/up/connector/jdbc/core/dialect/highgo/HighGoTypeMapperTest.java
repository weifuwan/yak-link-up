package com.link.up.connector.jdbc.core.dialect.highgo;

import com.link.up.api.table.catalog.Column;
import com.link.up.api.table.type.BasicType;
import com.link.up.api.table.type.DecimalType;
import org.junit.Test;

import java.sql.Types;

import static org.junit.Assert.assertEquals;

public class HighGoTypeMapperTest {

    private final HighGoTypeMapper mapper = new HighGoTypeMapper();

    @Test
    public void supportsNumericPrecisionUpToOneThousand() {
        Column column = Column.builder("amount", new DecimalType(1000, 20)).build();
        assertEquals("NUMERIC(1000,20)", mapper.toDatabaseType(column));
    }

    @Test(expected = IllegalArgumentException.class)
    public void rejectsTargetNumericPrecisionAboveHighGoLimit() {
        mapper.toDatabaseType(
                Column.builder("amount", new DecimalType(1001, 20)).build());
    }

    @Test
    public void unconstrainedNegativeScaleAndOverLimitNumericUseExactText() {
        assertEquals(
                BasicType.STRING_TYPE,
                mapper.mapType(Types.NUMERIC, "NUMERIC", 0, 0));
        assertEquals(
                BasicType.STRING_TYPE,
                mapper.mapType(Types.NUMERIC, "NUMERIC", 20, -2));
        assertEquals(
                BasicType.STRING_TYPE,
                mapper.mapType(Types.NUMERIC, "NUMERIC", 1001, 20));
    }

    @Test
    public void unrepresentableSameDialectNumericUsesUnconstrainedNumericDdl() {
        Column column = Column.builder("amount", BasicType.STRING_TYPE)
                .sourceType("NUMERIC(131089,0)")
                .build();
        assertEquals("NUMERIC", mapper.toDatabaseType(column, true));
    }

    @Test
    public void mapsMySqlCompatibilityIntegersWithoutOverflow() {
        assertEquals(
                BasicType.BYTE_TYPE,
                mapper.mapType(Types.TINYINT, "TINYINT", 3, 0));
        assertEquals(
                BasicType.SHORT_TYPE,
                mapper.mapType(Types.SMALLINT, "TINYINT UNSIGNED", 3, 0));
        assertEquals(
                BasicType.INT_TYPE,
                mapper.mapType(Types.INTEGER, "SMALLINT UNSIGNED", 5, 0));
        assertEquals(
                BasicType.INT_TYPE,
                mapper.mapType(Types.INTEGER, "MEDIUMINT", 7, 0));
        assertEquals(
                BasicType.LONG_TYPE,
                mapper.mapType(Types.BIGINT, "INT UNSIGNED", 10, 0));
        assertEquals(
                new DecimalType(20, 0),
                mapper.mapType(Types.DECIMAL, "BIGINT UNSIGNED", 20, 0));
    }

    @Test
    public void bitAndMoneyStayExactText() {
        assertEquals(
                BasicType.STRING_TYPE,
                mapper.mapType(Types.BIT, "BIT(8)", 8, 0));
        assertEquals(
                BasicType.STRING_TYPE,
                mapper.mapType(Types.OTHER, "BIT VARYING(64)", 64, 0));
        assertEquals(
                BasicType.STRING_TYPE,
                mapper.mapType(Types.OTHER, "MONEY", 0, 0));
    }

    @Test
    public void timezoneTypesDoNotLoseOffsets() {
        assertEquals(
                BasicType.STRING_TYPE,
                mapper.mapType(
                        Types.TIME_WITH_TIMEZONE,
                        "TIME(6) WITH TIME ZONE",
                        0,
                        6));
        assertEquals(
                BasicType.TIMESTAMP_TZ_TYPE,
                mapper.mapType(
                        Types.TIMESTAMP_WITH_TIMEZONE,
                        "TIMESTAMP(6) WITH TIME ZONE",
                        0,
                        6));
    }

    @Test
    public void withoutTimeZoneTypesStayLocal() {
        assertEquals(
                BasicType.TIME_TYPE,
                mapper.mapType(
                        Types.TIME,
                        "TIME(6) WITHOUT TIME ZONE",
                        0,
                        6));
        assertEquals(
                BasicType.TIMESTAMP_TYPE,
                mapper.mapType(
                        Types.TIMESTAMP,
                        "TIMESTAMP(6) WITHOUT TIME ZONE",
                        0,
                        6));
    }

    @Test(expected = IllegalArgumentException.class)
    public void rejectsTargetTimePrecisionAboveSix() {
        mapper.toDatabaseType(
                Column.builder("event_time", BasicType.TIME_TYPE)
                        .precision(7)
                        .build());
    }

    @Test(expected = IllegalArgumentException.class)
    public void rejectsTargetTimestampPrecisionAboveSix() {
        mapper.toDatabaseType(
                Column.builder("created_at", BasicType.TIMESTAMP_TYPE)
                        .precision(7)
                        .build());
    }

    @Test
    public void mapsCompatibilityLobAndOracleStringAliases() {
        assertEquals(
                BasicType.STRING_TYPE,
                mapper.mapType(Types.CLOB, "CLOB", 0, 0));
        assertEquals(
                BasicType.BYTES_TYPE,
                mapper.mapType(Types.BLOB, "BLOB", 0, 0));
        assertEquals(
                BasicType.STRING_TYPE,
                mapper.mapType(Types.VARCHAR, "VARCHAR2", 255, 0));
        assertEquals(
                BasicType.DATE_TYPE,
                mapper.mapType(Types.DATE, "DATE", 0, 0));
    }

    @Test
    public void usesPortableHighGoNativeTypesForCrossDatabaseTargets() {
        assertEquals(
                "VARCHAR(255)",
                mapper.toDatabaseType(
                        Column.builder("name", BasicType.STRING_TYPE)
                                .length(255L)
                                .build()));
        assertEquals(
                "TEXT",
                mapper.toDatabaseType(
                        Column.builder("payload", BasicType.STRING_TYPE).build()));
        assertEquals(
                "BYTEA",
                mapper.toDatabaseType(
                        Column.builder("payload", BasicType.BYTES_TYPE).build()));
    }

    @Test
    public void preservesSafeCoreTypesButNormalizesCompatibilityOnlyTypes() {
        Column jsonb = Column.builder("payload", BasicType.STRING_TYPE)
                .sourceType("JSONB")
                .build();
        assertEquals("JSONB", mapper.toDatabaseType(jsonb, true));

        Column clob = Column.builder("payload", BasicType.STRING_TYPE)
                .sourceType("CLOB")
                .build();
        assertEquals("TEXT", mapper.toDatabaseType(clob, true));

        Column varchar2 = Column.builder("legacy_name", BasicType.STRING_TYPE)
                .sourceType("VARCHAR2(64)")
                .length(64L)
                .build();
        assertEquals("VARCHAR(64)", mapper.toDatabaseType(varchar2, true));
    }
}
