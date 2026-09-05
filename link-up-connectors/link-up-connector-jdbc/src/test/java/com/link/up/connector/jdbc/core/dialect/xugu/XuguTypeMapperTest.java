package com.link.up.connector.jdbc.core.dialect.xugu;

import com.link.up.api.table.catalog.Column;
import com.link.up.api.table.type.BasicType;
import com.link.up.api.table.type.DecimalType;
import org.junit.Test;

import java.sql.Types;

import static org.junit.Assert.assertEquals;

public class XuguTypeMapperTest {

    private final XuguTypeMapper mapper = new XuguTypeMapper();

    @Test
    public void supportsNumericPrecisionUpToThirtyEight() {
        Column column = Column.builder("AMOUNT", new DecimalType(38, 18)).build();
        assertEquals("NUMERIC(38,18)", mapper.toDatabaseType(column));
    }

    @Test(expected = IllegalArgumentException.class)
    public void rejectsTargetNumericPrecisionAboveXuguLimit() {
        mapper.toDatabaseType(
                Column.builder("AMOUNT", new DecimalType(39, 18)).build());
    }

    @Test
    public void unspecifiedNumericUsesXuguDefaultTwelveZero() {
        assertEquals(
                new DecimalType(12, 0),
                mapper.mapType(Types.NUMERIC, "NUMERIC", 0, 0));
    }

    @Test
    public void invalidSourceNumericShapesStayExactText() {
        assertEquals(
                BasicType.STRING_TYPE,
                mapper.mapType(Types.NUMERIC, "NUMERIC", 39, 2));
        assertEquals(
                BasicType.STRING_TYPE,
                mapper.mapType(Types.NUMERIC, "NUMBER", 20, -2));
        assertEquals(
                BasicType.STRING_TYPE,
                mapper.mapType(Types.NUMERIC, "DECIMAL", 10, 12));
    }

    @Test
    public void bitFamiliesStayExactBitStrings() {
        assertEquals(
                BasicType.STRING_TYPE,
                mapper.mapType(Types.BIT, "BIT(8)", 8, 0));
        assertEquals(
                BasicType.STRING_TYPE,
                mapper.mapType(Types.OTHER, "VARBIT(60000)", 60000, 0));
        assertEquals(
                BasicType.STRING_TYPE,
                mapper.mapType(Types.OTHER, "BIT VARYING(64)", 64, 0));
    }

    @Test
    public void mapsScalarAndCompatibilityAliases() {
        assertEquals(
                BasicType.BYTE_TYPE,
                mapper.mapType(Types.TINYINT, "TINYINT", 3, 0));
        assertEquals(
                BasicType.SHORT_TYPE,
                mapper.mapType(Types.SMALLINT, "SHORT", 5, 0));
        assertEquals(
                BasicType.INT_TYPE,
                mapper.mapType(Types.INTEGER, "PLS_INTEGER", 10, 0));
        assertEquals(
                BasicType.LONG_TYPE,
                mapper.mapType(Types.BIGINT, "LONGINT", 19, 0));
        assertEquals(
                BasicType.STRING_TYPE,
                mapper.mapType(Types.VARCHAR, "VARCHAR2", 255, 0));
        assertEquals(
                BasicType.STRING_TYPE,
                mapper.mapType(Types.OTHER, "JSON", 0, 0));
        assertEquals(
                BasicType.STRING_TYPE,
                mapper.mapType(Types.OTHER, "GUID", 36, 0));
    }

    @Test
    public void timezoneTypesNeverLoseOffsets() {
        assertEquals(
                BasicType.STRING_TYPE,
                mapper.mapType(
                        Types.TIME_WITH_TIMEZONE,
                        "TIME WITH TIME ZONE",
                        0,
                        3));
        assertEquals(
                BasicType.TIMESTAMP_TZ_TYPE,
                mapper.mapType(
                        Types.TIMESTAMP_WITH_TIMEZONE,
                        "TIMESTAMP WITH TIME ZONE",
                        0,
                        6));
        assertEquals(
                "VARCHAR(64)",
                mapper.toDatabaseType(
                        Column.builder("EVENT_TIME", BasicType.STRING_TYPE)
                                .sourceType("TIME(3) WITH TIME ZONE")
                                .build(),
                        true));
        assertEquals(
                "VARCHAR(64)",
                mapper.toDatabaseType(
                        Column.builder("EVENT_AT", BasicType.TIMESTAMP_TZ_TYPE)
                                .precision(6)
                                .build()));
    }

    @Test
    public void enforcesXuguTemporalPrecisionBounds() {
        assertEquals(
                "TIME(3)",
                mapper.toDatabaseType(
                        Column.builder("EVENT_TIME", BasicType.TIME_TYPE)
                                .precision(3)
                                .build()));
        assertEquals(
                "TIMESTAMP(6)",
                mapper.toDatabaseType(
                        Column.builder("CREATED_AT", BasicType.TIMESTAMP_TYPE)
                                .precision(6)
                                .build()));
    }

    @Test(expected = IllegalArgumentException.class)
    public void rejectsTimePrecisionAboveThree() {
        mapper.toDatabaseType(
                Column.builder("EVENT_TIME", BasicType.TIME_TYPE)
                        .precision(4)
                        .build());
    }

    @Test(expected = IllegalArgumentException.class)
    public void rejectsTimestampPrecisionAboveSix() {
        mapper.toDatabaseType(
                Column.builder("CREATED_AT", BasicType.TIMESTAMP_TYPE)
                        .precision(7)
                        .build());
    }

    @Test
    public void usesLobsBeyondBoundedFieldLimits() {
        assertEquals(
                "VARCHAR(60000)",
                mapper.toDatabaseType(
                        Column.builder("TEXT_VALUE", BasicType.STRING_TYPE)
                                .length(60000L)
                                .build()));
        assertEquals(
                "CLOB",
                mapper.toDatabaseType(
                        Column.builder("TEXT_VALUE", BasicType.STRING_TYPE)
                                .length(60001L)
                                .build()));
        assertEquals(
                "BINARY(60000)",
                mapper.toDatabaseType(
                        Column.builder("BIN_VALUE", BasicType.BYTES_TYPE)
                                .length(60000L)
                                .build()));
        assertEquals(
                "BLOB",
                mapper.toDatabaseType(
                        Column.builder("BIN_VALUE", BasicType.BYTES_TYPE)
                                .length(60001L)
                                .build()));
    }

    @Test
    public void preservesSafeSameDialectTypesButNotTimezoneOrInvalidNumeric() {
        Column json = Column.builder("PAYLOAD", BasicType.STRING_TYPE)
                .sourceType("JSON")
                .build();
        assertEquals("JSON", mapper.toDatabaseType(json, true));

        Column badNumeric = Column.builder("AMOUNT", BasicType.STRING_TYPE)
                .sourceType("NUMERIC(39,2)")
                .build();
        assertEquals("CLOB", mapper.toDatabaseType(badNumeric, true));

        Column timestampTz = Column.builder("EVENT_AT", BasicType.TIMESTAMP_TZ_TYPE)
                .sourceType("TIMESTAMP(6) WITH TIME ZONE")
                .precision(6)
                .build();
        assertEquals("VARCHAR(64)", mapper.toDatabaseType(timestampTz, true));
    }
}
